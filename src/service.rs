use super::{
    lib::{flow::Flow, pool::Pool},
    utils,
};
use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use futures::{compat::Stream01CompatExt, FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use hyper::{
    rt::Future as HyperFuture, service::Service as HyperService, Body, Method, Request, Response,
    StatusCode,
};
use lazy_static::lazy_static;
use parking_lot::RwLock;
use regex::Regex;
use serde::Serialize;
use std::{collections::VecDeque, error::Error as StdError, sync::Arc};

const CHUNK_DEFREGSIZE: usize = 256;
const BLOCK_MAXSIZE: usize = 65536;

pub trait ServiceFactory {
    type Future: HyperFuture<Item = Response<Body>, Error = Box<dyn StdError + Send + Sync>> + Send;
    type Service: HyperService<
            ReqBody = Body,
            ResBody = Body,
            Error = <Self::Future as HyperFuture>::Error,
            Future = Self::Future,
        > + Send;

    fn new_service(&self) -> Self::Service;
}

pub struct FlowServiceFactory {
    pool: Arc<RwLock<Pool>>,
}

impl FlowServiceFactory {
    pub fn new() -> Self {
        FlowServiceFactory {
            pool: Arc::new(RwLock::new(Pool::new())),
        }
    }
}

impl ServiceFactory for FlowServiceFactory {
    type Service = FlowServiceWrapper;
    type Future = <Self::Service as HyperService>::Future;

    fn new_service(&self) -> Self::Service {
        FlowServiceWrapper(Arc::new(FlowService::new(self.pool.clone())))
    }
}

pub struct FlowServiceWrapper(Arc<FlowService>);

impl HyperService for FlowServiceWrapper {
    type ReqBody = Body;
    type ResBody = Body;
    type Error = Box<dyn StdError + Send + Sync>;
    type Future =
        Box<dyn hyper::rt::Future<Item = Response<Self::ResBody>, Error = Self::Error> + Send>;

    fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
        Box::new(FlowService::dispatch(self.0.clone(), req).boxed().compat())
    }
}

struct FlowService {
    pool: Arc<RwLock<Pool>>,
}

#[derive(Serialize, Debug)]
struct CreateResponse {
    id: String,
    token: String,
}

type BoxError = Box<dyn StdError + Send + Sync>;
type ResponseResult = Result<Response<Body>, BoxError>;
type PullGeneratorState = (Arc<Flow>, u64, VecDeque<Bytes>);

impl FlowService {
    fn new(pool: Arc<RwLock<Pool>>) -> Self {
        FlowService { pool }
    }

    fn encode_key(key: u128) -> String {
        let mut buf = [0u8; 16];
        LittleEndian::write_u128(&mut buf, key);
        utils::hex(&buf)
    }

    fn decode_key(key_string: &str) -> Result<u128, ()> {
        utils::unhex(key_string).and_then(|buf| {
            if buf.len() != 16 {
                Err(())
            } else {
                Ok(LittleEndian::read_u128(&buf))
            }
        })
    }

    fn response_status(status_code: StatusCode) -> Response<Body> {
        Response::builder()
            .status(status_code)
            .body(Body::empty())
            .unwrap()
    }

    fn response_object<T: Serialize>(obj: &T) -> Response<Body> {
        Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(serde_json::to_string(obj).unwrap()))
            .unwrap()
    }

    async fn handle_create(&self) -> ResponseResult {
        let flow = match self.pool.write().create() {
            Some(flow) => flow,
            None => return Ok(Self::response_status(StatusCode::SERVICE_UNAVAILABLE)),
        };
        Ok(Self::response_object(&CreateResponse {
            id: Self::encode_key(flow.get_id()),
            token: Self::encode_key(flow.get_token()),
        }))
    }

    async fn push_loop(flow: Arc<Flow>, body: Body) -> Result<(), BoxError> {
        let mut stream = body.compat();
        let mut chunks = VecDeque::new();
        let mut size = 0;
        while let Some(Ok(chunk)) = stream.next().await {
            let mut chunk = chunk.into_bytes();
            while size + chunk.len() > BLOCK_MAXSIZE {
                if size < BLOCK_MAXSIZE {
                    chunks.push_back(chunk.split_to(BLOCK_MAXSIZE - size));
                }
                size = 0;
                chunks = flow.push(chunks).await.map_err(|err| Box::new(err))?;
            }
            if chunk.len() > 0 {
                size += chunk.len();
                match chunks.back_mut() {
                    Some(ref mut last_chunk) if chunk.len() < CHUNK_DEFREGSIZE => {
                        last_chunk.extend_from_slice(&chunk);
                    }
                    _ => {
                        chunks.push_back(chunk);
                    }
                }
            }
        }
        if chunks.len() > 0 {
            flow.push(chunks).await.map_err(|err| Box::new(err))?;
        }
        Ok(())
    }

    async fn handle_push(
        &self,
        flow_id: u128,
        query: Option<String>,
        body: Body,
    ) -> ResponseResult {
        let query = match query {
            Some(query) => query,
            None => return Ok(Self::response_status(StatusCode::BAD_REQUEST)),
        };
        let token = match url::form_urlencoded::parse(query.as_bytes())
            .find(|&(ref key, _)| key == "token")
            .ok_or(())
            .and_then(|(_, token)| Self::decode_key(&token))
        {
            Ok(token) => token,
            Err(_) => return Ok(Self::response_status(StatusCode::BAD_REQUEST)),
        };
        let flow = match self.pool.read().get(flow_id) {
            Some(flow) => flow,
            None => return Ok(Self::response_status(StatusCode::NOT_FOUND)),
        };
        if flow.get_token() != token {
            return Ok(Self::response_status(StatusCode::NOT_FOUND));
        }
        Self::push_loop(flow, body)
            .await
            .map(|_| Response::new(Body::empty()))
    }

    async fn pull_generator(
        state: PullGeneratorState,
    ) -> Option<(Result<Bytes, BoxError>, PullGeneratorState)> {
        let (flow, block_index, chunks) = state;
        let (block_index, mut chunks) = if chunks.len() == 0 {
            let chunks = match flow.pull(block_index).await {
                Ok(chunks) => chunks,
                Err(_) => return None,
            };
            (block_index + 1, chunks)
        } else {
            (block_index, chunks)
        };
        assert!(chunks.len() > 0);
        Some((Ok(chunks.pop_front().unwrap()), (flow, block_index, chunks)))
    }

    async fn handle_pull(&self, flow_id: u128) -> ResponseResult {
        let flow = match self.pool.read().get(flow_id) {
            Some(flow) => flow,
            None => return Ok(Self::response_status(StatusCode::NOT_FOUND)),
        };
        let stream = futures::stream::unfold((flow, 0, VecDeque::new()), Self::pull_generator)
            .map_ok(|chunk| hyper::Chunk::from(chunk));
        Ok(Response::new(Body::wrap_stream(stream.boxed().compat())))
    }

    async fn dispatch(self: Arc<Self>, req: Request<Body>) -> ResponseResult {
        lazy_static! {
            static ref PATTERN_CREATE: Regex = Regex::new(r"^/create$").unwrap();
            static ref PATTERN_PUSH: Regex = Regex::new(r"^/push/([a-f0-9]{32})$").unwrap();
            static ref PATTERN_PULL: Regex = Regex::new(r"^/pull/([a-f0-9]{32})$").unwrap();
        }
        let (path, query) = match req.uri().path_and_query() {
            Some(path_query) => (path_query.path(), path_query.query()),
            None => return Ok(Self::response_status(StatusCode::NOT_FOUND)),
        };
        let method = req.method().clone();
        match method {
            Method::POST => {
                if PATTERN_CREATE.captures(path).is_some() {
                    self.handle_create().await
                } else if let Some(captures) = PATTERN_PUSH.captures(path) {
                    self.handle_push(
                        Self::decode_key(captures.get(1).unwrap().as_str()).unwrap(),
                        query.map(|s| s.to_owned()),
                        req.into_body(),
                    )
                    .await
                } else {
                    Ok(Self::response_status(StatusCode::NOT_FOUND))
                }
            }
            Method::GET => {
                if let Some(captures) = PATTERN_PULL.captures(path) {
                    self.handle_pull(Self::decode_key(captures.get(1).unwrap().as_str()).unwrap())
                        .await
                } else {
                    Ok(Self::response_status(StatusCode::NOT_FOUND))
                }
            }
            _ => Ok(Self::response_status(StatusCode::METHOD_NOT_ALLOWED)),
        }
    }
}

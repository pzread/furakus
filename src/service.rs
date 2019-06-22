use super::{lib::pool::Pool, utils};
use byteorder::{ByteOrder, LittleEndian};
use futures::{compat::Stream01CompatExt, FutureExt, StreamExt, TryFutureExt};
use hyper::{
    rt::{Future as HyperFuture, Stream as HyperStream},
    service::Service as HyperService,
    Body, Method, Request, Response, StatusCode,
};
use lazy_static::lazy_static;
use parking_lot::RwLock;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{error::Error as StdError, sync::Arc};

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

type ResponseResult = Result<Response<Body>, Box<dyn StdError + Send + Sync>>;

impl FlowService {
    pub fn new(pool: Arc<RwLock<Pool>>) -> Self {
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

        let mut body_stream = body.compat();
        let mut buffer = bytes::Bytes::with_capacity(65536 * 2);
        while let Some(Ok(chunk)) = body_stream.next().await {
            buffer.extend_from_slice(&chunk.into_bytes());
            while buffer.len() >= 65536 {
                let data = buffer.split_to(65536);
            }
        }
        if buffer.len() > 0 {
            
        }

        Ok(Response::new(Body::empty()))
    }

    async fn handle_pull(&self, flow_id: u128) -> ResponseResult {
        let flow = match self.pool.read().get(flow_id) {
            Some(flow) => flow,
            None => return Ok(Self::response_status(StatusCode::NOT_FOUND)),
        };
        Ok(Response::new(Body::empty()))
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

use futures::{
    future::{FutureExt, TryFutureExt},
    lock::Mutex,
};
use hyper::{
    rt::Future as HyperFuture, service::Service as HyperService, Body, Method, Request, Response,
    StatusCode,
};
use lazy_static::lazy_static;
use regex::Regex;
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
    counter: Arc<Mutex<u64>>,
}

impl FlowServiceFactory {
    pub fn new() -> Self {
        FlowServiceFactory {
            counter: Arc::new(Mutex::new(0)),
        }
    }
}

impl ServiceFactory for FlowServiceFactory {
    type Service = FlowServiceWrapper;
    type Future = <Self::Service as HyperService>::Future;

    fn new_service(&self) -> Self::Service {
        FlowServiceWrapper(Arc::new(FlowService::new(self.counter.clone())))
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
    counter: Arc<Mutex<u64>>,
}

type ResponseResult = Result<Response<Body>, Box<dyn StdError + Send + Sync>>;

impl FlowService {
    pub fn new(counter: Arc<Mutex<u64>>) -> Self {
        FlowService { counter }
    }

    fn response_status(status_code: StatusCode) -> Response<Body> {
        Response::builder()
            .status(status_code)
            .body(Body::empty())
            .unwrap()
    }

    async fn handle_create(&self) -> ResponseResult {
        Ok(Response::new(Body::empty()))
    }

    async fn handle_push(&self, token: String, query: Option<String>) -> ResponseResult {
        let query = match query {
            Some(query) => query,
            None => return Ok(Self::response_status(StatusCode::BAD_REQUEST)),
        };
        let token = match url::form_urlencoded::parse(query.as_bytes())
            .find(|&(ref key, _)| key == "token")
        {
            Some((_, token)) => token.into_owned(),
            None => return Ok(Self::response_status(StatusCode::BAD_REQUEST)),
        };
        Ok(Response::new(Body::empty()))
    }

    async fn handle_pull(&self, token: String) -> ResponseResult {
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
                        captures.get(1).unwrap().as_str().to_owned(),
                        query.map(|s| s.to_owned()),
                    )
                    .await
                } else {
                    Ok(Self::response_status(StatusCode::NOT_FOUND))
                }
            }
            Method::GET => {
                if let Some(captures) = PATTERN_PULL.captures(path) {
                    self.handle_pull(captures.get(1).unwrap().as_str().to_owned())
                        .await
                } else {
                    Ok(Self::response_status(StatusCode::NOT_FOUND))
                }
            }
            _ => Ok(Self::response_status(StatusCode::METHOD_NOT_ALLOWED)),
        }
    }
}

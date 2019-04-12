use auth::Authorizer;
use furakus::{
    flow::{self, Error as FlowError, Flow},
    pool::Pool,
    utils::*,
};
use futures::{
    future::{self, Loop},
    prelude::*,
    Future,
};
use header_ext::HeaderBuilderExt;
use headers::{ContentLength, ContentType, HeaderMapExt};
use hyper::{
    error::Error as HyperError, service::Service as HyperService, Body, Chunk, Method, Request,
    Response, StatusCode,
};
use lazy_static::lazy_static;
use regex::Regex;
use serde_derive::{Deserialize, Serialize};
use serde_json;
use std::{
    error::Error as StdError,
    fmt,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use url;

#[derive(Debug)]
pub enum Error {
    Invalid,
    NotReady,
    Internal(HyperError),
    Other,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Invalid => "Invalid",
            Error::NotReady => "NotReady",
            Error::Internal(ref err) => err.description(),
            Error::Other => "Other",
        }
    }
}

#[derive(Deserialize)]
pub struct PoolConfig {
    size: usize,
    deactive_timeout: u64,
}

#[derive(Clone, Deserialize)]
pub struct FlowConfig {
    meta_capacity: u64,
    data_capacity: u64,
}

#[derive(Deserialize)]
pub struct Config {
    pool: PoolConfig,
    flow: FlowConfig,
}

type ServiceRequest = Request<Body>;
type ServiceResponse = Response<Body>;
type ServiceError = Box<dyn StdError + Send + Sync>;
type ResponseFuture = Box<dyn Future<Item = ServiceResponse, Error = ServiceError> + Send>;

pub trait ServiceFactory {
    type Error: Into<ServiceError>;
    type Future: Future<Item = Response<Body>, Error = Self::Error> + Send;
    type Service: HyperService<
            ReqBody = Body,
            ResBody = Body,
            Error = Self::Error,
            Future = Self::Future,
        > + Send;

    fn new_service(&self) -> Self::Service;
}

pub struct FlowServiceFactory {
    pool: Arc<RwLock<Pool>>,
    authorizer: Arc<Authorizer>,
    flow_config: FlowConfig,
}

impl FlowServiceFactory {
    pub fn new<T: Authorizer>(config: &Config, authorizer: T) -> FlowServiceFactory {
        FlowServiceFactory {
            pool: Pool::new(
                Some(config.pool.size),
                Some(Duration::from_secs(config.pool.deactive_timeout)),
            ),
            authorizer: Arc::new(authorizer),
            flow_config: config.flow.clone(),
        }
    }
}

impl ServiceFactory for FlowServiceFactory {
    type Error = <Self::Service as HyperService>::Error;
    type Future = <Self::Service as HyperService>::Future;
    type Service = FlowService;

    fn new_service(&self) -> Self::Service {
        FlowService::new(
            self.flow_config.clone(),
            self.pool.clone(),
            self.authorizer.clone(),
        )
    }
}

#[derive(Serialize, Deserialize)]
struct NewRequest {
    pub size: Option<u64>,
    pub preserve_mode: bool,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct NewResponse {
    pub id: String,
    pub token: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct ErrorResponse {
    pub message: String,
}

pub struct FlowService {
    config: FlowConfig,
    pool: Arc<RwLock<Pool>>,
    authorizer: Arc<Authorizer>,
}

type FT = future::FutureResult<ServiceResponse, ServiceError>;

impl FlowService {
    fn new(
        config: FlowConfig,
        pool: Arc<RwLock<Pool>>,
        authorizer: Arc<Authorizer>,
    ) -> FlowService {
        FlowService {
            config,
            pool,
            authorizer,
        }
    }

    fn response_status(status: StatusCode) -> FT {
        future::ok(
            Response::builder()
                .status(status)
                .body(Body::empty())
                .unwrap(),
        )
    }

    fn response_error(error: &str) -> FT {
        let data = serde_json::to_string(&ErrorResponse {
            message: error.to_owned(),
        })
        .unwrap();
        future::ok(
            Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .typed_header(ContentLength(data.len() as u64))
                .body(data.into())
                .unwrap(),
        )
    }

    fn check_authorization(&self, flow_id: &str, token: &str) -> bool {
        self.authorizer.verify(flow_id, token).is_ok()
    }

    fn parse_request_parameter<T>(
        req: ServiceRequest,
    ) -> Box<Future<Item = T, Error = Error> + Send>
    where
        T: serde::de::DeserializeOwned + Send + 'static,
    {
        let content_length = match req.headers().typed_get() {
            Some(ContentLength(length)) => length,
            None => return future::err(Error::Invalid).into_box(),
        };
        if content_length == 0 || content_length > 4096 {
            return future::err(Error::Invalid).into_box();
        }
        req.into_body()
            .concat2()
            .map_err(|err| Error::Internal(err))
            .and_then(|body| serde_json::from_slice::<T>(&body).map_err(|_| Error::Invalid))
            .into_box()
    }

    fn parse_request_querystring(req: &ServiceRequest) -> url::form_urlencoded::Parse {
        url::form_urlencoded::parse(req.uri().query().unwrap_or("").as_bytes())
    }

    fn handle_new(&self, req: ServiceRequest, _route: regex::Captures) -> ResponseFuture {
        let pool_ptr = self.pool.clone();
        let meta_capacity = self.config.meta_capacity;
        let data_capacity = self.config.data_capacity;
        let authorizer = self.authorizer.clone();
        Self::parse_request_parameter::<NewRequest>(req)
            .and_then(move |param| {
                let flow_ptr = Flow::new(flow::Config {
                    length: param.size,
                    meta_capacity,
                    data_capacity,
                    keepcount: Some(1),
                    preserve_mode: param.preserve_mode,
                });
                let flow_id = flow_ptr.read().unwrap().id.to_owned();
                {
                    let mut pool = pool_ptr.write().unwrap();
                    pool.insert(flow_ptr)
                        .map(|_| flow_id.clone())
                        .map_err(|_| Error::NotReady)
                }
            })
            .and_then(move |flow_id: String| {
                let token = authorizer.sign(&flow_id);
                let body = serde_json::to_string(&NewResponse { id: flow_id, token })
                    .unwrap()
                    .into_bytes();
                future::ok(
                    Response::builder()
                        .typed_header(ContentType::json())
                        .typed_header(ContentLength(body.len() as u64))
                        .body(Body::from(body))
                        .unwrap(),
                )
            })
            .or_else(|err| match err {
                Error::Invalid => Self::response_error("Invalid Parameter"),
                Error::NotReady => Self::response_status(StatusCode::SERVICE_UNAVAILABLE),
                Error::Internal(err) => future::err(Box::new(err) as ServiceError),
                Error::Other => Self::response_status(StatusCode::INTERNAL_SERVER_ERROR),
            })
            .into_box()
    }

    fn handle_push(&self, req: ServiceRequest, route: regex::Captures) -> ResponseFuture {
        let token = match Self::parse_request_querystring(&req).find(|&(ref key, _)| key == "token")
        {
            Some((_, token)) => token.into_owned(),
            None => return Self::response_error("Missing Token").into_box(),
        };
        let flow_id = route.get(1).unwrap().as_str();
        if !self.check_authorization(flow_id, &token) {
            return Self::response_status(StatusCode::NOT_FOUND).into_box();
        }
        let flow_ptr = match self.pool.read().unwrap().get(flow_id) {
            Some(flow) => flow.clone(),
            None => return Self::response_status(StatusCode::NOT_FOUND).into_box(),
        };

        let init_buffer = Vec::with_capacity(flow::REF_SIZE * 2);
        future::loop_fn((req.into_body(), init_buffer), move |(body, mut buffer)| {
            body.into_future()
                .map_err(|(err, _)| Error::Internal(err))
                .map(|(chunk, body)| match chunk {
                    Some(chunk) => {
                        buffer.extend_from_slice(&chunk);
                        if buffer.len() < flow::REF_SIZE {
                            (None, Loop::Continue((body, buffer)))
                        } else {
                            let new_buffer = Vec::with_capacity(flow::REF_SIZE * 2);
                            (Some(buffer), Loop::Continue((body, new_buffer)))
                        }
                    }
                    None => (Some(buffer), Loop::Break(())),
                })
                .and_then({
                    let flow_ptr = flow_ptr.clone();
                    move |(chunk, ret)| match chunk {
                        Some(chunk) => {
                            let mut flow = flow_ptr.write().unwrap();
                            flow.push(chunk)
                                .map_err(|err| match err {
                                    FlowError::Other => Error::Other,
                                    _ => Error::NotReady,
                                })
                                .map(|_| ret)
                                .into_box()
                        }
                        None => future::ok(ret).into_box(),
                    }
                })
        })
        .then(move |result| match result {
            Ok(()) => Self::response_status(StatusCode::OK),
            Err(Error::Invalid) => unreachable!(),
            Err(Error::NotReady) => Self::response_error("Not Ready"),
            Err(Error::Internal(err)) => future::err(Box::new(err) as ServiceError),
            Err(Error::Other) => Self::response_status(StatusCode::INTERNAL_SERVER_ERROR),
        })
        .into_box()
    }
}

impl HyperService for FlowService {
    type ReqBody = Body;
    type ResBody = Body;
    type Error = ServiceError;
    type Future = ResponseFuture;

    fn call(&mut self, req: ServiceRequest) -> Self::Future {
        lazy_static! {
            static ref PATTERN_NEW: Regex = Regex::new(r"^/new$").unwrap();
            static ref PATTERN_PUSH: Regex = Regex::new(r"^/flow/([a-f0-9]{32})/push$").unwrap();
            static ref PATTERN_PULL: Regex = Regex::new(r"^/flow/([a-f0-9]{32})/pull$").unwrap();
        }

        let path = &req.uri().path().to_string();
        match req.method() {
            &Method::POST => {
                if let Some(route) = PATTERN_NEW.captures(path) {
                    self.handle_new(req, route)
                } else {
                    Self::response_status(StatusCode::BAD_REQUEST).into_box()
                }
            }
            &Method::PUT => {
                if let Some(route) = PATTERN_PUSH.captures(path) {
                    self.handle_push(req, route)
                } else {
                    Self::response_status(StatusCode::BAD_REQUEST).into_box()
                }
            }
            /*&Method::GET => {
                if let Some(route) = PATTERN_PULL.captures(path) {
                    self.handle_pull(req, route)
                } else {
                    Self::response_status(StatusCode::BAD_REQUEST)
                }
            }*/
            _ => Self::response_status(StatusCode::BAD_REQUEST).into_box(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Config, FlowConfig, FlowService, FlowServiceFactory, NewRequest, NewResponse, PoolConfig,
        ServiceFactory,
    };
    use auth::HMACAuthorizer;
    use futures::Stream;
    use hyper::{header::CONTENT_LENGTH, service::Service, Body, Method, Request, StatusCode};
    use tokio::runtime::Runtime;

    const TEST_URI: &str = "http://example.com";
    const TEST_URI_NEW: &str = "http://example.com/new";

    fn new_factory() -> FlowServiceFactory {
        FlowServiceFactory::new(
            &Config {
                pool: PoolConfig {
                    size: 65536,
                    deactive_timeout: 5,
                },
                flow: FlowConfig {
                    meta_capacity: 1 * 1024 * 1024,
                    data_capacity: 1 * 1024 * 1024,
                },
            },
            HMACAuthorizer::new(),
        )
    }

    #[test]
    fn unexpected_method() {
        let mut service = new_factory().new_service();
        let mut runner = Runtime::new().unwrap();

        let req = Request::builder()
            .uri(TEST_URI)
            .method(Method::PATCH)
            .body("".into())
            .unwrap();
        let res = runner.block_on(service.call(req)).unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn handle_new() {
        let mut service = new_factory().new_service();
        let mut runner = Runtime::new().unwrap();

        let req = Request::post(TEST_URI_NEW)
            .method(Method::POST)
            .body("".into())
            .unwrap();
        let res = runner.block_on(service.call(req)).unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);

        let data = serde_json::to_string(&NewRequest {
            size: None,
            preserve_mode: false,
        })
        .unwrap();
        let req = Request::post(TEST_URI_NEW)
            .header(CONTENT_LENGTH, data.len())
            .body(data.into())
            .unwrap();
        let res = runner.block_on(service.call(req)).unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        assert!(serde_json::from_slice::<NewResponse>(
            &runner
                .block_on(res.into_body().concat2())
                .unwrap()
                .into_bytes()
        )
        .is_ok());
    }
}

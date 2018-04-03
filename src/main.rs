extern crate bytes;
extern crate dotenv;
extern crate futures;
extern crate hyper;
#[macro_use]
extern crate language_tags;
#[macro_use]
extern crate lazy_static;
extern crate native_tls;
extern crate regex;
extern crate ring;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tokio_core as tokio;
extern crate tokio_tls;
extern crate unicase;
extern crate url;
extern crate uuid;
mod auth;
mod flow;
mod pool;
mod tls;
mod utils;

use auth::{Authorizer, HMACAuthorizer};
use dotenv::dotenv;
use flow::{Error as FlowError, Flow};
use futures::{future, stream, Future, Sink, Stream, Then};
use hyper::{Error as HyperError, Method, StatusCode,
            header::{AcceptRanges, AccessControlAllowHeaders, AccessControlAllowMethods,
                     AccessControlAllowOrigin, AccessControlRequestHeaders, ByteRangeSpec,
                     CacheControl, CacheDirective, Charset, ContentDisposition, ContentLength,
                     ContentRange, ContentRangeSpec, ContentType, DispositionParam,
                     DispositionType, ETag, EntityTag, Range, RangeUnit}};
use hyper::server::{Http, Request, Response, Service};
use native_tls::TlsAcceptor;
use pool::Pool;
use regex::Regex;
use serde::de::DeserializeOwned;
use std::{error, fmt, io::{self, Error as IoError}, marker::PhantomData, sync::{Arc, RwLock},
          time::Duration, {env, mem, thread}};
use tokio::reactor::{self, Core};
use tokio_tls::TlsAcceptorExt;
use utils::BoxedFuture;

#[derive(Debug)]
pub enum Error {
    Invalid,
    NotReady,
    Internal(HyperError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Invalid => "Invalid",
            Error::NotReady => "NotReady",
            Error::Internal(ref err) => err.description(),
        }
    }
}

struct FlowService<ProtoReq, ProtoRes, ProtoErr> {
    pool: Arc<RwLock<Pool>>,
    remote: reactor::Remote,
    meta_capacity: u64,
    data_capacity: u64,
    authorizer: Arc<Authorizer>,
    _marker: PhantomData<(ProtoReq, ProtoRes, ProtoErr)>,
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
struct StatusResponse {
    pub tail: u64,
    pub next: u64,
    pub dropped: u64,
    pub pushed: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct ErrorResponse {
    pub message: String,
}

type ResponseFuture = Box<Future<Item = Response, Error = HyperError> + Send>;

impl<ProtoReq, ProtoRes, ProtoErr> FlowService<ProtoReq, ProtoRes, ProtoErr> {
    fn new(
        pool: Arc<RwLock<Pool>>,
        remote: reactor::Remote,
        meta_capacity: u64,
        data_capacity: u64,
        authorizer: Arc<Authorizer>,
    ) -> Self {
        FlowService {
            pool,
            remote,
            meta_capacity,
            data_capacity,
            authorizer,
            _marker: PhantomData,
        }
    }

    fn check_authorization(&self, flow_id: &str, token: &str) -> bool {
        self.authorizer.verify(flow_id, token).is_ok()
    }

    fn parse_request_querystring(req: &Request) -> url::form_urlencoded::Parse {
        url::form_urlencoded::parse(req.query().unwrap_or("").as_bytes())
    }

    fn parse_request_parameter<T>(req: Request) -> Box<Future<Item = T, Error = Error> + Send>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let content_length = match req.headers().get() {
            Some(&ContentLength(length)) => length,
            None => return future::err(Error::Invalid).boxed2(),
        };
        if content_length == 0 || content_length > 4096 {
            return future::err(Error::Invalid).boxed2();
        }
        req.body()
            .concat2()
            .map_err(|err| Error::Internal(err))
            .and_then(|body| serde_json::from_slice::<T>(&body).map_err(|_| Error::Invalid))
            .boxed2()
    }

    fn response_ok() -> Response {
        Response::new().with_header(ContentLength(0))
    }

    fn response_error(error: &str) -> Response {
        let body = serde_json::to_string(&ErrorResponse {
            message: error.to_owned(),
        }).unwrap();
        Response::new()
            .with_status(StatusCode::BadRequest)
            .with_header(ContentLength(body.len() as u64))
            .with_body(body)
    }

    fn handle_new(&self, req: Request, _route: regex::Captures) -> ResponseFuture {
        let pool_ptr = self.pool.clone();
        let meta_capacity = self.meta_capacity;
        let data_capacity = self.data_capacity;
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
                    Response::new()
                        .with_header(ContentType::json())
                        .with_header(ContentLength(body.len() as u64))
                        .with_body(body),
                )
            })
            .or_else(|err| match err {
                Error::Invalid => Ok(Self::response_error("Invalid Parameter")),
                Error::NotReady => Ok(Response::new().with_status(StatusCode::ServiceUnavailable)),
                Error::Internal(err) => Err(err),
            })
            .boxed2()
    }

    fn handle_push(&self, req: Request, route: regex::Captures) -> ResponseFuture {
        let token = match Self::parse_request_querystring(&req).find(|&(ref key, _)| key == "token")
        {
            Some((_, token)) => token.into_owned(),
            None => return future::ok(Self::response_error("Missing Token")).boxed2(),
        };
        let flow_id = route.get(1).unwrap().as_str();
        if !self.check_authorization(flow_id, &token) {
            return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed2();
        }
        let flow_ptr = match self.pool.read().unwrap().get(flow_id) {
            Some(flow) => flow.clone(),
            None => return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed2(),
        };
        req.body()
            .fold(Vec::<u8>::with_capacity(flow::REF_SIZE * 2), {
                let flow_ptr = flow_ptr.clone();
                move |mut buf_chunk, chunk| {
                    buf_chunk.extend_from_slice(&chunk);
                    if buf_chunk.len() >= flow::REF_SIZE {
                        let chunk = mem::replace(
                            &mut buf_chunk,
                            Vec::<u8>::with_capacity(flow::REF_SIZE * 2),
                        );
                        let mut flow = flow_ptr.write().unwrap();
                        flow.push(chunk)
                            .map(|_| buf_chunk)
                            .map_err(|err| HyperError::Io(IoError::new(io::ErrorKind::Other, err)))
                            .boxed2()
                    } else {
                        future::ok(buf_chunk).boxed2()
                    }
                }
            })
            .and_then({
                let flow_ptr = flow_ptr.clone();
                move |chunk| {
                    // Flush remaining chunk.
                    if chunk.len() > 0 {
                        let mut flow = flow_ptr.write().unwrap();
                        flow.push(chunk)
                            .map(|_| ())
                            .map_err(|err| HyperError::Io(IoError::new(io::ErrorKind::Other, err)))
                            .boxed2()
                    } else {
                        future::ok(()).boxed2()
                    }
                }
            })
            .and_then(|_| Ok(Self::response_ok()))
            .or_else(|err| match err {
                HyperError::Io(ref err)
                    if err.get_ref()
                        .and_then(|inner| inner.downcast_ref::<FlowError>())
                        .map(|inner| *inner != FlowError::Other)
                        .unwrap_or(false) =>
                {
                    Ok(Self::response_error("Not Ready"))
                }
                _ => Err(err),
                    let mut flow = flow_ptr.write().unwrap();
                    flow.close().then(|_| Err(err))
            })
            .boxed2()
    }

    fn handle_eof(&self, req: Request, route: regex::Captures) -> ResponseFuture {
        let token = match Self::parse_request_querystring(&req).find(|&(ref key, _)| key == "token")
        {
            Some((_, token)) => token.into_owned(),
            None => return future::ok(Self::response_error("Missing Token")).boxed2(),
        };
        let flow_id = route.get(1).unwrap().as_str();
        if !self.check_authorization(flow_id, &token) {
            return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed2();
        }
        let flow_ptr = match self.pool.read().unwrap().get(flow_id) {
            Some(flow) => flow.clone(),
            None => return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed2(),
        };
        {
            let mut flow = flow_ptr.write().unwrap();
            flow.close()
                .then(|result| match result {
                    Ok(_) => Ok(Self::response_ok()),
                    Err(FlowError::Invalid) => Ok(Self::response_error("Closed")),
                    _ => Ok(Response::new().with_status(StatusCode::InternalServerError)),
                })
                .boxed2()
        }
    }

    fn handle_status(&self, _req: Request, route: regex::Captures) -> ResponseFuture {
        let flow_id = route.get(1).unwrap().as_str();
        let flow_ptr = match self.pool.read().unwrap().get(flow_id) {
            Some(flow) => flow.clone(),
            None => return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed2(),
        };
        let body = {
            let flow = flow_ptr.read().unwrap();
            let (tail, next) = flow.get_range();
            let statistic = flow.get_statistic();
            serde_json::to_string(&StatusResponse {
                tail,
                next,
                dropped: statistic.dropped,
                pushed: statistic.pushed,
            }).unwrap()
        }.into_bytes();
        future::ok(
            Response::new()
                .with_header(ContentType::json())
                .with_header(ContentLength(body.len() as u64))
                .with_body(body),
        ).boxed2()
    }

    fn handle_fetch(&self, _req: Request, route: regex::Captures) -> ResponseFuture {
        let flow_id = route.get(1).unwrap().as_str();
        let chunk_index: u64 = match route.get(2).unwrap().as_str().parse() {
            Ok(index) => index,
            Err(_) => return future::ok(Self::response_error("Invalid Parameter")).boxed2(),
        };
        let flow_ptr = match self.pool.read().unwrap().get(flow_id) {
            Some(flow) => flow.clone(),
            None => return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed2(),
        };
        {
            let flow = flow_ptr.read().unwrap();
            flow.pull(chunk_index, None)
                .and_then(|chunk| {
                    future::ok(
                        Response::new()
                            .with_header(ContentType::octet_stream())
                            .with_header(ContentLength(chunk.len() as u64))
                            .with_header(CacheControl(vec![
                                CacheDirective::MaxAge(365000000),
                                CacheDirective::Extension("immutable".into(), None),
                            ]))
                            .with_body(chunk),
                    )
                })
                .or_else(|err| {
                    let status = match err {
                        FlowError::Eof | FlowError::Dropped => StatusCode::NotFound,
                        _ => StatusCode::InternalServerError,
                    };
                    future::ok(Response::new().with_status(status))
                })
                .boxed2()
        }
    }

    fn handle_pull(&self, req: Request, route: regex::Captures) -> ResponseFuture {
        let opt_filename = Self::parse_request_querystring(&req)
            .find(|&(ref key, _)| key == "filename")
            .map(|(_, token)| token.into_owned());
        let flow_id = route.get(1).unwrap().as_str();
        let flow_ptr = match self.pool.read().unwrap().get(flow_id) {
            Some(flow) => flow.clone(),
            None => return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed2(),
        };
        let (tx, body) = hyper::Body::pair();
        let mut response = Response::new()
            .with_header(ContentType::octet_stream())
            .with_header(CacheControl(vec![CacheDirective::NoCache]))
            .with_header(ETag(EntityTag::new(false, flow_id.to_owned())))
            .with_body(body);
        if let Some(filename) = opt_filename {
            let content_disp = ContentDisposition {
                disposition: DispositionType::Attachment,
                parameters: vec![
                    DispositionParam::Filename(
                        Charset::Ext("UTF-8".into()),
                        Some(langtag!(en)),
                        filename.as_bytes().to_vec(),
                    ),
                ],
            };
            response.headers_mut().set(content_disp);
        }
        let (pull_fut, mut chunk_index, mut skip_len) = {
            let flow = flow_ptr.read().unwrap();
            let (tail_index, _) = flow.get_range();
            let config = flow.get_config();
            let mut skip_len = 0;
            if let Some(length) = config.length {
                if let Some(range) = req.headers().get::<Range>() {
                    let range_start = match *range {
                        Range::Bytes(ref ranges) if ranges.len() == 1 => match ranges[0] {
                            ByteRangeSpec::FromTo(start, _) | ByteRangeSpec::AllFrom(start) => {
                                Some(start)
                            }
                            _ => None,
                        },
                        _ => None,
                    };
                    if let Some(range_start) = range_start {
                        let tail_offset = flow.get_statistic().dropped;
                        if range_start < tail_offset {
                            return future::ok(Response::new().with_status(StatusCode::NotFound))
                                .boxed2();
                        } else if range_start >= length {
                            return future::ok(
                                Response::new()
                                    .with_status(StatusCode::RangeNotSatisfiable)
                                    .with_header(ContentRange(ContentRangeSpec::Bytes {
                                        range: None,
                                        instance_length: Some(length),
                                    })),
                            ).boxed2();
                        } else {
                            skip_len = range_start - tail_offset;
                            response.set_status(StatusCode::PartialContent);
                            response
                                .headers_mut()
                                .set(ContentRange(ContentRangeSpec::Bytes {
                                    range: Some((range_start, length - 1)),
                                    instance_length: Some(length),
                                }));
                            response
                                .headers_mut()
                                .set(ContentLength(length - range_start));
                        }
                    } else {
                        return future::ok(Response::new().with_status(StatusCode::NotFound))
                            .boxed2();
                    }
                } else if tail_index == 0 {
                    // Only set content length when the flow is still complete.
                    response
                        .headers_mut()
                        .set(AcceptRanges(vec![RangeUnit::Bytes]));
                    response.headers_mut().set(ContentLength(length));
                }
            }
            (flow.pull(tail_index, None), tail_index, skip_len)
        };
        let remote = self.remote.clone();
        pull_fut
            .and_then(move |chunk| {
                let body_stream = stream::unfold(Some(chunk), move |previous| {
                    // Check if the flow is EOF.
                    if let Some(prev_chunk) = previous {
                        let flow = flow_ptr.read().unwrap();
                        let prev_chunk_len = prev_chunk.len() as u64;
                        let hyper_chunk: Result<hyper::Chunk, _> = if skip_len == 0 {
                            Ok(prev_chunk.into())
                        } else if prev_chunk_len <= skip_len {
                            skip_len -= prev_chunk_len;
                            Ok(vec![].into())
                        } else {
                            let slice_chunk = prev_chunk.slice_from(skip_len as usize);
                            skip_len = 0;
                            Ok(slice_chunk.into())
                        };
                        chunk_index += 1;
                        let fut = flow.pull(chunk_index, None).then(move |ret| match ret {
                            Ok(chunk) => future::ok((hyper_chunk, Some(chunk))),
                            Err(_) => future::ok((hyper_chunk, None)),
                        });
                        Some(fut)
                    } else {
                        None
                    }
                });
                // Schedule the sender to the reactor.
                remote.spawn(move |_| {
                    tx.send_all(body_stream)
                        .and_then(|(mut tx, _)| tx.close())
                        .then(|_| Ok(()))
                });
                Ok(response)
            })
            .or_else(|_| Ok(Response::new().with_status(StatusCode::NotFound)))
            .boxed2()
    }
}

impl<ProtoReq, ProtoRes, ProtoErr> Service for FlowService<ProtoReq, ProtoRes, ProtoErr>
where
    Request: From<ProtoReq>,
    Response: Into<ProtoRes>,
    HyperError: Into<ProtoErr>,
{
    type Request = ProtoReq;
    type Response = ProtoRes;
    type Error = ProtoErr;
    type Future = Then<
        ResponseFuture,
        Result<ProtoRes, ProtoErr>,
        fn(Result<Response, HyperError>) -> Result<ProtoRes, ProtoErr>,
    >;

    fn call(&self, req: Self::Request) -> Self::Future {
        lazy_static! {
            static ref PATTERN_NEW: Regex = Regex::new(r"^/new$").unwrap();
            static ref PATTERN_PUSH: Regex = Regex::new(r"^/flow/([a-f0-9]{32})/push$").unwrap();
            static ref PATTERN_EOF: Regex = Regex::new(r"^/flow/([a-f0-9]{32})/eof$").unwrap();
            static ref PATTERN_STATUS: Regex = Regex::new(r"^/flow/([a-f0-9]{32})/status$").unwrap();
            static ref PATTERN_FETCH: Regex =
                Regex::new(r"^/flow/([a-f0-9]{32})/fetch/(\d+)$").unwrap();
            static ref PATTERN_PULL: Regex = Regex::new(r"^/flow/([a-f0-9]{32})/pull$").unwrap();
        }
        let req = Request::from(req);
        let path = &req.path().to_owned();
        match req.method() {
            &Method::Post => if let Some(route) = PATTERN_NEW.captures(path) {
                self.handle_new(req, route)
            } else if let Some(route) = PATTERN_PUSH.captures(path) {
                self.handle_push(req, route)
            } else if let Some(route) = PATTERN_EOF.captures(path) {
                self.handle_eof(req, route)
            } else if let Some(route) = PATTERN_STATUS.captures(path) {
                self.handle_status(req, route)
            } else {
                future::ok(Response::new().with_status(StatusCode::NotFound)).boxed2()
            },
            &Method::Put => if let Some(route) = PATTERN_PUSH.captures(path) {
                self.handle_push(req, route)
            } else {
                future::ok(Response::new().with_status(StatusCode::NotFound)).boxed2()
            },
            &Method::Get => if let Some(route) = PATTERN_FETCH.captures(path) {
                self.handle_fetch(req, route)
            } else if let Some(route) = PATTERN_PULL.captures(path) {
                self.handle_pull(req, route)
            } else {
                future::ok(Response::new().with_status(StatusCode::NotFound)).boxed2()
            },
            &Method::Options => {
                let mut response = Response::new().with_header(AccessControlAllowMethods(vec![
                    Method::Post,
                    Method::Put,
                    Method::Get,
                    Method::Options,
                ]));
                if let Some(headers) = req.headers().get::<AccessControlRequestHeaders>() {
                    response
                        .headers_mut()
                        .set(AccessControlAllowHeaders(headers.to_vec()));
                };
                future::ok(response).boxed2()
            }
            _ => future::ok(Response::new().with_status(StatusCode::MethodNotAllowed)).boxed2(),
        }.then(|result| match result {
            Ok(res) => Ok(res.with_header(AccessControlAllowOrigin::Any).into()),
            Err(err) => Err(err.into()),
        })
    }
}

fn start_service(
    addr: std::net::SocketAddr,
    num_worker: usize,
    pool_size: Option<usize>,
    deactive_timeout: Option<Duration>,
    meta_capacity: u64,
    data_capacity: u64,
    tls_acceptor: Option<TlsAcceptor>,
) -> (std::net::SocketAddr, thread::JoinHandle<()>) {
    let upstream_listener = std::net::TcpListener::bind(&addr).unwrap();
    let pool_ptr = Pool::new(pool_size, deactive_timeout);
    let auth_ptr = Arc::new(HMACAuthorizer::new());
    let mut workers = Vec::with_capacity(num_worker);

    for idx in 0..num_worker {
        // Size of backlog = 64.
        let (io_tx, io_rx) = futures::sync::mpsc::channel::<std::net::TcpStream>(64);
        let pool_ptr = pool_ptr.clone();
        let auth_ptr = auth_ptr.clone();
        let tls_acceptor = tls_acceptor.clone();
        thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let handle = core.handle();
            let remote = core.remote();
            // Create the corresponding binding function.
            let bind_fn: Box<Fn(_, _) -> _> = if let Some(tls_acceptor) = tls_acceptor {
                Box::new(move |io, service| {
                    tls_acceptor
                        .accept_async(io)
                        .map_err(|_| ())
                        .and_then(move |io| {
                            let http = Http::<hyper::Chunk>::new();
                            http.serve_connection(io, service)
                                .map(|_| ())
                                .map_err(|_| ())
                        })
                        .boxed2()
                })
            } else {
                Box::new(move |io, service| {
                    let http = Http::<hyper::Chunk>::new();
                    http.serve_connection(io, service)
                        .map(|_| ())
                        .map_err(|_| ())
                        .boxed2()
                })
            };
            println!("Worker #{} is started.", idx);
            core.run(io_rx.for_each(|io| {
                let io = tokio::net::TcpStream::from_stream(io, &handle).unwrap();
                // 4x REF_SIZE should be enough for sending a chunk.
                io.set_send_buffer_size(flow::REF_SIZE * 4).unwrap();
                let service = FlowService::new(
                    pool_ptr.clone(),
                    remote.clone(),
                    meta_capacity,
                    data_capacity,
                    auth_ptr.clone(),
                );
                handle.spawn(bind_fn(io, service));
                Ok(())
            })).unwrap();
        });
        workers.push(io_tx);
    }
    let bind_addr = upstream_listener.local_addr().unwrap();
    let service_thd = thread::spawn(move || {
        for (idx, io) in upstream_listener.incoming().enumerate() {
            workers[idx % workers.len()]
                .clone()
                .send(io.unwrap())
                .wait()
                .unwrap();
        }
    });
    (bind_addr, service_thd)
}

fn main() {
    dotenv().ok();
    let addr: std::net::SocketAddr = env::var("SERVER_ADDRESS").unwrap().parse().unwrap();
    let num_worker: usize = env::var("NUM_WORKER").unwrap().parse().unwrap();
    let pool_size: usize = env::var("POOL_SIZE").unwrap().parse().unwrap();
    let deactive_timeout: u64 = env::var("DEACTIVE_TIMEOUT").unwrap().parse().unwrap();
    let meta_capacity: u64 = env::var("META_CAPACITY").unwrap().parse().unwrap();
    let data_capacity: u64 = env::var("DATA_CAPACITY").unwrap().parse().unwrap();
    #[cfg(target_os = "windows")]
    let tls_acceptor = tls::build_tls_from_pfx(&env::var("TLS_PFX").unwrap());
    #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "ios")))]
    let tls_acceptor = tls::build_tls_from_pem(
        &env::var("TLS_CERT").unwrap(),
        &env::var("TLS_PRIVATE").unwrap(),
    );
    let (_, service_thd) = start_service(
        addr,
        num_worker,
        Some(pool_size),
        Some(Duration::from_secs(deactive_timeout)),
        meta_capacity,
        data_capacity,
        Some(tls_acceptor),
    );
    service_thd.join().unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::Uri;
    use hyper::client::{Client, HttpConnector};
    use native_tls::{Certificate, TlsConnector};
    use std::collections::HashSet;
    use std::fs::File;
    use std::io::Read;
    use std::sync::mpsc;
    use std::u64;
    use tokio::net::TcpStream;
    use tokio_tls::{TlsConnectorExt, TlsStream};

    const MAX_CAPACITY: u64 = 1048576;
    const DEFL_FLOW_PARAM: &str = r#"{"preserve_mode": false}"#;

    fn spawn_server() -> String {
        let (bind_addr, _) = start_service(
            "127.0.0.1:0".parse().unwrap(),
            1,
            Some(32),
            Some(Duration::from_secs(6)),
            MAX_CAPACITY,
            MAX_CAPACITY,
            None,
        );
        format!("http://127.0.0.1:{}", bind_addr.port())
    }

    fn create_flow(prefix: &str, param: &str) -> (String, String) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(param.to_owned());
        req.headers_mut().set(ContentLength(param.len() as u64));

        let data = core.run(client.request(req).and_then(|res| {
            assert_eq!(res.status(), StatusCode::Ok);
            res.body()
                .concat2()
                .and_then(|body| Ok(serde_json::from_slice::<NewResponse>(&body).unwrap()))
        })).unwrap();

        (data.id, data.token)
    }

    fn req_push(
        prefix: &str,
        flow_id: &str,
        token: &str,
        payload: &[u8],
    ) -> (StatusCode, Option<String>) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let mut req = Request::new(
            Method::Post,
            format!("{}/flow/{}/push?token={}", prefix, flow_id, token)
                .parse()
                .unwrap(),
        );
        req.set_body(payload.to_vec());

        let (status_code, response) = core.run(client.request(req).and_then(|res| {
            let status_code = res.status();
            let fut = if status_code == StatusCode::BadRequest {
                res.body()
                    .concat2()
                    .and_then(|body| {
                        let data = serde_json::from_slice::<ErrorResponse>(&body).unwrap();
                        Ok(Some(data.message))
                    })
                    .boxed2()
            } else {
                future::ok(None).boxed2()
            };
            fut.and_then(move |body| Ok((status_code, body)))
        })).unwrap();

        (status_code, response)
    }

    fn req_close(prefix: &str, flow_id: &str, token: &str) -> (StatusCode, Option<String>) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let req = Request::new(
            Method::Post,
            format!("{}/flow/{}/eof?token={}", prefix, flow_id, token)
                .parse()
                .unwrap(),
        );

        let (status_code, response) = core.run(client.request(req).and_then(|res| {
            let status_code = res.status();
            let fut = if status_code == StatusCode::BadRequest {
                res.body()
                    .concat2()
                    .and_then(|body| {
                        let data = serde_json::from_slice::<ErrorResponse>(&body).unwrap();
                        Ok(Some(data.message))
                    })
                    .boxed2()
            } else {
                future::ok(None).boxed2()
            };
            fut.and_then(move |body| Ok((status_code, body)))
        })).unwrap();

        (status_code, response)
    }

    fn req_status(prefix: &str, flow_id: &str) -> (StatusCode, Option<StatusResponse>) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let req = Request::new(
            Method::Post,
            format!("{}/flow/{}/status", prefix, flow_id)
                .parse()
                .unwrap(),
        );

        let (status_code, response) = core.run(client.request(req).and_then(|res| {
            let status_code = res.status();
            let fut = if status_code == StatusCode::Ok {
                res.body()
                    .concat2()
                    .and_then(|body| Ok(Some(body.to_vec())))
                    .boxed2()
            } else {
                future::ok(None).boxed2()
            };
            fut.and_then(move |body| {
                let response =
                    body.map(|data| serde_json::from_slice::<StatusResponse>(&data).unwrap());
                Ok((status_code, response))
            })
        })).unwrap();

        (status_code, response)
    }

    fn req_fetch(prefix: &str, flow_id: &str, index: u64) -> (StatusCode, Option<Vec<u8>>) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let req = Request::new(
            Method::Get,
            format!("{}/flow/{}/fetch/{}", prefix, flow_id, index)
                .parse()
                .unwrap(),
        );

        let (status_code, response) = core.run(client.request(req).and_then(|res| {
            let status_code = res.status();
            if status_code == StatusCode::Ok {
                let policies = res.headers().get::<CacheControl>().unwrap();
                let mut check_immutable = false;
                let mut check_maxage = false;
                for policy in policies.iter() {
                    match policy {
                        &CacheDirective::Extension(ref ext, None) if ext == "immutable" => {
                            check_immutable = true;
                        }
                        &CacheDirective::MaxAge(age) if age == 365000000 => {
                            check_maxage = true;
                        }
                        _ => panic!("Unexpected cache policy"),
                    }
                }
                assert!(check_immutable && check_maxage);
            }
            let fut = if status_code == StatusCode::Ok {
                res.body()
                    .concat2()
                    .and_then(|body| Ok(Some(body.to_vec())))
                    .boxed2()
            } else {
                future::ok(None).boxed2()
            };
            fut.and_then(move |body| Ok((status_code, body)))
        })).unwrap();

        (status_code, response)
    }

    fn req_pull(prefix: &str, flow_id: &str) -> (StatusCode, Option<Vec<u8>>) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let req = Request::new(
            Method::Get,
            format!("{}/flow/{}/pull", prefix, flow_id).parse().unwrap(),
        );

        let (status_code, response) = core.run(client.request(req).and_then(|res| {
            let status_code = res.status();
            if status_code == StatusCode::Ok {
                assert_eq!(
                    res.headers().get::<CacheControl>().unwrap(),
                    &CacheControl(vec![CacheDirective::NoCache])
                );
                assert_eq!(
                    res.headers().get::<ETag>().unwrap(),
                    &ETag(EntityTag::new(false, flow_id.to_owned()))
                );
            }
            let fut = if status_code == StatusCode::Ok {
                res.body()
                    .concat2()
                    .and_then(|body| Ok(Some(body.to_vec())))
                    .boxed2()
            } else {
                future::ok(None).boxed2()
            };
            fut.and_then(move |body| Ok((status_code, body)))
        })).unwrap();

        (status_code, response)
    }

    fn check_error_response(
        res: Response,
        error: &str,
    ) -> Box<Future<Item = (), Error = HyperError>> {
        assert_eq!(res.status(), StatusCode::BadRequest);
        let error = error.to_owned();
        res.body()
            .concat2()
            .and_then(move |body| {
                assert_eq!(
                    serde_json::from_slice::<ErrorResponse>(&body).unwrap(),
                    ErrorResponse { message: error }
                );
                Ok(())
            })
            .boxed2()
    }

    #[test]
    fn validate_route() {
        let prefix = &spawn_server();
        let (ref flow_id, _) = create_flow(prefix, DEFL_FLOW_PARAM);

        fn check_status(req: Request, status_code: StatusCode) -> Response {
            let mut core = Core::new().unwrap();
            let client = Client::new(&core.handle());
            core.run(client.request(req).and_then(|res| {
                assert_eq!(
                    res.headers().get::<AccessControlAllowOrigin>(),
                    Some(&AccessControlAllowOrigin::Any)
                );
                assert_eq!(res.status(), status_code);
                Ok(res)
            })).unwrap()
        }

        let req = Request::new(Method::Post, format!("{}/neo", prefix).parse().unwrap());
        check_status(req, StatusCode::NotFound);

        let req = Request::new(
            Method::Post,
            format!("{}/new/../new", prefix).parse().unwrap(),
        );
        check_status(req, StatusCode::NotFound);

        let req = Request::new(Method::Post, format!("{}//new", prefix).parse().unwrap());
        check_status(req, StatusCode::NotFound);

        let req = Request::new(Method::Post, format!("{}/new/", prefix).parse().unwrap());
        check_status(req, StatusCode::NotFound);

        let req = Request::new(
            Method::Post,
            format!("{}/{}/push", prefix, flow_id).parse().unwrap(),
        );
        check_status(req, StatusCode::NotFound);

        let req = Request::new(
            Method::Post,
            format!("{}/flow/{}/pusha", prefix, flow_id)
                .parse()
                .unwrap(),
        );
        check_status(req, StatusCode::NotFound);

        let req = Request::new(
            Method::Post,
            format!("{}/flow/{}/eofa", prefix, flow_id).parse().unwrap(),
        );
        check_status(req, StatusCode::NotFound);

        let req = Request::new(
            Method::Post,
            format!("{}/flow/{}/statusa", prefix, flow_id)
                .parse()
                .unwrap(),
        );
        check_status(req, StatusCode::NotFound);
        let req = Request::new(
            Method::Get,
            format!("{}/flow/{}/pullb", prefix, flow_id)
                .parse()
                .unwrap(),
        );
        check_status(req, StatusCode::NotFound);

        let req = Request::new(
            Method::Get,
            format!("{}/flow/{}/fetchb", prefix, flow_id)
                .parse()
                .unwrap(),
        );
        check_status(req, StatusCode::NotFound);

        let req = Request::new(Method::Put, format!("{}/new", prefix).parse().unwrap());
        check_status(req, StatusCode::NotFound);

        let access_headers = vec![
            unicase::Ascii::new("Content-Type".to_string()),
            unicase::Ascii::new("Content-Encoding".to_string()),
        ];
        let mut req = Request::new(Method::Options, format!("{}/abc", prefix).parse().unwrap());
        req.headers_mut()
            .set(AccessControlRequestHeaders(access_headers.clone()));
        let res = check_status(req, StatusCode::Ok);
        let allow_methods: HashSet<_> = res.headers()
            .get::<AccessControlAllowMethods>()
            .unwrap()
            .to_vec()
            .into_iter()
            .collect();
        assert_eq!(
            allow_methods,
            vec![Method::Get, Method::Post, Method::Put, Method::Options]
                .into_iter()
                .collect()
        );
        let allow_headers: HashSet<_> = res.headers()
            .get::<AccessControlAllowHeaders>()
            .unwrap()
            .to_vec()
            .into_iter()
            .collect();
        assert_eq!(allow_headers, access_headers.into_iter().collect());

        let req = Request::new(Method::Patch, format!("{}/new", prefix).parse().unwrap());
        check_status(req, StatusCode::MethodNotAllowed);
    }

    #[test]
    fn handle_new() {
        let prefix = &spawn_server();
        let mut core = Core::new().unwrap();
        let handle = &core.handle();

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        let param = r#"{"preserve_mode": false}"#;
        req.set_body(param);
        req.headers_mut().set(ContentLength(param.len() as u64));
        core.run({
            let client = Client::new(handle);
            client.request(req).and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                res.body().concat2().and_then(|body| {
                    let data = serde_json::from_slice::<NewResponse>(&body).unwrap();
                    assert!(
                        Regex::new("^[a-f0-9]{32}$")
                            .unwrap()
                            .find(&data.id)
                            .is_some()
                    );
                    assert!(
                        Regex::new("^[a-f0-9]{64}$")
                            .unwrap()
                            .find(&data.token)
                            .is_some()
                    );
                    Ok(())
                })
            })
        }).unwrap();

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        let param = r#"{"size": 4096, "preserve_mode": false}"#;
        req.set_body(param);
        req.headers_mut().set(ContentLength(param.len() as u64));
        core.run({
            let client = Client::new(handle);
            client.request(req).and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                res.body().concat2().and_then(|body| {
                    let data = serde_json::from_slice::<NewResponse>(&body).unwrap();
                    assert!(
                        Regex::new("^[a-f0-9]{32}$")
                            .unwrap()
                            .find(&data.id)
                            .is_some()
                    );
                    assert!(
                        Regex::new("^[a-f0-9]{64}$")
                            .unwrap()
                            .find(&data.token)
                            .is_some()
                    );
                    Ok(())
                })
            })
        }).unwrap();

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{"preserve_mode": false}"#);
        core.run({
            let client = Client::new(handle);
            client
                .request(req)
                .and_then(|res| check_error_response(res, "Invalid Parameter"))
        }).unwrap();

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        let param = r#"{"size": 4O96, "preserve_mode": false}"#;
        req.set_body(param);
        req.headers_mut().set(ContentLength(param.len() as u64));
        core.run({
            let client = Client::new(handle);
            client
                .request(req)
                .and_then(|res| check_error_response(res, "Invalid Parameter"))
        }).unwrap();

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(vec![65u8; 4097]);
        req.headers_mut().set(ContentLength(4097));
        core.run({
            let client = Client::new(handle);
            client
                .request(req)
                .and_then(|res| check_error_response(res, "Invalid Parameter"))
        }).unwrap();

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{"preserve_mode": false}"#);
        req.headers_mut().set(ContentLength(4097));
        core.run({
            let client = Client::new(handle);
            client
                .request(req)
                .and_then(|res| check_error_response(res, "Invalid Parameter"))
        }).unwrap();

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        let mut body = vec![65u8; 4096];
        body.extend_from_slice(r#"{"preserve_mode": false}"#.as_bytes());
        req.set_body(body);
        req.headers_mut().set(ContentLength(4096));
        core.run({
            let client = Client::new(handle);
            client
                .request(req)
                .and_then(|res| check_error_response(res, "Invalid Parameter"))
        }).unwrap();

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        let mut body = r#"{"preserve_mode": false}"#.to_string();
        body.extend(&vec![' '; 4096]);
        req.set_body(body);
        req.headers_mut().set(ContentLength(4096));
        core.run({
            let client = Client::new(handle);
            client.request(req).and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                res.body().concat2().and_then(|body| {
                    let data = serde_json::from_slice::<NewResponse>(&body).unwrap();
                    assert!(
                        Regex::new("^[a-f0-9]{32}$")
                            .unwrap()
                            .find(&data.id)
                            .is_some()
                    );
                    assert!(
                        Regex::new("^[a-f0-9]{64}$")
                            .unwrap()
                            .find(&data.token)
                            .is_some()
                    );
                    Ok(())
                })
            })
        }).unwrap();
    }

    #[test]
    fn handle_push_fetch() {
        let prefix = &spawn_server();
        let mut core = Core::new().unwrap();
        let handle = &core.handle();

        let (ref flow_id, ref token) = create_flow(prefix, DEFL_FLOW_PARAM);
        let fake_id = "bdc62e9323003d0f5cb44c8c745a0470";
        let mal_token = "sjlc(84c84w47wq87a";
        let fake_token = "bdc62e9323003d0f5cb44c8c745a0470bdc62e9323003d0f5cb44c8c745a0470";
        let payload1: &[u8] = b"The quick brown fox jumps\nover the lazy dog";
        let payload2: &[u8] = b"The guick yellow fox jumps\nover the fast cat";

        // The empty chunk should be ignored.
        // No content length.
        let req = Request::new(
            Method::Post,
            format!("{}/flow/{}/push?token={}", prefix, flow_id, token)
                .parse()
                .unwrap(),
        );
        core.run({
            let client = Client::new(handle);
            client.request(req).and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                Ok(())
            })
        }).unwrap();
        // With 0 content length.
        assert_eq!(
            req_push(prefix, flow_id, token, b""),
            (StatusCode::Ok, None)
        );

        let req = Request::new(
            Method::Post,
            format!("{}/flow/{}/push", prefix, flow_id).parse().unwrap(),
        );
        core.run({
            let client = Client::new(handle);
            client
                .request(req)
                .and_then(|res| check_error_response(res, "Missing Token"))
        }).unwrap();

        assert_eq!(
            req_push(prefix, fake_id, token, payload1),
            (StatusCode::NotFound, None)
        );
        assert_eq!(
            req_push(prefix, flow_id, mal_token, payload1),
            (StatusCode::NotFound, None)
        );
        assert_eq!(
            req_push(prefix, flow_id, fake_token, payload1),
            (StatusCode::NotFound, None)
        );
        assert_eq!(
            req_push(prefix, flow_id, token, payload1),
            (StatusCode::Ok, None)
        );
        assert_eq!(
            req_push(prefix, flow_id, token, payload2),
            (StatusCode::Ok, None)
        );

        assert_eq!(req_fetch(prefix, fake_id, 0), (StatusCode::NotFound, None));
        assert_eq!(
            req_fetch(prefix, flow_id, 0),
            (StatusCode::Ok, Some(payload1.to_vec()))
        );
        assert_eq!(
            req_fetch(prefix, flow_id, 1),
            (StatusCode::Ok, Some(payload2.to_vec()))
        );

        let req = Request::new(
            Method::Get,
            format!("{}/flow/{}/fetch/123{}", prefix, flow_id, u64::MAX)
                .parse()
                .unwrap(),
        );
        core.run({
            let client = Client::new(handle);
            client
                .request(req)
                .and_then(|res| check_error_response(res, "Invalid Parameter"))
        }).unwrap();

        let mut req = Request::new(
            Method::Put,
            format!("{}/flow/{}/push?token={}", prefix, flow_id, token)
                .parse()
                .unwrap(),
        );
        req.headers_mut().set(ContentLength(payload1.len() as u64));
        req.set_body(payload1.to_vec());
        core.run({
            let client = Client::new(handle);
            client.request(req).and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                Ok(())
            })
        }).unwrap();
        assert_eq!(
            req_fetch(prefix, flow_id, 2),
            (StatusCode::Ok, Some(payload1.to_vec()))
        );

        let thd = {
            let prefix = prefix.clone();
            let flow_id = flow_id.clone();
            let token = token.clone();
            let payload1 = payload1.clone();
            thread::spawn(move || {
                let prefix = &prefix;
                let flow_id = &flow_id;
                let token = &token;
                thread::park();
                thread::sleep(Duration::from_millis(1000));
                assert_eq!(
                    req_push(prefix, flow_id, token, payload1),
                    (StatusCode::Ok, None)
                );
            })
        };

        thd.thread().unpark();
        assert_eq!(
            req_fetch(prefix, flow_id, 2),
            (StatusCode::Ok, Some(payload1.to_vec()))
        );
        thd.join().unwrap();
    }

    #[test]
    fn handle_push_pull() {
        let prefix = &spawn_server();
        let mut core = Core::new().unwrap();
        let handle = &core.handle();
        let payload = vec![1u8; flow::REF_SIZE * 10];
        let (ref flow_id, ref token) = create_flow(prefix, DEFL_FLOW_PARAM);
        let fake_id = "bdc62e9323003d0f5cb44c8c745a0470";

        let thd = {
            let prefix = prefix.clone();
            let flow_id = flow_id.clone();
            let token = token.clone();
            let payload = payload.clone();
            thread::spawn(move || {
                let prefix = &prefix;
                let flow_id = &flow_id;
                let token = &token;
                for chunk in payload.chunks(flow::REF_SIZE * 2 + 13) {
                    assert_eq!(
                        req_push(prefix, flow_id, token, chunk),
                        (StatusCode::Ok, None)
                    );
                }
                assert_eq!(req_close(prefix, flow_id, token), (StatusCode::Ok, None));
            })
        };

        assert_eq!(req_pull(prefix, fake_id), (StatusCode::NotFound, None));
        assert_eq!(req_pull(prefix, flow_id), (StatusCode::Ok, Some(payload)));
        thd.join().unwrap();

        let (ref flow_id, ref token) = create_flow(prefix, DEFL_FLOW_PARAM);
        assert_eq!(
            req_push(prefix, flow_id, token, b"Hello"),
            (StatusCode::Ok, None)
        );
        assert_eq!(req_close(prefix, flow_id, token), (StatusCode::Ok, None));

        let filename = "Sc r\r\nipト.рус";
        let qs = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("filename", filename)
            .finish();
        let req = Request::new(
            Method::Get,
            format!("{}/flow/{}/pull?{}", prefix, flow_id, qs)
                .parse()
                .unwrap(),
        );
        core.run({
            let client = Client::new(handle);
            client.request(req).and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                let res_disp = res.headers().get::<ContentDisposition>().unwrap();
                let check_disp = ContentDisposition {
                    disposition: DispositionType::Attachment,
                    parameters: vec![
                        DispositionParam::Filename(
                            Charset::Ext("UTF-8".into()),
                            Some(langtag!(en)),
                            filename.as_bytes().to_vec(),
                        ),
                    ],
                };
                assert_eq!(res_disp, &check_disp);
                Ok(())
            })
        }).unwrap();

        let (ref flow_id, ref token) = create_flow(prefix, DEFL_FLOW_PARAM);
        assert_eq!(req_close(prefix, flow_id, token), (StatusCode::Ok, None));
        assert_eq!(req_pull(prefix, flow_id), (StatusCode::NotFound, None));
    }

    #[test]
    fn handle_eof() {
        let prefix = &spawn_server();
        let mut core = Core::new().unwrap();
        let handle = &core.handle();
        let (ref flow_id, ref token) = create_flow(prefix, DEFL_FLOW_PARAM);
        let fake_id = "bdc62e9323003d0f5cb44c8c745a0470";
        let mal_token = "sjlc(84c84w47wq87a";
        let fake_token = "bdc62e9323003d0f5cb44c8c745a0470bdc62e9323003d0f5cb44c8c745a0470";

        let req = Request::new(
            Method::Post,
            format!("{}/flow/{}/eof", prefix, flow_id).parse().unwrap(),
        );
        core.run({
            let client = Client::new(handle);
            client
                .request(req)
                .and_then(|res| check_error_response(res, "Missing Token"))
        }).unwrap();

        assert_eq!(
            req_close(prefix, fake_id, token),
            (StatusCode::NotFound, None)
        );
        assert_eq!(
            req_close(prefix, flow_id, mal_token),
            (StatusCode::NotFound, None)
        );
        assert_eq!(
            req_close(prefix, flow_id, fake_token),
            (StatusCode::NotFound, None)
        );
        assert_eq!(req_close(prefix, flow_id, token), (StatusCode::Ok, None));
        assert_eq!(
            req_push(prefix, flow_id, token, b"Hello"),
            (StatusCode::BadRequest, Some("Not Ready".to_string()),)
        );
        assert_eq!(
            req_close(prefix, flow_id, token),
            (StatusCode::BadRequest, Some("Closed".to_string()))
        );
        assert_eq!(req_fetch(prefix, flow_id, 0), (StatusCode::NotFound, None));
        assert_eq!(
            req_push(prefix, flow_id, token, b"Hello"),
            (StatusCode::NotFound, None)
        );
    }

    #[test]
    fn recycle_and_release() {
        let prefix = &spawn_server();
        let (ref flow_id, ref token) = create_flow(prefix, DEFL_FLOW_PARAM);

        let (tx, rx) = mpsc::channel();
        let thd = {
            let prefix = prefix.to_owned();
            let flow_id = flow_id.to_owned();
            thread::spawn(move || {
                tx.send(()).unwrap();
                assert_eq!(
                    req_fetch(&prefix, &flow_id, 100),
                    (StatusCode::InternalServerError, None)
                );
            })
        };
        rx.recv().unwrap();
        thread::sleep(Duration::from_millis(1000));

        assert_eq!(req_close(prefix, flow_id, token), (StatusCode::Ok, None));
        assert_eq!(
            req_close(prefix, flow_id, token),
            (StatusCode::BadRequest, Some("Closed".to_string()))
        );
        assert_eq!(req_fetch(prefix, flow_id, 0), (StatusCode::NotFound, None));
        assert_eq!(
            req_close(prefix, flow_id, token),
            (StatusCode::NotFound, None)
        );

        thd.join().unwrap();
    }

    #[test]
    fn dropped() {
        let prefix = &spawn_server();
        let (ref flow_id, ref token) = create_flow(prefix, DEFL_FLOW_PARAM);
        let payload1: &[u8] = b"The quick brown fox jumps\nover the lazy dog";
        let payload2: &[u8] = b"The guick yellow fox jumps\nover the fast cat";

        assert_eq!(
            req_push(prefix, flow_id, token, payload1),
            (StatusCode::Ok, None)
        );
        assert_eq!(
            req_push(prefix, flow_id, token, payload2),
            (StatusCode::Ok, None)
        );
        assert_eq!(req_close(prefix, flow_id, token), (StatusCode::Ok, None));
        assert_eq!(
            req_fetch(prefix, flow_id, 0),
            (StatusCode::Ok, Some(payload1.to_vec()))
        );
        assert_eq!(
            req_pull(prefix, flow_id),
            (StatusCode::Ok, Some([payload1, payload2].concat()))
        );
        assert_eq!(req_fetch(prefix, flow_id, 0), (StatusCode::NotFound, None));
    }

    #[test]
    fn full_push() {
        let prefix = &spawn_server();
        let (ref flow_id, ref token) = create_flow(prefix, DEFL_FLOW_PARAM);

        assert_eq!(
            req_push(prefix, flow_id, token, b"Hello"),
            (StatusCode::Ok, None)
        );

        let (tx, rx) = mpsc::channel();
        {
            let prefix = prefix.clone();
            let flow_id = flow_id.clone();
            let token = token.clone();
            let tx = tx.clone();
            thread::spawn(move || {
                let prefix = &prefix;
                let flow_id = &flow_id;
                let token = &token;
                req_push(prefix, flow_id, token, &vec![0u8; MAX_CAPACITY as usize]);
                req_close(prefix, flow_id, token);
                tx.send(()).unwrap();
            });
        }

        loop {
            let status = req_status(prefix, flow_id).1.unwrap();
            if status.pushed >= MAX_CAPACITY {
                break;
            }
        }

        assert_eq!(
            req_push(prefix, flow_id, token, b"Hello"),
            (StatusCode::BadRequest, Some("Not Ready".to_string()),)
        );

        req_pull(prefix, flow_id);
        rx.recv().unwrap();
    }

    #[test]
    fn racing_pull() {
        let prefix = &spawn_server();
        let (ref flow_id, ref token) = create_flow(prefix, DEFL_FLOW_PARAM);

        let (send_tx, send_rx) = mpsc::channel();

        {
            let prefix = prefix.clone();
            let flow_id = flow_id.clone();
            let token = token.clone();
            thread::spawn(move || {
                let prefix = &prefix;
                let flow_id = &flow_id;
                let token = &token;

                let mut core = Core::new().unwrap();
                let handle = core.handle();

                // Infinite flow.
                let body_stream = stream::unfold((), move |_| {
                    send_tx.send(()).unwrap();
                    Some(future::ok((
                        Ok(hyper::Chunk::from(vec![0u8; flow::REF_SIZE])),
                        (),
                    )))
                });

                let (tx, body) = hyper::Body::pair();
                let url = format!("{}/flow/{}/push?token={}", prefix, flow_id, token)
                    .parse()
                    .unwrap();
                let mut req = Request::new(Method::Post, url);
                req.set_body(body);

                // Schedule the sender to the reactor.
                handle.spawn(tx.send_all(body_stream).then(|_| Err(())));
                core.run({
                    let client = Client::new(&handle);
                    client.request(req).and_then(|_| Ok(()))
                }).unwrap();
            });
        }

        let thd = {
            let prefix = prefix.clone();
            let flow_id = flow_id.clone();
            thread::spawn(move || {
                let mut core = Core::new().unwrap();
                let handle = &core.handle();
                let prefix = &prefix;
                let flow_id = &flow_id;

                let req = Request::new(
                    Method::Get,
                    format!("{}/flow/{}/pull", prefix, flow_id).parse().unwrap(),
                );

                let mut park_once = true;
                core.run({
                    let client = Client::new(handle);
                    client.request(req).and_then(|res| {
                        assert_eq!(res.status(), StatusCode::Ok);
                        res.body()
                            .for_each(move |_| {
                                if park_once {
                                    park_once = false;
                                    thread::park();
                                }
                                Ok(())
                            })
                            .boxed2()
                    })
                }).unwrap();
            })
        };

        while send_rx.recv_timeout(Duration::from_millis(5000)).is_ok() {}

        let status = req_status(prefix, flow_id).1.unwrap();

        for idx in status.tail..status.next {
            assert_eq!(req_fetch(prefix, flow_id, idx).0, StatusCode::Ok);
        }

        thd.thread().unpark();
        thd.join().unwrap();
    }

    #[test]
    fn overload() {
        let prefix = &spawn_server();
        let mut core = Core::new().unwrap();
        let handle = &core.handle();

        let (ref flow_id, ref token) = create_flow(prefix, DEFL_FLOW_PARAM);
        for _ in 1..32 {
            create_flow(prefix, DEFL_FLOW_PARAM);
        }

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(DEFL_FLOW_PARAM);
        req.headers_mut()
            .set(ContentLength(DEFL_FLOW_PARAM.len() as u64));
        core.run({
            let client = Client::new(handle);
            client.request(req).and_then(|res| {
                assert_eq!(res.status(), StatusCode::ServiceUnavailable);
                Ok(())
            })
        }).unwrap();

        thread::sleep(Duration::from_secs(4));
        assert_eq!(
            req_push(prefix, flow_id, token, b"Hello"),
            (StatusCode::Ok, None)
        );
        thread::sleep(Duration::from_secs(4));
        for _ in 0..31 {
            create_flow(prefix, DEFL_FLOW_PARAM);
        }

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(DEFL_FLOW_PARAM);
        req.headers_mut()
            .set(ContentLength(DEFL_FLOW_PARAM.len() as u64));
        core.run({
            let client = Client::new(handle);
            client.request(req).and_then(|res| {
                assert_eq!(res.status(), StatusCode::ServiceUnavailable);
                Ok(())
            })
        }).unwrap();
    }

    #[test]
    fn handle_status() {
        let prefix = &spawn_server();
        let (ref flow_id, ref token) = create_flow(prefix, DEFL_FLOW_PARAM);
        let fake_id = "bdc62e9323003d0f5cb44c8c745a0470";
        assert_eq!(req_status(prefix, fake_id), (StatusCode::NotFound, None));
        assert_eq!(
            req_push(prefix, flow_id, token, b"Hello"),
            (StatusCode::Ok, None)
        );
        assert_eq!(
            req_push(prefix, flow_id, token, b"Hello"),
            (StatusCode::Ok, None)
        );
        assert_eq!(
            req_status(prefix, flow_id),
            (
                StatusCode::Ok,
                Some(StatusResponse {
                    tail: 0,
                    next: 2,
                    dropped: 0,
                    pushed: 10,
                }),
            )
        );
    }

    #[test]
    fn fixed_length() {
        let prefix = &spawn_server();
        let mut core = Core::new().unwrap();
        let handle = &core.handle();

        let param = serde_json::to_vec(&NewRequest {
            size: Some(5),
            preserve_mode: false,
        }).unwrap();
        let (ref flow_id, ref token) = create_flow(prefix, &String::from_utf8(param).unwrap());

        assert_eq!(
            req_push(prefix, flow_id, token, b"Hel"),
            (StatusCode::Ok, None)
        );
        assert_eq!(
            req_push(prefix, flow_id, token, b"World"),
            (StatusCode::BadRequest, Some("Not Ready".to_string()),)
        );
        assert_eq!(
            req_push(prefix, flow_id, token, b"lo"),
            (StatusCode::Ok, None)
        );
        assert_eq!(
            req_close(prefix, flow_id, token),
            (StatusCode::BadRequest, Some("Closed".to_string()))
        );

        let req = Request::new(
            Method::Get,
            format!("{}/flow/{}/pull", prefix, flow_id).parse().unwrap(),
        );
        core.run({
            let client = Client::new(handle);
            client.request(req).and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                assert_eq!(res.headers().get::<ContentLength>().unwrap().0, 5);
                res.body().concat2().and_then(|body| {
                    assert_eq!(body.to_vec(), b"Hello".to_vec());
                    Ok(())
                })
            })
        }).unwrap();

        let param = serde_json::to_vec(&NewRequest {
            size: Some(0),
            preserve_mode: false,
        }).unwrap();
        let (ref flow_id, ref token) = create_flow(prefix, &String::from_utf8(param).unwrap());

        assert_eq!(
            req_push(prefix, flow_id, token, b"A"),
            (StatusCode::BadRequest, Some("Not Ready".to_string()),)
        );
        assert_eq!(req_close(prefix, flow_id, token), (StatusCode::Ok, None));
    }

    #[test]
    fn tls_service() {
        #[cfg(target_os = "windows")]
        let tls_acceptor = tls::build_tls_from_pfx("./tests/cert.p12");
        #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "ios")))]
        let tls_acceptor = tls::build_tls_from_pem("./tests/cert.pem", "./tests/private.pem");
        let (bind_addr, _) = start_service(
            "127.0.0.1:0".parse().unwrap(),
            1,
            Some(32),
            Some(Duration::from_secs(6)),
            MAX_CAPACITY,
            MAX_CAPACITY,
            Some(tls_acceptor),
        );

        let prefix = format!("https://127.0.0.1:{}", bind_addr.port());
        let mut core = Core::new().unwrap();

        let rootca = {
            let mut cert_file = File::open("./tests/cert.der").unwrap();
            let mut buf = Vec::new();
            cert_file.read_to_end(&mut buf).unwrap();
            Certificate::from_der(&buf).unwrap()
        };
        let mut tls_builder = TlsConnector::builder().unwrap();
        tls_builder.add_root_certificate(rootca).unwrap();
        let tls_connector = tls_builder.build().unwrap();
        let mut connector = HttpsConnector {
            tls: Arc::new(tls_connector),
            http: HttpConnector::new(2, &core.handle()),
        };
        connector.http.enforce_http(false);
        let client = Client::configure()
            .connector(connector)
            .build(&core.handle());

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(DEFL_FLOW_PARAM);
        req.headers_mut()
            .set(ContentLength(DEFL_FLOW_PARAM.len() as u64));
        core.run(client.request(req).and_then(|res| {
            assert_eq!(res.status(), StatusCode::Ok);
            res.body().concat2().and_then(|body| {
                let data = serde_json::from_slice::<NewResponse>(&body).unwrap();
                assert!(
                    Regex::new("^[a-f0-9]{32}$")
                        .unwrap()
                        .find(&data.id)
                        .is_some()
                );
                assert!(
                    Regex::new("^[a-f0-9]{64}$")
                        .unwrap()
                        .find(&data.token)
                        .is_some()
                );
                Ok(())
            })
        })).unwrap();
    }

    struct HttpsConnector {
        tls: Arc<TlsConnector>,
        http: HttpConnector,
    }

    impl Service for HttpsConnector {
        type Request = Uri;
        type Response = TlsStream<TcpStream>;
        type Error = IoError;
        type Future = Box<Future<Item = Self::Response, Error = IoError>>;

        fn call(&self, uri: Uri) -> Self::Future {
            let tls_connector = self.tls.clone();
            Box::new(self.http.call(uri).and_then(move |io| {
                tls_connector
                    .connect_async("example.com", io)
                    .map_err(|err| IoError::new(io::ErrorKind::Other, err))
            }))
        }
    }

    #[test]
    fn multi_workers() {
        start_service(
            "127.0.0.1:0".parse().unwrap(),
            4,
            None,
            None,
            MAX_CAPACITY,
            MAX_CAPACITY,
            None,
        );
    }

    #[test]
    fn preserve_mode() {
        let prefix = &spawn_server();
        let mut core = Core::new().unwrap();
        let handle = &core.handle();

        let param = serde_json::to_vec(&NewRequest {
            size: Some(MAX_CAPACITY * 4),
            preserve_mode: true,
        }).unwrap();
        let (ref flow_id, ref token) = create_flow(prefix, &String::from_utf8(param).unwrap());

        let thd1 = {
            let prefix = prefix.to_owned();
            let flow_id = flow_id.to_owned();
            let token = token.to_owned();
            thread::spawn(move || {
                let prefix = &prefix;
                let flow_id = &flow_id;
                let token = &token;
                let payload = vec![0u8; flow::REF_SIZE];
                for _ in 0..(MAX_CAPACITY * 2) / flow::REF_SIZE as u64 {
                    assert_eq!(
                        req_push(prefix, flow_id, token, &payload),
                        (StatusCode::Ok, None)
                    )
                }
            })
        };

        let req = Request::new(
            Method::Get,
            format!("{}/flow/{}/pull", prefix, flow_id).parse().unwrap(),
        );
        core.run({
            let client = Client::new(handle);
            client.request(req).and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                assert_eq!(
                    res.headers().get::<AcceptRanges>().unwrap(),
                    &AcceptRanges(vec![RangeUnit::Bytes])
                );
                let mut partial_len = 0;
                res.body()
                    .take_while(move |chunk| {
                        partial_len += chunk.len();
                        Ok((partial_len as u64) < MAX_CAPACITY)
                    })
                    .for_each(|_| Ok(()))
            })
        }).unwrap();

        thd1.join().unwrap();

        let status = req_status(prefix, flow_id).1.unwrap();

        let thd2 = {
            let prefix = prefix.to_owned();
            let flow_id = flow_id.to_owned();
            let token = token.to_owned();
            thread::spawn(move || {
                let prefix = &prefix;
                let flow_id = &flow_id;
                let token = &token;
                let payload = vec![1u8; flow::REF_SIZE];
                for _ in 0..(MAX_CAPACITY * 2) / flow::REF_SIZE as u64 {
                    assert_eq!(
                        req_push(prefix, flow_id, token, &payload),
                        (StatusCode::Ok, None)
                    )
                }
            })
        };

        let mut req = Request::new(
            Method::Get,
            format!("{}/flow/{}/pull", prefix, flow_id).parse().unwrap(),
        );
        req.headers_mut()
            .set(Range::Bytes(vec![ByteRangeSpec::Last(0)]));
        core.run({
            let client = Client::new(handle);
            client.request(req).and_then(|res| {
                assert_eq!(res.status(), StatusCode::NotFound);
                Ok(())
            })
        }).unwrap();

        let mut req = Request::new(
            Method::Get,
            format!("{}/flow/{}/pull", prefix, flow_id).parse().unwrap(),
        );
        req.headers_mut().set(Range::Bytes(vec![
            ByteRangeSpec::FromTo(MAX_CAPACITY * 4, MAX_CAPACITY * 4),
            ByteRangeSpec::FromTo(MAX_CAPACITY * 4, MAX_CAPACITY * 4),
        ]));
        core.run({
            let client = Client::new(handle);
            client.request(req).and_then(|res| {
                assert_eq!(res.status(), StatusCode::NotFound);
                Ok(())
            })
        }).unwrap();

        let mut req = Request::new(
            Method::Get,
            format!("{}/flow/{}/pull", prefix, flow_id).parse().unwrap(),
        );
        req.headers_mut()
            .set(Range::Bytes(vec![ByteRangeSpec::AllFrom(0)]));
        core.run({
            let client = Client::new(handle);
            client.request(req).and_then(|res| {
                assert_eq!(res.status(), StatusCode::NotFound);
                Ok(())
            })
        }).unwrap();

        let mut req = Request::new(
            Method::Get,
            format!("{}/flow/{}/pull", prefix, flow_id).parse().unwrap(),
        );
        req.headers_mut().set(Range::Bytes(vec![
            ByteRangeSpec::FromTo(MAX_CAPACITY * 4, MAX_CAPACITY * 4),
        ]));
        core.run({
            let client = Client::new(handle);
            client.request(req).and_then(|res| {
                assert_eq!(res.status(), StatusCode::RangeNotSatisfiable);
                assert_eq!(
                    res.headers().get::<ContentRange>().unwrap(),
                    &ContentRange(ContentRangeSpec::Bytes {
                        range: None,
                        instance_length: Some(MAX_CAPACITY * 4),
                    })
                );
                Ok(())
            })
        }).unwrap();

        let range_start = (status.pushed + status.dropped) / 2 + 1;
        let mut req = Request::new(
            Method::Get,
            format!("{}/flow/{}/pull", prefix, flow_id).parse().unwrap(),
        );
        req.headers_mut()
            .set(Range::Bytes(vec![ByteRangeSpec::AllFrom(range_start)]));
        core.run({
            let client = Client::new(handle);
            client.request(req).and_then(|res| {
                assert_eq!(res.status(), StatusCode::PartialContent);
                assert_eq!(
                    res.headers().get::<ContentRange>().unwrap(),
                    &ContentRange(ContentRangeSpec::Bytes {
                        range: Some((range_start, MAX_CAPACITY * 4 - 1)),
                        instance_length: Some(MAX_CAPACITY * 4),
                    })
                );
                res.body().concat2().and_then(|body| {
                    assert_eq!(body.len() as u64, MAX_CAPACITY * 4 - range_start);
                    Ok(())
                })
            })
        }).unwrap();

        thd2.join().unwrap();
    }
}

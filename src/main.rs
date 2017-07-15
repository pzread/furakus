#[macro_use]
extern crate language_tags;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
extern crate bytes;
extern crate dotenv;
extern crate futures;
extern crate hyper;
extern crate native_tls;
extern crate openssl;
extern crate regex;
extern crate ring;
extern crate serde;
extern crate serde_json;
extern crate tokio_core as tokio;
extern crate tokio_tls;
extern crate url;
extern crate uuid;
mod auth;
mod flow;
mod pool;
mod utils;

use auth::{Authorizer, HMACAuthorizer};
use dotenv::dotenv;
use flow::Flow;
use futures::{Future, Sink, Stream, future, stream};
use hyper::{Method, StatusCode};
use hyper::header::{AccessControlAllowMethods, AccessControlAllowOrigin, CacheControl,
                    CacheDirective, Charset, ContentDisposition, ContentLength, ContentType,
                    DispositionParam, DispositionType};
use hyper::server::{Http, Request, Response, Service};
use native_tls::{TlsAcceptor, TlsAcceptorBuilder};
use native_tls::backend::openssl::TlsAcceptorBuilderExt;
use openssl::pkey::PKey;
use openssl::ssl::{SslAcceptorBuilder, SslMethod};
use openssl::x509::X509;
use pool::Pool;
use regex::Regex;
use serde::de::DeserializeOwned;
use std::{env, thread};
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::{Arc, Barrier, RwLock};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::reactor::{self, Core};
use tokio_tls::TlsAcceptorExt;

#[derive(Debug)]
pub enum Error {
    Invalid,
    NotReady,
    Internal(hyper::Error),
}

struct FlowService {
    pool: Arc<RwLock<Pool>>,
    remote: reactor::Remote,
    meta_capacity: u64,
    data_capacity: u64,
    authorizer: Arc<Authorizer>,
}

#[derive(Serialize, Deserialize)]
struct NewRequest {
    pub size: Option<u64>,
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
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct ErrorResponse {
    pub message: String,
}

type ResponseFuture = future::BoxFuture<Response, hyper::Error>;

impl FlowService {
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
        }
    }

    fn check_authorization(&self, flow_id: &str, token: &str) -> bool {
        self.authorizer.verify(flow_id, token).is_ok()
    }

    fn parse_request_querystring(req: &Request) -> url::form_urlencoded::Parse {
        url::form_urlencoded::parse(req.query().unwrap_or("").as_bytes())
    }

    fn parse_request_parameter<T>(req: Request) -> future::BoxFuture<T, Error>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let content_length = match req.headers().get() {
            Some(&ContentLength(length)) => length,
            None => return future::err(Error::Invalid).boxed(),
        };
        if content_length == 0u64 || content_length > 4096u64 {
            return future::err(Error::Invalid).boxed();
        }
        req.body()
            .concat2()
            .map_err(|err| Error::Internal(err))
            .and_then(|body| serde_json::from_slice::<T>(&body).map_err(|_| Error::Invalid))
            .boxed()
    }

    fn response_ok() -> Response {
        Response::new().with_header(ContentLength(0))
    }

    fn response_error(error: &str) -> Response {
        let body = serde_json::to_string(&ErrorResponse { message: error.to_owned() }).unwrap();
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
                });
                let flow_id = flow_ptr.read().unwrap().id.to_owned();
                {
                    let mut pool = pool_ptr.write().unwrap();
                    pool.insert(flow_ptr).map(|_| flow_id.clone()).map_err(|_| Error::NotReady)
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
            .boxed()
    }

    fn handle_push(&self, req: Request, route: regex::Captures) -> ResponseFuture {
        let token =
            match Self::parse_request_querystring(&req).find(|&(ref key, _)| key == "token") {
                Some((_, token)) => token.into_owned(),
                None => return future::ok(Self::response_error("Missing Token")).boxed(),
            };
        let flow_id = route.get(1).unwrap().as_str();
        if !self.check_authorization(flow_id, &token) {
            return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed();
        }
        let flow_ptr = match self.pool.read().unwrap().get(flow_id) {
            Some(flow) => flow.clone(),
            None => return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed(),
        };
        req.body()
            .for_each(move |chunk| {
                let mut flow = flow_ptr.write().unwrap();
                flow.push(chunk.into_bytes()).map(|_| ()).map_err(
                    |_| hyper::error::Error::Incomplete,
                )
            })
            .and_then(|_| Ok(Self::response_ok()))
            .or_else(|_| Ok(Self::response_error("Not Ready")))
            .boxed()
    }

    fn handle_eof(&self, req: Request, route: regex::Captures) -> ResponseFuture {
        let token =
            match Self::parse_request_querystring(&req).find(|&(ref key, _)| key == "token") {
                Some((_, token)) => token.into_owned(),
                None => return future::ok(Self::response_error("Missing Token")).boxed(),
            };
        let flow_id = route.get(1).unwrap().as_str();
        if !self.check_authorization(flow_id, &token) {
            return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed();
        }
        let flow_ptr = match self.pool.read().unwrap().get(flow_id) {
            Some(flow) => flow.clone(),
            None => return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed(),
        };
        {
            let mut flow = flow_ptr.write().unwrap();
            flow.close()
                .then(|result| match result {
                    Ok(_) => Ok(Self::response_ok()),
                    Err(flow::Error::Invalid) => Ok(Self::response_error("Closed")),
                    _ => Ok(Response::new().with_status(StatusCode::InternalServerError)),
                })
                .boxed()
        }
    }

    fn handle_status(&self, _req: Request, route: regex::Captures) -> ResponseFuture {
        let flow_id = route.get(1).unwrap().as_str();
        let flow_ptr = match self.pool.read().unwrap().get(flow_id) {
            Some(flow) => flow.clone(),
            None => return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed(),
        };
        let body = {
            let flow = flow_ptr.read().unwrap();
            let (tail, next) = flow.get_range();
            serde_json::to_string(&StatusResponse { tail, next }).unwrap()
        }.into_bytes();
        future::ok(
            Response::new()
                .with_header(ContentType::json())
                .with_header(ContentLength(body.len() as u64))
                .with_body(body),
        ).boxed()
    }

    fn handle_fetch(&self, _req: Request, route: regex::Captures) -> ResponseFuture {
        let flow_id = route.get(1).unwrap().as_str();
        let chunk_index: u64 = match route.get(2).unwrap().as_str().parse() {
            Ok(index) => index,
            Err(_) => return future::ok(Self::response_error("Invalid Parameter")).boxed(),
        };
        let flow_ptr = match self.pool.read().unwrap().get(flow_id) {
            Some(flow) => flow.clone(),
            None => return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed(),
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
                        flow::Error::Eof | flow::Error::Dropped => StatusCode::NotFound,
                        _ => StatusCode::InternalServerError,
                    };
                    future::ok(Response::new().with_status(status))
                })
                .boxed()
        }
    }

    fn handle_pull(&self, req: Request, route: regex::Captures) -> ResponseFuture {
        let opt_filename = Self::parse_request_querystring(&req)
            .find(|&(ref key, _)| key == "filename")
            .map(|(_, token)| token.into_owned());
        let flow_id = route.get(1).unwrap().as_str();
        let flow_ptr = match self.pool.read().unwrap().get(flow_id) {
            Some(flow) => flow.clone(),
            None => return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed(),
        };
        let (tx, body) = hyper::Body::pair();
        let mut response = Response::new()
            .with_header(ContentType::octet_stream())
            .with_header(CacheControl(vec![CacheDirective::NoCache]))
            .with_body(body);
        if let Some(filename) = opt_filename {
            let content_disp = ContentDisposition {
                disposition: DispositionType::Attachment,
                parameters: vec![
                    DispositionParam::Filename(
                        Charset::Ext("UTF-8".into()),
                        Some(langtag!(en)),
                        filename.as_bytes().to_vec()
                    ),
                ],
            };
            response.headers_mut().set(content_disp);
        }
        let (pull_fut, tail_index) = {
            let flow = flow_ptr.read().unwrap();
            let (tail_index, _) = flow.get_range();
            let config = flow.get_config();
            if let Some(length) = config.length {
                // Only set content length when the flow is still complete.
                if tail_index == 0 {
                    response.headers_mut().set(ContentLength(length));
                }
            }
            (flow.pull(tail_index, None), tail_index)
        };
        let remote = self.remote.clone();
        pull_fut
            .and_then(move |chunk| {
                let body_stream = stream::unfold(Some((tail_index, chunk)), move |previous| {
                    // Check if the flow is EOF.
                    if let Some((prev_index, prev_chunk)) = previous {
                        let flow = flow_ptr.read().unwrap();
                        let chunk_index = prev_index + 1;
                        let hyper_chunk = Ok(prev_chunk.into());
                        let fut = flow.pull(chunk_index, None).then(move |ret| match ret {
                            Ok(chunk) => future::ok((hyper_chunk, Some((chunk_index, chunk)))),
                            Err(_) => future::ok((hyper_chunk, None)),
                        });
                        Some(fut)
                    } else {
                        None
                    }
                });
                // Schedule the sender to the reactor.
                remote.spawn(move |_| {
                    tx.send_all(body_stream).and_then(|(mut tx, _)| tx.close()).then(|_| Ok(()))
                });
                Ok(response)
            })
            .or_else(|_| Ok(Response::new().with_status(StatusCode::NotFound)))
            .boxed()
    }
}

impl Service for FlowService {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = ResponseFuture;

    fn call(&self, req: Request) -> Self::Future {
        lazy_static! {
            static ref PATTERN_NEW: Regex = Regex::new(r"^/new$").unwrap();
            static ref PATTERN_PUSH: Regex = Regex::new(r"^/flow/([a-f0-9]{32})/push$").unwrap();
            static ref PATTERN_EOF: Regex = Regex::new(r"^/flow/([a-f0-9]{32})/eof$").unwrap();
            static ref PATTERN_STATUS: Regex = Regex::new(r"^/flow/([a-f0-9]{32})/status$")
                .unwrap();
            static ref PATTERN_FETCH: Regex = Regex::new(r"^/flow/([a-f0-9]{32})/fetch/(\d+)$")
                .unwrap();
            static ref PATTERN_PULL: Regex = Regex::new(r"^/flow/([a-f0-9]{32})/pull$").unwrap();
        }

        let path = &req.path().to_owned();
        match req.method() {
            &Method::Post => {
                if let Some(route) = PATTERN_NEW.captures(path) {
                    self.handle_new(req, route)
                } else if let Some(route) = PATTERN_PUSH.captures(path) {
                    self.handle_push(req, route)
                } else if let Some(route) = PATTERN_EOF.captures(path) {
                    self.handle_eof(req, route)
                } else if let Some(route) = PATTERN_STATUS.captures(path) {
                    self.handle_status(req, route)
                } else {
                    future::ok(Response::new().with_status(StatusCode::NotFound)).boxed()
                }
            }
            &Method::Put => {
                if let Some(route) = PATTERN_PUSH.captures(path) {
                    self.handle_push(req, route)
                } else {
                    future::ok(Response::new().with_status(StatusCode::NotFound)).boxed()
                }
            }
            &Method::Get => {
                if let Some(route) = PATTERN_FETCH.captures(path) {
                    self.handle_fetch(req, route)
                } else if let Some(route) = PATTERN_PULL.captures(path) {
                    self.handle_pull(req, route)
                } else {
                    future::ok(Response::new().with_status(StatusCode::NotFound)).boxed()
                }
            }
            &Method::Options => {
                future::ok(Response::new().with_header(AccessControlAllowMethods(
                    vec![Method::Post, Method::Put, Method::Get, Method::Options],
                ))).boxed()
            }
            _ => future::ok(Response::new().with_status(StatusCode::MethodNotAllowed)).boxed(),
        }.map(|res| res.with_header(AccessControlAllowOrigin::Any))
            .boxed()
    }
}

fn config_tls(cert_path: &str, priv_path: &str) -> TlsAcceptor {
    let fullchain = {
        let cert_file = File::open(cert_path).unwrap();
        let mut buf = Vec::new();
        BufReader::new(cert_file).read_to_end(&mut buf).unwrap();
        X509::stack_from_pem(&buf).unwrap()
    };
    let privkey = {
        let priv_file = File::open(priv_path).unwrap();
        let mut buf = Vec::new();
        BufReader::new(priv_file).read_to_end(&mut buf).unwrap();
        PKey::private_key_from_pem(&buf).unwrap()
    };
    let cert = fullchain.first().unwrap();
    let builder =
        SslAcceptorBuilder::mozilla_modern(SslMethod::tls(), &privkey, &cert, &fullchain[1..])
            .unwrap();
    TlsAcceptorBuilder::from_openssl(builder).build().unwrap()
}

fn start_service(
    addr: std::net::SocketAddr,
    num_worker: usize,
    pool_size: Option<usize>,
    deactive_timeout: Option<Duration>,
    meta_capacity: u64,
    data_capacity: u64,
    tls_config: Option<TlsAcceptor>,
    blocking: bool,
) -> Option<std::net::SocketAddr> {
    if !cfg!(unix) && num_worker > 1 {
        panic!("Multi-workers isn't supported on Windows");
    }

    let upstream_listener = std::net::TcpListener::bind(&addr).unwrap();
    let pool_ptr = Pool::new(pool_size, deactive_timeout);
    let auth_ptr = Arc::new(HMACAuthorizer::new());
    let mut workers = Vec::with_capacity(num_worker);
    let barrier = Arc::new(Barrier::new(num_worker.checked_add(1).unwrap()));
    let tls_ptr = tls_config.map(|tls_config| Arc::new(tls_config));

    for idx in 0..num_worker {
        let addr = addr.clone();
        let listener = upstream_listener.try_clone().unwrap();
        let barrier = barrier.clone();
        let pool_ptr = pool_ptr.clone();
        let auth_ptr = auth_ptr.clone();
        let tls_acceptor = tls_ptr.clone();
        let worker = thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let handle = core.handle();
            let remote = core.remote();
            let listener = TcpListener::from_listener(listener, &addr, &handle).unwrap();
            let acceptor: Box<Future<Item = _, Error = _>> =
                if let Some(tls_acceptor) = tls_acceptor {
                    Box::new(listener.incoming().for_each(move |(io, addr)| {
                        let handle = handle.clone();
                        let remote = remote.clone();
                        let pool_ptr = pool_ptr.clone();
                        let auth_ptr = auth_ptr.clone();
                        tls_acceptor
                            .accept_async(io)
                            .and_then(move |io| {
                                let service = FlowService::new(
                                    pool_ptr,
                                    remote,
                                    meta_capacity,
                                    data_capacity,
                                    auth_ptr,
                                );
                                Http::new().bind_connection(&handle, io, addr, service);
                                Ok(())
                            })
                            .or_else(|_| Ok(()))
                    }))
                } else {
                    Box::new(listener.incoming().for_each(|(io, addr)| {
                        let service = FlowService::new(
                            pool_ptr.clone(),
                            remote.clone(),
                            meta_capacity,
                            data_capacity,
                            auth_ptr.clone(),
                        );
                        Http::new().bind_connection(&handle, io, addr, service);
                        Ok(())
                    }))
                };
            barrier.wait();
            println!("Worker #{} is started.", idx);
            core.run(acceptor).unwrap();
        });
        workers.push(worker);
    }

    barrier.wait();

    if blocking {
        for worker in workers {
            worker.join().unwrap();
        }
        unreachable!();
    } else {
        Some(upstream_listener.local_addr().unwrap())
    }
}

fn main() {
    dotenv().ok();
    let addr: std::net::SocketAddr = env::var("SERVER_ADDRESS").unwrap().parse().unwrap();
    let num_worker: usize = env::var("NUM_WORKER").unwrap().parse().unwrap();
    let pool_size: usize = env::var("POOL_SIZE").unwrap().parse().unwrap();
    let deactive_timeout: u64 = env::var("DEACTIVE_TIMEOUT").unwrap().parse().unwrap();
    let meta_capacity: u64 = env::var("META_CAPACITY").unwrap().parse().unwrap();
    let data_capacity: u64 = env::var("DATA_CAPACITY").unwrap().parse().unwrap();
    let tls_config = config_tls(&env::var("TLS_CERT").unwrap(), &env::var("TLS_PRIVATE").unwrap());
    start_service(
        addr,
        num_worker,
        Some(pool_size),
        Some(Duration::from_secs(deactive_timeout)),
        meta_capacity,
        data_capacity,
        Some(tls_config),
        true,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow;
    use futures::{Future, Sink, Stream, future, stream};
    use hyper;
    use hyper::{Method, Request, StatusCode, Uri};
    use hyper::client::{Client, HttpConnector};
    use hyper::header::{AccessControlAllowMethods, AccessControlAllowOrigin, CacheControl,
                        CacheDirective, ContentDisposition, ContentLength};
    use hyper::server::Service;
    use native_tls::{Certificate, TlsConnector};
    use regex::Regex;
    use serde_json;
    use std::{io, str, thread};
    use std::collections::HashSet;
    use std::sync::mpsc;
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio::reactor::Core;
    use tokio_tls::{TlsConnectorExt, TlsStream};
    use url;

    const MAX_CAPACITY: u64 = 1048576;

    fn spawn_server() -> (String, String) {
        let port = start_service(
            "127.0.0.1:0".parse().unwrap(),
            1,
            Some(32),
            Some(Duration::from_secs(6)),
            MAX_CAPACITY,
            MAX_CAPACITY,
            None,
            false,
        ).unwrap()
            .port();
        (format!("http://127.0.0.1:{}", port), format!("127.0.0.1:{}", port))
    }

    fn create_flow(prefix: &str, param: &str) -> (String, String) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(param.to_owned());
        req.headers_mut().set(ContentLength(param.len() as u64));

        let data = core.run(client.request(req).and_then(|res| {
            assert_eq!(res.status(), StatusCode::Ok);
            res.body().concat2().and_then(
                |body| Ok(serde_json::from_slice::<NewResponse>(&body).unwrap()),
            )
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
            format!("{}/flow/{}/push?token={}", prefix, flow_id, token).parse().unwrap(),
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
                    .boxed()
            } else {
                future::ok(None).boxed()
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
            format!("{}/flow/{}/eof?token={}", prefix, flow_id, token).parse().unwrap(),
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
                    .boxed()
            } else {
                future::ok(None).boxed()
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
            format!("{}/flow/{}/status", prefix, flow_id).parse().unwrap(),
        );

        let (status_code, response) = core.run(client.request(req).and_then(|res| {
            let status_code = res.status();
            let fut = if status_code == StatusCode::Ok {
                res.body().concat2().and_then(|body| Ok(Some(body.to_vec()))).boxed()
            } else {
                future::ok(None).boxed()
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
            format!("{}/flow/{}/fetch/{}", prefix, flow_id, index).parse().unwrap(),
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
                res.body().concat2().and_then(|body| Ok(Some(body.to_vec()))).boxed()
            } else {
                future::ok(None).boxed()
            };
            fut.and_then(move |body| Ok((status_code, body)))
        })).unwrap();

        (status_code, response)
    }

    fn req_pull(prefix: &str, flow_id: &str) -> (StatusCode, Option<Vec<u8>>) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let req =
            Request::new(Method::Get, format!("{}/flow/{}/pull", prefix, flow_id).parse().unwrap());

        let (status_code, response) = core.run(client.request(req).and_then(|res| {
            let status_code = res.status();
            if status_code == StatusCode::Ok {
                assert_eq!(
                    res.headers().get::<CacheControl>().unwrap(),
                    &CacheControl(vec![CacheDirective::NoCache])
                );
            }
            let fut = if status_code == StatusCode::Ok {
                res.body().concat2().and_then(|body| Ok(Some(body.to_vec()))).boxed()
            } else {
                future::ok(None).boxed()
            };
            fut.and_then(move |body| Ok((status_code, body)))
        })).unwrap();

        (status_code, response)
    }

    fn check_error_response(res: Response, error: &str) -> future::BoxFuture<(), hyper::Error> {
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
            .boxed()
    }

    #[test]
    fn validate_route() {
        let (ref prefix, _) = spawn_server();
        let (ref flow_id, _) = create_flow(prefix, r#"{}"#);

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

        let req = Request::new(Method::Post, format!("{}/new/../new", prefix).parse().unwrap());
        check_status(req, StatusCode::NotFound);

        let req = Request::new(Method::Post, format!("{}//new", prefix).parse().unwrap());
        check_status(req, StatusCode::NotFound);

        let req = Request::new(Method::Post, format!("{}/new/", prefix).parse().unwrap());
        check_status(req, StatusCode::NotFound);

        let req =
            Request::new(Method::Post, format!("{}/{}/push", prefix, flow_id).parse().unwrap());
        check_status(req, StatusCode::NotFound);

        let req = Request::new(
            Method::Post,
            format!("{}/flow/{}/pusha", prefix, flow_id).parse().unwrap(),
        );
        check_status(req, StatusCode::NotFound);

        let req = Request::new(
            Method::Post,
            format!("{}/flow/{}/eofa", prefix, flow_id).parse().unwrap(),
        );
        check_status(req, StatusCode::NotFound);

        let req = Request::new(
            Method::Post,
            format!("{}/flow/{}/statusa", prefix, flow_id).parse().unwrap(),
        );
        check_status(req, StatusCode::NotFound);
        let req = Request::new(
            Method::Get,
            format!("{}/flow/{}/pullb", prefix, flow_id).parse().unwrap(),
        );
        check_status(req, StatusCode::NotFound);

        let req = Request::new(
            Method::Get,
            format!("{}/flow/{}/fetchb", prefix, flow_id).parse().unwrap(),
        );
        check_status(req, StatusCode::NotFound);

        let req = Request::new(Method::Put, format!("{}/new", prefix).parse().unwrap());
        check_status(req, StatusCode::NotFound);

        let req = Request::new(Method::Options, format!("{}/abc", prefix).parse().unwrap());
        let res = check_status(req, StatusCode::Ok);
        let allow_methods: HashSet<_> = res.headers()
            .get::<AccessControlAllowMethods>()
            .unwrap()
            .clone()
            .0
            .into_iter()
            .collect();
        assert_eq!(
            allow_methods,
            vec![Method::Get, Method::Post, Method::Put, Method::Options].into_iter().collect()
        );

        let req = Request::new(Method::Patch, format!("{}/new", prefix).parse().unwrap());
        check_status(req, StatusCode::MethodNotAllowed);
    }

    #[test]
    fn handle_new() {
        let prefix = &spawn_server().0;
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{}"#);
        req.headers_mut().set(ContentLength(2));
        core.run(client.request(req).and_then(|res| {
            assert_eq!(res.status(), StatusCode::Ok);
            res.body().concat2().and_then(|body| {
                let data = serde_json::from_slice::<NewResponse>(&body).unwrap();
                assert!(Regex::new("^[a-f0-9]{32}$").unwrap().find(&data.id).is_some());
                assert!(Regex::new("^[a-f0-9]{64}$").unwrap().find(&data.token).is_some());
                Ok(())
            })
        })).unwrap();

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{"size": 4096}"#);
        req.headers_mut().set(ContentLength(14));
        core.run(client.request(req).and_then(|res| {
            assert_eq!(res.status(), StatusCode::Ok);
            res.body().concat2().and_then(|body| {
                let data = serde_json::from_slice::<NewResponse>(&body).unwrap();
                assert!(Regex::new("^[a-f0-9]{32}$").unwrap().find(&data.id).is_some());
                assert!(Regex::new("^[a-f0-9]{64}$").unwrap().find(&data.token).is_some());
                Ok(())
            })
        })).unwrap();

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{}"#);
        core.run(
            client.request(req).and_then(|res| check_error_response(res, "Invalid Parameter")),
        ).unwrap();

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{"size": 4O96}"#);
        req.headers_mut().set(ContentLength(14));
        core.run(
            client.request(req).and_then(|res| check_error_response(res, "Invalid Parameter")),
        ).unwrap();

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(vec![65u8; 4097]);
        req.headers_mut().set(ContentLength(4097));
        core.run(
            client.request(req).and_then(|res| check_error_response(res, "Invalid Parameter")),
        ).unwrap();

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{"size": 4096}"#);
        req.headers_mut().set(ContentLength(4097));
        core.run(
            client.request(req).and_then(|res| check_error_response(res, "Invalid Parameter")),
        ).unwrap();

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        let mut body = vec![65u8; 4096];
        body.extend_from_slice(r#"{"size": 4096}"#.as_bytes());
        req.set_body(body);
        req.headers_mut().set(ContentLength(4096));
        core.run(
            client.request(req).and_then(|res| check_error_response(res, "Invalid Parameter")),
        ).unwrap();

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        let mut body = r#"{"size": 4096}"#.to_string();
        body.extend(&vec![' '; 4096]);
        req.set_body(body);
        req.headers_mut().set(ContentLength(4096));
        core.run(client.request(req).and_then(|res| {
            assert_eq!(res.status(), StatusCode::Ok);
            res.body().concat2().and_then(|body| {
                let data = serde_json::from_slice::<NewResponse>(&body).unwrap();
                assert!(Regex::new("^[a-f0-9]{32}$").unwrap().find(&data.id).is_some());
                assert!(Regex::new("^[a-f0-9]{64}$").unwrap().find(&data.token).is_some());
                Ok(())
            })
        })).unwrap();
    }

    #[test]
    fn handle_push_fetch() {
        let prefix = &spawn_server().0;
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let (ref flow_id, ref token) = create_flow(prefix, r#"{}"#);
        let fake_id = "bdc62e9323003d0f5cb44c8c745a0470";
        let mal_token = "sjlc(84c84w47wq87a";
        let fake_token = "bdc62e9323003d0f5cb44c8c745a0470bdc62e9323003d0f5cb44c8c745a0470";
        let payload1: &[u8] = b"The quick brown fox jumps\nover the lazy dog";
        let payload2: &[u8] = b"The guick yellow fox jumps\nover the fast cat";

        // The empty chunk should be ignored.
        // No content length.
        let req = Request::new(
            Method::Post,
            format!("{}/flow/{}/push?token={}", prefix, flow_id, token).parse().unwrap(),
        );
        core.run(client.request(req).and_then(|res| {
            assert_eq!(res.status(), StatusCode::Ok);
            Ok(())
        })).unwrap();
        // With 0 content length.
        assert_eq!(req_push(prefix, flow_id, token, b""), (StatusCode::Ok, None));

        let req = Request::new(
            Method::Post,
            format!("{}/flow/{}/push", prefix, flow_id).parse().unwrap(),
        );
        core.run(client.request(req).and_then(|res| check_error_response(res, "Missing Token")))
            .unwrap();

        assert_eq!(req_push(prefix, fake_id, token, payload1), (StatusCode::NotFound, None));
        assert_eq!(req_push(prefix, flow_id, mal_token, payload1), (StatusCode::NotFound, None));
        assert_eq!(req_push(prefix, flow_id, fake_token, payload1), (StatusCode::NotFound, None));
        assert_eq!(req_push(prefix, flow_id, token, payload1), (StatusCode::Ok, None));
        assert_eq!(req_push(prefix, flow_id, token, payload2), (StatusCode::Ok, None));

        assert_eq!(req_fetch(prefix, fake_id, 0), (StatusCode::NotFound, None));
        assert_eq!(req_fetch(prefix, flow_id, 0), (StatusCode::Ok, Some(payload1.to_vec())));
        assert_eq!(req_fetch(prefix, flow_id, 1), (StatusCode::Ok, Some(payload2.to_vec())));

        let mut req = Request::new(
            Method::Put,
            format!("{}/flow/{}/push?token={}", prefix, flow_id, token).parse().unwrap(),
        );
        req.headers_mut().set(ContentLength(payload1.len() as u64));
        req.set_body(payload1.to_vec());
        core.run(client.request(req).and_then(|res| {
            assert_eq!(res.status(), StatusCode::Ok);
            Ok(())
        })).unwrap();
        assert_eq!(req_fetch(prefix, flow_id, 2), (StatusCode::Ok, Some(payload1.to_vec())));

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
                assert_eq!(req_push(prefix, flow_id, token, payload1), (StatusCode::Ok, None));
            })
        };

        thd.thread().unpark();
        assert_eq!(req_fetch(prefix, flow_id, 2), (StatusCode::Ok, Some(payload1.to_vec())));
        thd.join().unwrap();
    }

    #[test]
    fn handle_push_pull() {
        let prefix = &spawn_server().0;
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());
        let payload = vec![1u8; flow::REF_SIZE * 10];
        let (ref flow_id, ref token) = create_flow(prefix, r#"{}"#);
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
                    assert_eq!(req_push(prefix, flow_id, token, chunk), (StatusCode::Ok, None));
                }
                assert_eq!(req_close(prefix, flow_id, token), (StatusCode::Ok, None));
            })
        };

        assert_eq!(req_pull(prefix, fake_id), (StatusCode::NotFound, None));
        assert_eq!(req_pull(prefix, flow_id), (StatusCode::Ok, Some(payload)));
        thd.join().unwrap();

        let (ref flow_id, ref token) = create_flow(prefix, r#"{}"#);
        assert_eq!(req_push(prefix, flow_id, token, b"Hello"), (StatusCode::Ok, None));
        assert_eq!(req_close(prefix, flow_id, token), (StatusCode::Ok, None));

        let filename = "Sc r\r\nipト.рус";
        let qs = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("filename", filename)
            .finish();
        let req = Request::new(
            Method::Get,
            format!("{}/flow/{}/pull?{}", prefix, flow_id, qs).parse().unwrap(),
        );
        core.run(client.request(req).and_then(|res| {
            assert_eq!(res.status(), StatusCode::Ok);
            let res_disp = res.headers().get::<ContentDisposition>().unwrap();
            let check_disp = ContentDisposition {
                disposition: DispositionType::Attachment,
                parameters: vec![
                    DispositionParam::Filename(
                        Charset::Ext("UTF-8".into()),
                        Some(langtag!(en)),
                        filename.as_bytes().to_vec()
                    ),
                ],
            };
            assert_eq!(res_disp, &check_disp);
            Ok(())
        })).unwrap();

        let (ref flow_id, ref token) = create_flow(prefix, r#"{}"#);
        assert_eq!(req_close(prefix, flow_id, token), (StatusCode::Ok, None));
        assert_eq!(req_pull(prefix, flow_id), (StatusCode::NotFound, None));
    }

    #[test]
    fn handle_eof() {
        let prefix = &spawn_server().0;
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());
        let (ref flow_id, ref token) = create_flow(prefix, r#"{}"#);
        let fake_id = "bdc62e9323003d0f5cb44c8c745a0470";
        let mal_token = "sjlc(84c84w47wq87a";
        let fake_token = "bdc62e9323003d0f5cb44c8c745a0470bdc62e9323003d0f5cb44c8c745a0470";

        let req =
            Request::new(Method::Post, format!("{}/flow/{}/eof", prefix, flow_id).parse().unwrap());
        core.run(client.request(req).and_then(|res| check_error_response(res, "Missing Token")))
            .unwrap();

        assert_eq!(req_close(prefix, fake_id, token), (StatusCode::NotFound, None));
        assert_eq!(req_close(prefix, flow_id, mal_token), (StatusCode::NotFound, None));
        assert_eq!(req_close(prefix, flow_id, fake_token), (StatusCode::NotFound, None));
        assert_eq!(req_close(prefix, flow_id, token), (StatusCode::Ok, None));
        assert_eq!(req_push(prefix, flow_id, token, b"Hello"), (
            StatusCode::BadRequest,
            Some("Not Ready".to_string()),
        ));
        assert_eq!(
            req_close(prefix, flow_id, token),
            (StatusCode::BadRequest, Some("Closed".to_string()))
        );
        assert_eq!(req_fetch(prefix, flow_id, 0), (StatusCode::NotFound, None));
        assert_eq!(req_push(prefix, flow_id, token, b"Hello"), (StatusCode::NotFound, None));
    }

    #[test]
    fn recycle_and_release() {
        let prefix = &spawn_server().0;
        let (ref flow_id, ref token) = create_flow(prefix, r#"{}"#);

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
        assert_eq!(req_close(prefix, flow_id, token), (StatusCode::NotFound, None));

        thd.join().unwrap();
    }

    #[test]
    fn dropped() {
        let prefix = &spawn_server().0;
        let (ref flow_id, ref token) = create_flow(prefix, r#"{}"#);
        let payload1: &[u8] = b"The quick brown fox jumps\nover the lazy dog";
        let payload2: &[u8] = b"The guick yellow fox jumps\nover the fast cat";

        assert_eq!(req_push(prefix, flow_id, token, payload1), (StatusCode::Ok, None));
        assert_eq!(req_push(prefix, flow_id, token, payload2), (StatusCode::Ok, None));
        assert_eq!(req_close(prefix, flow_id, token), (StatusCode::Ok, None));
        assert_eq!(req_fetch(prefix, flow_id, 0), (StatusCode::Ok, Some(payload1.to_vec())));
        assert_eq!(req_fetch(prefix, flow_id, 0), (StatusCode::NotFound, None));
        assert_eq!(req_pull(prefix, flow_id), (StatusCode::Ok, Some(payload2.to_vec())));
    }

    #[test]
    fn full_push() {
        let prefix = &spawn_server().0;
        let (ref flow_id, ref token) = create_flow(prefix, r#"{}"#);

        assert_eq!(req_push(prefix, flow_id, token, b"Hello"), (StatusCode::Ok, None));

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
        rx.recv().unwrap();

        assert_eq!(req_push(prefix, flow_id, token, b"Hello"), (
            StatusCode::BadRequest,
            Some("Not Ready".to_string()),
        ));

        req_pull(prefix, flow_id);
        rx.recv().unwrap();
    }

    #[test]
    fn racing_pull() {
        let prefix = &spawn_server().0;
        let (ref flow_id, ref token) = create_flow(prefix, r#"{}"#);

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
                let client = Client::new(&handle);

                // Infinite flow.
                let body_stream = stream::unfold((), move |_| {
                    send_tx.send(()).unwrap();
                    Some(future::ok((Ok(hyper::Chunk::from(vec![0u8; flow::REF_SIZE])), ())))
                });

                let (tx, body) = hyper::Body::pair();
                let url =
                    format!("{}/flow/{}/push?token={}", prefix, flow_id, token).parse().unwrap();
                let mut req = Request::new(Method::Post, url);
                req.set_body(body);

                // Schedule the sender to the reactor.
                handle.spawn(tx.send_all(body_stream).then(|_| Err(())));
                core.run(client.request(req).and_then(|_| Ok(()))).unwrap();
            });
        }

        let thd = {
            let prefix = prefix.clone();
            let flow_id = flow_id.clone();
            thread::spawn(move || {
                let mut core = Core::new().unwrap();
                let client = Client::new(&core.handle());
                let prefix = &prefix;
                let flow_id = &flow_id;

                let req = Request::new(
                    Method::Get,
                    format!("{}/flow/{}/pull", prefix, flow_id).parse().unwrap(),
                );

                let mut park_once = true;
                core.run(client.request(req).and_then(|res| {
                    assert_eq!(res.status(), StatusCode::Ok);
                    res.body()
                        .for_each(move |_| {
                            if park_once {
                                park_once = false;
                                thread::park();
                            }
                            Ok(())
                        })
                        .boxed()
                })).unwrap();
            })
        };

        while !send_rx.recv_timeout(Duration::from_millis(5000)).is_err() {}

        let status = req_status(prefix, flow_id).1.unwrap();

        for idx in status.tail..status.next {
            assert_eq!(req_fetch(prefix, flow_id, idx).0, StatusCode::Ok);
        }

        thd.thread().unpark();
        thd.join().unwrap();
    }

    #[test]
    fn overload() {
        let prefix = &spawn_server().0;
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let (ref flow_id, ref token) = create_flow(prefix, r#"{}"#);
        for _ in 1..32 {
            create_flow(prefix, r#"{}"#);
        }

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{}"#);
        req.headers_mut().set(ContentLength(2));
        core.run(client.request(req).and_then(|res| {
            assert_eq!(res.status(), StatusCode::ServiceUnavailable);
            Ok(())
        })).unwrap();

        thread::sleep(Duration::from_secs(4));
        assert_eq!(req_push(prefix, flow_id, token, b"Hello"), (StatusCode::Ok, None));
        thread::sleep(Duration::from_secs(4));
        for _ in 0..31 {
            create_flow(prefix, r#"{}"#);
        }

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{}"#);
        req.headers_mut().set(ContentLength(2));
        core.run(client.request(req).and_then(|res| {
            assert_eq!(res.status(), StatusCode::ServiceUnavailable);
            Ok(())
        })).unwrap();
    }

    #[test]
    fn handle_status() {
        let prefix = &spawn_server().0;
        let (ref flow_id, ref token) = create_flow(prefix, r#"{}"#);
        let fake_id = "bdc62e9323003d0f5cb44c8c745a0470";
        assert_eq!(req_status(prefix, fake_id), (StatusCode::NotFound, None));
        assert_eq!(req_push(prefix, flow_id, token, b"Hello"), (StatusCode::Ok, None));
        assert_eq!(req_push(prefix, flow_id, token, b"Hello"), (StatusCode::Ok, None));
        assert_eq!(
            req_status(prefix, flow_id),
            (StatusCode::Ok, Some(StatusResponse { tail: 0, next: 2 }))
        );
    }

    #[test]
    fn fixed_length() {
        let prefix = &spawn_server().0;
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let param = serde_json::to_vec(&NewRequest { size: Some(5) }).unwrap();
        let (ref flow_id, ref token) = create_flow(prefix, &String::from_utf8(param).unwrap());

        assert_eq!(req_push(prefix, flow_id, token, b"Hel"), (StatusCode::Ok, None));
        assert_eq!(req_push(prefix, flow_id, token, b"World"), (
            StatusCode::BadRequest,
            Some("Not Ready".to_string()),
        ));
        assert_eq!(req_push(prefix, flow_id, token, b"lo"), (StatusCode::Ok, None));
        assert_eq!(req_push(prefix, flow_id, token, b"World"), (
            StatusCode::BadRequest,
            Some("Not Ready".to_string()),
        ));
        assert_eq!(
            req_close(prefix, flow_id, token),
            (StatusCode::BadRequest, Some("Closed".to_string()))
        );

        let req =
            Request::new(Method::Get, format!("{}/flow/{}/pull", prefix, flow_id).parse().unwrap());
        core.run(client.request(req).and_then(|res| {
            assert_eq!(res.status(), StatusCode::Ok);
            assert_eq!(res.headers().get::<ContentLength>().unwrap().0, 5);
            res.body().concat2().and_then(|body| {
                assert_eq!(body.to_vec(), b"Hello".to_vec());
                Ok(())
            })
        })).unwrap();

        let param = serde_json::to_vec(&NewRequest { size: Some(0) }).unwrap();
        let (ref flow_id, ref token) = create_flow(prefix, &String::from_utf8(param).unwrap());

        assert_eq!(req_push(prefix, flow_id, token, b"A"), (
            StatusCode::BadRequest,
            Some("Not Ready".to_string()),
        ));
        assert_eq!(req_close(prefix, flow_id, token), (StatusCode::Ok, None));
    }

    #[test]
    fn dropped_fixed_length() {
        let prefix = &spawn_server().0;
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());
        let param = serde_json::to_vec(&NewRequest { size: Some(5) }).unwrap();
        let (ref flow_id, ref token) = create_flow(prefix, &String::from_utf8(param).unwrap());

        assert_eq!(req_push(prefix, flow_id, token, b"Hel"), (StatusCode::Ok, None));
        assert_eq!(req_push(prefix, flow_id, token, b"lo"), (StatusCode::Ok, None));
        assert_eq!(req_fetch(prefix, flow_id, 0), (StatusCode::Ok, Some(b"Hel".to_vec())));

        let req =
            Request::new(Method::Get, format!("{}/flow/{}/pull", prefix, flow_id).parse().unwrap());
        core.run(client.request(req).and_then(|res| {
            assert_eq!(res.status(), StatusCode::Ok);
            assert!(res.headers().get::<ContentLength>().is_none());
            res.body().concat2().and_then(|body| {
                assert_eq!(body.to_vec(), b"lo".to_vec());
                Ok(())
            })
        })).unwrap();
    }

    #[test]
    fn tls_service() {
        let tls_config = config_tls("./tests/cert.pem", "./tests/private.pem");
        let port = start_service(
            "127.0.0.1:0".parse().unwrap(),
            1,
            Some(32),
            Some(Duration::from_secs(6)),
            MAX_CAPACITY,
            MAX_CAPACITY,
            Some(tls_config),
            false,
        ).unwrap()
            .port();

        let prefix = format!("https://127.0.0.1:{}", port);
        let mut core = Core::new().unwrap();

        let rootca = {
            let cert_file = File::open("./tests/cert.der").unwrap();
            let mut buf = Vec::new();
            BufReader::new(cert_file).read_to_end(&mut buf).unwrap();
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
        let client = Client::configure().connector(connector).build(&core.handle());

        let mut req = Request::new(Method::Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{}"#);
        req.headers_mut().set(ContentLength(2));
        core.run(client.request(req).and_then(|res| {
            assert_eq!(res.status(), StatusCode::Ok);
            res.body().concat2().and_then(|body| {
                let data = serde_json::from_slice::<NewResponse>(&body).unwrap();
                assert!(Regex::new("^[a-f0-9]{32}$").unwrap().find(&data.id).is_some());
                assert!(Regex::new("^[a-f0-9]{64}$").unwrap().find(&data.token).is_some());
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
        type Error = io::Error;
        type Future = Box<Future<Item = Self::Response, Error = io::Error>>;

        fn call(&self, uri: Uri) -> Self::Future {
            let tls_connector = self.tls.clone();
            Box::new(self.http.call(uri).and_then(move |io| {
                tls_connector.connect_async("example.com", io).map_err(|err| {
                    io::Error::new(io::ErrorKind::Other, err)
                })
            }))
        }
    }

    #[test]
    fn multi_workers() {
        let res = thread::spawn(|| {
            start_service(
                "127.0.0.1:0".parse().unwrap(),
                4,
                None,
                None,
                MAX_CAPACITY,
                MAX_CAPACITY,
                None,
                false,
            ).unwrap();
        }).join()
            .is_ok();
        if cfg!(unix) {
            assert_eq!(res, true);
        } else {
            assert_eq!(res, false);
        }
    }
}

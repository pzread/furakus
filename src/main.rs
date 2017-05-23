#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
extern crate bytes;
extern crate dotenv;
extern crate futures;
extern crate hyper;
extern crate regex;
extern crate ring;
extern crate serde;
extern crate serde_json;
extern crate tokio_core as tokio;
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
use hyper::header::{ContentLength, ContentType, Host};
use hyper::server::{Http, Request, Response, Service};
use pool::Pool;
use regex::Regex;
use std::{env, mem, thread};
use std::sync::{Arc, Barrier, RwLock};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::reactor::{self, Core};

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
    fn new(pool: Arc<RwLock<Pool>>,
           remote: reactor::Remote,
           meta_capacity: u64,
           data_capacity: u64,
           authorizer: Arc<Authorizer>)
           -> Self {
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
        match req.headers().get() {
            Some(&ContentLength(0)) |
            None => return future::ok(Self::response_error("Invalid Parameter")).boxed(),
            _ => (),
        };
        let pool_ptr = self.pool.clone();
        let meta_capacity = self.meta_capacity;
        let data_capacity = self.data_capacity;
        let authorizer = self.authorizer.clone();
        req.body()
            .concat()
            .map_err(|err| Error::Internal(err))
            .and_then(|body| {
                serde_json::from_slice::<NewRequest>(&body).map_err(|_| Error::Invalid)
            })
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
                future::ok(Response::new()
                               .with_header(ContentType::json())
                               .with_header(ContentLength(body.len() as u64))
                               .with_body(body))
            })
            .or_else(|err| match err {
                         Error::Invalid => Ok(Self::response_error("Invalid Parameter")),
                         Error::NotReady => {
                             Ok(Response::new().with_status(StatusCode::ServiceUnavailable))
                         }
                         Error::Internal(err) => Err(err),
                     })
            .boxed()
    }

    fn handle_push(&self,
                   req: Request,
                   route: regex::Captures,
                   mut query: url::form_urlencoded::Parse)
                   -> ResponseFuture {
        let token = match query.find(|&(ref key, _)| key == "token") {
            Some((_, token)) => token.to_owned(),
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
                flow.push(chunk.into_bytes())
                    .map(|_| ())
                    .map_err(|_| hyper::error::Error::Incomplete)
            })
            .and_then(|_| Ok(Self::response_ok()))
            .or_else(|_| Ok(Self::response_error("Not Ready")))
            .boxed()
    }

    fn handle_eof(&self,
                  _req: Request,
                  route: regex::Captures,
                  mut query: url::form_urlencoded::Parse)
                  -> ResponseFuture {
        let token = match query.find(|&(ref key, _)| key == "token") {
            Some((_, token)) => token.to_owned(),
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
                          Err(flow::Error::NotReady) => Ok(Self::response_error("Not Ready")),
                          Err(err) => Err(err),
                      })
                .or_else(|_| Ok(Response::new().with_status(StatusCode::InternalServerError)))
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
            }
            .into_bytes();
        future::ok(Response::new()
                       .with_header(ContentType::json())
                       .with_header(ContentLength(body.len() as u64))
                       .with_body(body))
                .boxed()
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
                    future::ok(Response::new()
                                   .with_header(ContentType::octet_stream())
                                   .with_header(ContentLength(chunk.len() as u64))
                                   .with_body(chunk))
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

    fn handle_pull(&self, _req: Request, route: regex::Captures) -> ResponseFuture {
        let flow_id = route.get(1).unwrap().as_str();
        let flow_ptr = match self.pool.read().unwrap().get(flow_id) {
            Some(flow) => flow.clone(),
            None => return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed(),
        };
        let body_stream = stream::unfold(Some(0), move |chunk_index| {
            // Check if the flow is EOF.
            if let Some(chunk_index) = chunk_index {
                let flow = flow_ptr.read().unwrap();
                // Check if we need to get the first chunk index.
                let chunk_index = if chunk_index == 0 {
                    flow.get_range().0
                } else {
                    chunk_index
                };
                let fut = flow.pull(chunk_index, None)
                    .and_then(move |chunk| {
                        let hyper_chunk = Ok(hyper::Chunk::from(chunk));
                        future::ok((hyper_chunk, Some(chunk_index + 1)))
                    })
                    .or_else(|_| future::ok((Ok(hyper::Chunk::from(vec![])), None)));
                Some(fut)
            } else {
                None
            }
        });
        let (tx, body) = hyper::Body::pair();
        // Schedule the sender to the reactor.
        self.remote
            .spawn(move |_| {
                tx.send_all(body_stream).and_then(|(mut tx, _)| tx.close()).then(|_| Ok(()))
            });
        future::ok(Response::new().with_header(ContentType::octet_stream()).with_body(body)).boxed()
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
            static ref PATTERN_PUSH: Regex = Regex::new(r"^/([a-f0-9]{32})/push$").unwrap();
            static ref PATTERN_EOF: Regex = Regex::new(r"^/([a-f0-9]{32})/eof$").unwrap();
            static ref PATTERN_STATUS: Regex = Regex::new(r"^/([a-f0-9]{32})/status$").unwrap();
            static ref PATTERN_FETCH: Regex = Regex::new(r"^/([a-f0-9]{32})/fetch/(\d+)$").unwrap();
            static ref PATTERN_PULL: Regex = Regex::new(r"^/([a-f0-9]{32})/pull$").unwrap();
        }

        let url = {
            let uri = req.uri().to_owned();
            let resolved = if uri.is_absolute() {
                url::Url::parse(uri.as_ref())
            } else {
                if let Some(host) = req.headers().get::<Host>() {
                    let hostname = host.hostname();
                    let base = match host.port() {
                        Some(port) => format!("http://{}:{}", hostname, port),
                        None => format!("http://{}", hostname),
                    };
                    url::Url::parse(&base).and_then(|url| url.join(uri.as_ref()))
                } else {
                    Err(url::ParseError::RelativeUrlWithoutBase)
                }
            };
            match resolved {
                Ok(url) => url,
                Err(_) => {
                    return future::ok(Response::new().with_status(StatusCode::BadRequest)).boxed()
                }
            }
        };

        let path = url.path();
        match req.method() {
            &Method::Post => {
                if let Some(route) = PATTERN_NEW.captures(path) {
                    self.handle_new(req, route)
                } else if let Some(route) = PATTERN_PUSH.captures(path) {
                    self.handle_push(req, route, url.query_pairs())
                } else if let Some(route) = PATTERN_EOF.captures(path) {
                    self.handle_eof(req, route, url.query_pairs())
                } else if let Some(route) = PATTERN_STATUS.captures(path) {
                    self.handle_status(req, route)
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
            _ => future::ok(Response::new().with_status(StatusCode::MethodNotAllowed)).boxed(),
        }
    }
}

fn start_service(addr: std::net::SocketAddr,
                 num_worker: usize,
                 pool_size: Option<usize>,
                 deactive_timeout: Option<Duration>,
                 meta_capacity: u64,
                 data_capacity: u64,
                 blocking: bool)
                 -> Option<std::net::SocketAddr> {
    let upstream_listener = std::net::TcpListener::bind(&addr).unwrap();
    let pool_ptr = Pool::new(pool_size, deactive_timeout);
    let auth_ptr = Arc::new(HMACAuthorizer::new());
    let mut workers = Vec::with_capacity(num_worker);
    let barrier = Arc::new(Barrier::new(num_worker.checked_add(1).unwrap()));

    for idx in 0..num_worker {
        let addr = addr.clone();
        let listener = upstream_listener.try_clone().unwrap();
        let barrier = barrier.clone();
        let pool_ptr = pool_ptr.clone();
        let auth_ptr = auth_ptr.clone();
        let worker = thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let handle = core.handle();
            let remote = core.remote();
            let listener = TcpListener::from_listener(listener, &addr, &handle).unwrap();
            let http = Http::new();
            let acceptor = listener
                .incoming()
                .for_each(|(io, addr)| {
                    let service = FlowService::new(pool_ptr.clone(),
                                                   remote.clone(),
                                                   meta_capacity,
                                                   data_capacity,
                                                   auth_ptr.clone());
                    http.bind_connection(&handle, io, addr, service);
                    Ok(())
                });
            println!("Worker #{} is started.", idx);
            barrier.wait();
            core.run(acceptor).unwrap();
        });
        workers.push(worker);
    }

    barrier.wait();

    if blocking {
        for worker in workers {
            worker.join().unwrap();
        }
        None
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
    start_service(addr,
                  num_worker,
                  Some(pool_size),
                  Some(Duration::from_secs(deactive_timeout)),
                  meta_capacity,
                  data_capacity,
                  true);
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow;
    use futures::{Future, Sink, Stream, future, stream};
    use hyper;
    use hyper::Method::{Get, Post, Put};
    use hyper::client::{Client, Request};
    use hyper::header::ContentLength;
    use hyper::status::StatusCode;
    use regex::Regex;
    use serde_json;
    use std::{str, thread};
    use std::io::prelude::*;
    use std::net::TcpStream;
    use std::sync::mpsc;
    use std::time::Duration;
    use tokio::reactor::Core;

    const MAX_CAPACITY: u64 = 1048576;

    fn spawn_server() -> (String, String) {
        let port = start_service("127.0.0.1:0".parse().unwrap(),
                                 1,
                                 Some(32),
                                 Some(Duration::from_secs(6)),
                                 MAX_CAPACITY,
                                 MAX_CAPACITY,
                                 false)
                .unwrap()
                .port();
        (format!("http://127.0.0.1:{}", port), format!("127.0.0.1:{}", port))
    }

    fn create_flow(prefix: &str, param: &str) -> (String, String) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let mut req = Request::new(Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(param.to_owned());
        req.headers_mut().set(ContentLength(param.len() as u64));

        let mut id = String::new();
        let mut token = String::new();
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                res.body()
                    .concat()
                    .and_then(|body| {
                        let data = serde_json::from_slice::<NewResponse>(&body).unwrap();
                        id = data.id;
                        token = data.token;
                        Ok(())
                    })
            }))
            .unwrap();
        (id, token)
    }

    fn req_push(prefix: &str,
                flow_id: &str,
                token: &str,
                payload: &[u8])
                -> (StatusCode, Option<String>) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let mut req =
            Request::new(Post,
                         format!("{}/{}/push?token={}", prefix, flow_id, token).parse().unwrap());
        req.set_body(payload.to_vec());

        let mut status_code = StatusCode::ImATeapot;
        let mut response = None;
        core.run(client
                     .request(req)
                     .and_then(|res| {
                status_code = res.status();
                let fut = if status_code == StatusCode::BadRequest {
                    res.body()
                        .concat()
                        .and_then(|body| {
                            let data = serde_json::from_slice::<ErrorResponse>(&body).unwrap();
                            Ok(Some(data.message))
                        })
                        .boxed()
                } else {
                    future::ok(None).boxed()
                };
                fut.and_then(|body| {
                    response = body;
                    Ok(())
                })
            }))
            .unwrap();

        (status_code, response)
    }

    fn req_close(prefix: &str, flow_id: &str, token: &str) -> (StatusCode, Option<String>) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let req =
            Request::new(Post,
                         format!("{}/{}/eof?token={}", prefix, flow_id, token).parse().unwrap());

        let mut status_code = StatusCode::ImATeapot;
        let mut response = None;
        core.run(client
                     .request(req)
                     .and_then(|res| {
                status_code = res.status();
                let fut = if status_code == StatusCode::BadRequest {
                    res.body()
                        .concat()
                        .and_then(|body| {
                            let data = serde_json::from_slice::<ErrorResponse>(&body).unwrap();
                            Ok(Some(data.message))
                        })
                        .boxed()
                } else {
                    future::ok(None).boxed()
                };
                fut.and_then(|body| {
                    response = body;
                    Ok(())
                })
            }))
            .unwrap();

        (status_code, response)
    }

    fn req_status(prefix: &str, flow_id: &str) -> (StatusCode, Option<StatusResponse>) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let req = Request::new(Post, format!("{}/{}/status", prefix, flow_id).parse().unwrap());

        let mut status_code = StatusCode::ImATeapot;
        let mut response = None;
        core.run(client
                     .request(req)
                     .and_then(|res| {
                status_code = res.status();
                let fut = if status_code == StatusCode::Ok {
                    res.body().concat().and_then(|body| Ok(Some(body.to_vec()))).boxed()
                } else {
                    future::ok(None).boxed()
                };
                fut.and_then(|body| {
                    response =
                        body.map(|data| serde_json::from_slice::<StatusResponse>(&data).unwrap());
                    Ok(())
                })
            }))
            .unwrap();

        (status_code, response)
    }

    fn req_fetch(prefix: &str, flow_id: &str, index: u64) -> (StatusCode, Option<Vec<u8>>) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let req = Request::new(Get,
                               format!("{}/{}/fetch/{}", prefix, flow_id, index).parse().unwrap());

        let mut status_code = StatusCode::ImATeapot;
        let mut response = None;
        core.run(client
                     .request(req)
                     .and_then(|res| {
                status_code = res.status();
                let fut = if status_code == StatusCode::Ok {
                    res.body().concat().and_then(|body| Ok(Some(body.to_vec()))).boxed()
                } else {
                    future::ok(None).boxed()
                };
                fut.and_then(|body| {
                    response = body;
                    Ok(())
                })
            }))
            .unwrap();

        (status_code, response)
    }

    fn req_pull(prefix: &str, flow_id: &str) -> (StatusCode, Option<Vec<u8>>) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let req = Request::new(Get, format!("{}/{}/pull", prefix, flow_id).parse().unwrap());

        let mut status_code = StatusCode::ImATeapot;
        let mut response = None;
        core.run(client
                     .request(req)
                     .and_then(|res| {
                status_code = res.status();
                let fut = if status_code == StatusCode::Ok {
                    res.body().concat().and_then(|body| Ok(Some(body.to_vec()))).boxed()
                } else {
                    future::ok(None).boxed()
                };
                fut.and_then(|body| {
                    response = body;
                    Ok(())
                })
            }))
            .unwrap();

        (status_code, response)
    }

    fn check_error_response(res: Response, error: &str) -> future::BoxFuture<(), hyper::Error> {
        assert_eq!(res.status(), StatusCode::BadRequest);
        let error = error.to_owned();
        res.body()
            .concat()
            .and_then(move |body| {
                assert_eq!(serde_json::from_slice::<ErrorResponse>(&body).unwrap(),
                           ErrorResponse { message: error });
                Ok(())
            })
            .boxed()
    }

    #[test]
    fn validate_route() {
        let (ref prefix, ref ip_prefix) = spawn_server();
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());
        let (ref flow_id, ref token) = create_flow(prefix, r#"{}"#);

        let req = Request::new(Post,
                               format!("{}/{}/x/../push?token={}", prefix, flow_id, token)
                                   .parse()
                                   .unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                Ok(())
            }))
            .unwrap();

        let mut stream = TcpStream::connect(ip_prefix).unwrap();
        stream.write(b"GET /neo HTTP/1.0\r\n\r\n").unwrap();
        let mut data = [0u8; 12];
        stream.read_exact(&mut data).unwrap();
        assert_eq!(&str::from_utf8(&data).unwrap(), &"HTTP/1.1 400");

        let mut stream = TcpStream::connect(ip_prefix).unwrap();
        stream.write(b"GET /neo HTTP/1.0\r\nHost: http://x\r\n\r\n").unwrap();
        let mut data = [0u8; 12];
        stream.read_exact(&mut data).unwrap();
        assert_eq!(&str::from_utf8(&data).unwrap(), &"HTTP/1.1 404");

        let mut stream = TcpStream::connect(ip_prefix).unwrap();
        stream.write(&format!("GET {}/neo HTTP/1.0\r\n\r\n", prefix).into_bytes()).unwrap();
        let mut data = [0u8; 12];
        stream.read_exact(&mut data).unwrap();
        assert_eq!(&str::from_utf8(&data).unwrap(), &"HTTP/1.1 404");

        let req = Request::new(Post, format!("{}/neo", prefix).parse().unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::NotFound);
                Ok(())
            }))
            .unwrap();

        let req = Request::new(Post, format!("{}/x/{}/push", prefix, flow_id).parse().unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::NotFound);
                Ok(())
            }))
            .unwrap();

        let req = Request::new(Post, format!("{}/{}/pusha", prefix, flow_id).parse().unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::NotFound);
                Ok(())
            }))
            .unwrap();

        let req = Request::new(Post, format!("{}/{}/eofa", prefix, flow_id).parse().unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::NotFound);
                Ok(())
            }))
            .unwrap();

        let req = Request::new(Get, format!("{}/neo", prefix).parse().unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::NotFound);
                Ok(())
            }))
            .unwrap();

        let req = Request::new(Get, format!("{}/{}/pullb", prefix, flow_id).parse().unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::NotFound);
                Ok(())
            }))
            .unwrap();

        let req = Request::new(Get, format!("{}/{}/fetchb", prefix, flow_id).parse().unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::NotFound);
                Ok(())
            }))
            .unwrap();

        let req = Request::new(Put, format!("{}/neo", prefix).parse().unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::MethodNotAllowed);
                Ok(())
            }))
            .unwrap();
    }

    #[test]
    fn handle_new() {
        let prefix = &spawn_server().0;
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let mut req = Request::new(Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{}"#);
        req.headers_mut().set(ContentLength(2));
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                res.body()
                    .concat()
                    .and_then(|body| {
                        let data = serde_json::from_slice::<NewResponse>(&body).unwrap();
                        assert!(Regex::new("^[a-f0-9]{32}$").unwrap().find(&data.id).is_some());
                        assert!(Regex::new("^[a-f0-9]{64}$").unwrap().find(&data.token).is_some());
                        Ok(())
                    })
            }))
            .unwrap();

        let mut req = Request::new(Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{"size": 4096}"#);
        req.headers_mut().set(ContentLength(14));
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                res.body()
                    .concat()
                    .and_then(|body| {
                        let data = serde_json::from_slice::<NewResponse>(&body).unwrap();
                        assert!(Regex::new("^[a-f0-9]{32}$").unwrap().find(&data.id).is_some());
                        assert!(Regex::new("^[a-f0-9]{64}$").unwrap().find(&data.token).is_some());
                        Ok(())
                    })
            }))
            .unwrap();

        let mut req = Request::new(Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{}"#);
        core.run(client.request(req).and_then(|res| {
                check_error_response(res, "Invalid Parameter")
            }))
            .unwrap();

        let mut req = Request::new(Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{"size": 4O96}"#);
        req.headers_mut().set(ContentLength(14));
        core.run(client.request(req).and_then(|res| {
                check_error_response(res, "Invalid Parameter")
            }))
            .unwrap();
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
        let req =
            Request::new(Post,
                         format!("{}/{}/push?token={}", prefix, flow_id, token).parse().unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                Ok(())
            }))
            .unwrap();
        // With 0 content length.
        assert_eq!(req_push(prefix, flow_id, token, b""), (StatusCode::Ok, None));

        let req = Request::new(Post, format!("{}/{}/push", prefix, flow_id).parse().unwrap());
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
    }

    #[test]
    fn handle_push_pull() {
        let prefix = &spawn_server().0;
        let payload = vec![1u8; flow::REF_SIZE * 10];
        let (ref flow_id, ref token) = create_flow(prefix,
                                                   &format!(r#"{{"size": {}}}"#, payload.len()));
        let fake_id = "bdc62e9323003d0f5cb44c8c745a0470";

        {
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
            });
        }

        assert_eq!(req_pull(prefix, fake_id), (StatusCode::NotFound, None));
        assert_eq!(req_pull(prefix, flow_id), (StatusCode::Ok, Some(payload)));
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

        let req = Request::new(Post, format!("{}/{}/eof", prefix, flow_id).parse().unwrap());
        core.run(client.request(req).and_then(|res| check_error_response(res, "Missing Token")))
            .unwrap();

        assert_eq!(req_close(prefix, fake_id, token), (StatusCode::NotFound, None));
        assert_eq!(req_close(prefix, flow_id, mal_token), (StatusCode::NotFound, None));
        assert_eq!(req_close(prefix, flow_id, fake_token), (StatusCode::NotFound, None));
        assert_eq!(req_close(prefix, flow_id, token), (StatusCode::Ok, None));
        assert_eq!(req_push(prefix, flow_id, token, b"Hello"),
                   (StatusCode::BadRequest, Some("Not Ready".to_string())));
        assert_eq!(req_close(prefix, flow_id, token),
                   (StatusCode::BadRequest, Some("Closed".to_string())));
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
                assert_eq!(req_fetch(&prefix, &flow_id, 100),
                           (StatusCode::InternalServerError, None));
            })
        };
        rx.recv().unwrap();
        thread::sleep(Duration::from_millis(1000));

        assert_eq!(req_close(prefix, flow_id, token), (StatusCode::Ok, None));
        assert_eq!(req_close(prefix, flow_id, token),
                   (StatusCode::BadRequest, Some("Closed".to_string())));
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

        assert_eq!(req_push(prefix, flow_id, token, b"Hello"),
                   (StatusCode::BadRequest, Some("Not Ready".to_string())));
        assert_eq!(req_close(prefix, flow_id, token),
                   (StatusCode::BadRequest, Some("Not Ready".to_string())));

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

                let mut req = Request::new(Post,
                                           format!("{}/{}/push?token={}", prefix, flow_id, token)
                                               .parse()
                                               .unwrap());
                let (tx, body) = hyper::Body::pair();
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

                let req = Request::new(Get,
                                       format!("{}/{}/pull", prefix, flow_id).parse().unwrap());

                let mut park_once = true;
                core.run(client
                             .request(req)
                             .and_then(|res| {
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
                    }))
                    .unwrap();
            })
        };

        while !send_rx.recv_timeout(Duration::from_millis(5000)).is_err() {}

        for idx in 0.. {
            if req_fetch(prefix, flow_id, idx).0 == StatusCode::Ok {
                for succ_idx in (idx + 1)..(idx + MAX_CAPACITY / flow::REF_SIZE as u64) {
                    assert_eq!(req_fetch(prefix, flow_id, succ_idx).0, StatusCode::Ok);
                }
                break;
            }
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

        let mut req = Request::new(Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{}"#);
        req.headers_mut().set(ContentLength(2));
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::ServiceUnavailable);
                Ok(())
            }))
            .unwrap();

        thread::sleep(Duration::from_secs(4));
        assert_eq!(req_push(prefix, flow_id, token, b"Hello"), (StatusCode::Ok, None));
        thread::sleep(Duration::from_secs(4));
        for _ in 0..31 {
            create_flow(prefix, r#"{}"#);
        }

        let mut req = Request::new(Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{}"#);
        req.headers_mut().set(ContentLength(2));
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::ServiceUnavailable);
                Ok(())
            }))
            .unwrap();
    }

    #[test]
    fn handle_status() {
        let prefix = &spawn_server().0;
        let (ref flow_id, ref token) = create_flow(prefix, r#"{}"#);
        let fake_id = "bdc62e9323003d0f5cb44c8c745a0470";
        assert_eq!(req_status(prefix, fake_id), (StatusCode::NotFound, None));
        assert_eq!(req_push(prefix, flow_id, token, b"Hello"), (StatusCode::Ok, None));
        assert_eq!(req_push(prefix, flow_id, token, b"Hello"), (StatusCode::Ok, None));
        assert_eq!(req_status(prefix, flow_id),
                   (StatusCode::Ok, Some(StatusResponse { tail: 0, next: 2 })));
    }
}

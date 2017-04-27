#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate mime;
#[macro_use]
extern crate serde_derive;
extern crate dotenv;
extern crate futures;
extern crate hyper;
extern crate regex;
extern crate serde;
extern crate serde_json;
extern crate tokio_core as tokio;
extern crate uuid;
mod flow;

use dotenv::dotenv;
use flow::Flow;
use futures::{Future, Stream, future, stream};
use hyper::{Method, StatusCode};
use hyper::header::{ContentLength, ContentType};
use hyper::server::{Http, Request, Response, Service};
use regex::Regex;
use std::{env, thread};
use std::collections::HashMap;
use std::sync::{Arc, Barrier, RwLock};
use tokio::reactor::Core;

#[derive(Debug)]
pub enum Error {
    Invalid,
    Internal(hyper::Error),
}

type SharedFlow = Arc<RwLock<Flow>>;

#[derive(Clone)]
struct FluxService {
    flow_bucket: Arc<RwLock<HashMap<String, SharedFlow>>>,
}

#[derive(Serialize, Deserialize)]
struct FlowReqParam {
    pub size: Option<u64>,
}

type ResponseFuture = future::BoxFuture<Response, hyper::Error>;

impl FluxService {
    fn new() -> Self {
        FluxService { flow_bucket: Arc::new(RwLock::new(HashMap::new())) }
    }

    fn handle_new(&self, req: Request, _route: regex::Captures) -> ResponseFuture {
        let flow_bucket = self.flow_bucket.clone();
        req.body()
            .concat()
            .map_err(|err| Error::Internal(err))
            .and_then(|body| {
                serde_json::from_slice::<FlowReqParam>(&body).map_err(|_| Error::Invalid)
            })
            .and_then(move |param| {
                let flow = Flow::new(param.size);
                let flow_id = flow.id.clone();
                {
                    let mut bucket = flow_bucket.write().unwrap();
                    bucket.insert(flow_id.clone(), Arc::new(RwLock::new(flow)));
                }
                let body = flow_id.into_bytes();
                future::ok(Response::new()
                               .with_header(ContentType::plaintext())
                               .with_header(ContentLength(body.len() as u64))
                               .with_body(body))
            })
            .or_else(|err| match err {
                         Error::Invalid => Ok(Response::new().with_status(StatusCode::BadRequest)),
                         Error::Internal(err) => Err(err),
                     })
            .boxed()
    }

    fn handle_push(&self, req: Request, route: regex::Captures) -> ResponseFuture {
        let flow_id = route.get(1).unwrap().as_str();
        let flow = {
            let bucket = self.flow_bucket.read().unwrap();
            if let Some(flow) = bucket.get(flow_id) {
                flow.clone()
            } else {
                return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed();
            }
        };

        // Chain a EOF on the stream to flush the remaining chunk.
        let body_stream = req.body().map(|chunk| Some(chunk)).chain(stream::once(Ok(None)));
        // Read the body and push chunks.
        let init_chunk = Vec::<u8>::with_capacity(flow::MAX_SIZE);
        body_stream
            .fold(init_chunk, move |mut rem_chunk, chunk| {
                let (flush_chunk, ret_chunk) = if let Some(chunk) = chunk {
                    if rem_chunk.len() + chunk.len() >= flow::MAX_SIZE {
                        let caplen = flow::MAX_SIZE - rem_chunk.len();
                        rem_chunk.extend_from_slice(&chunk[..caplen]);
                        (Some(rem_chunk), chunk[caplen..].to_vec())
                    } else {
                        rem_chunk.extend_from_slice(&chunk);
                        (None, rem_chunk)
                    }
                } else {
                    // EOF, flush the remaining chunk.
                    if rem_chunk.len() > 0 {
                        (Some(rem_chunk), Vec::new())
                    } else {
                        (None, Vec::new())
                    }
                };
                if let Some(flush_chunk) = flush_chunk {
                    let mut flow = flow.write().unwrap();
                    flow.push(&flush_chunk).map(|_| ret_chunk).map_err(|_| {
                        hyper::error::Error::Incomplete
                    }).boxed()
                } else {
                    future::ok(ret_chunk).boxed()
                }
            })
            .and_then(|_| {
                let body = "Ok";
                Ok(Response::new()
                       .with_header(ContentType::plaintext())
                       .with_header(ContentLength(body.len() as u64))
                       .with_body(body))
            })
            .or_else(|_| Ok(Response::new().with_status(StatusCode::InternalServerError)))
            .boxed()
    }

    fn handle_fetch(&self, _req: Request, route: regex::Captures) -> ResponseFuture {
        let flow_id = route.get(1).unwrap().as_str();
        let chunk_index: u64 = if let Ok(index) = route.get(2).unwrap().as_str().parse() {
            index
        } else {
            return future::ok(Response::new().with_status(StatusCode::BadRequest)).boxed();
        };
        let flow = {
            let bucket = self.flow_bucket.read().unwrap();
            if let Some(flow) = bucket.get(flow_id) {
                flow.clone()
            } else {
                return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed();
            }
        };
        let chunk = {
            let flow = flow.read().unwrap();
            if let Ok(chunk) = flow.fetch(chunk_index) {
                chunk.to_vec()
            } else {
                return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed();
            }
        };
        future::ok(Response::new()
                       .with_header(ContentType(mime!(Application / OctetStream)))
                       .with_header(ContentLength(chunk.len() as u64))
                       .with_body(chunk))
                .boxed()
    }

    fn handle_pull(&self, _req: Request, _route: regex::Captures) -> ResponseFuture {
        unreachable!();
    }
}

impl Service for FluxService {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = ResponseFuture;

    fn call(&self, req: Request) -> Self::Future {
        lazy_static! {
            static ref PATTERN_NEW: Regex = Regex::new(r"/new").unwrap();
            static ref PATTERN_PUSH: Regex = Regex::new(r"/([a-f0-9]{32})/push").unwrap();
            static ref PATTERN_FETCH: Regex = Regex::new(r"/([a-f0-9]{32})/fetch/(\d+)").unwrap();
            static ref PATTERN_PULL: Regex = Regex::new(r"/([a-f0-9]{32})/pull").unwrap();
        }

        let path = &req.path().to_owned();
        match req.method() {
            &Method::Post => {
                if let Some(route) = PATTERN_NEW.captures(path) {
                    self.handle_new(req, route)
                } else if let Some(route) = PATTERN_PUSH.captures(path) {
                    self.handle_push(req, route)
                } else if let Some(route) = PATTERN_PULL.captures(path) {
                    self.handle_pull(req, route)
                } else {
                    future::ok(Response::new().with_status(StatusCode::NotFound)).boxed()
                }
            }
            &Method::Get => {
                if let Some(route) = PATTERN_FETCH.captures(path) {
                    self.handle_fetch(req, route)
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
                 block: bool)
                 -> Option<std::net::SocketAddr> {
    let service = FluxService::new();

    let upstream_listener = std::net::TcpListener::bind(&addr).unwrap();
    let mut workers = Vec::with_capacity(num_worker);
    let barrier = Arc::new(Barrier::new(num_worker.checked_add(1).unwrap()));

    for idx in 0..num_worker {
        let moved_addr = addr.clone();
        let moved_listener = upstream_listener.try_clone().unwrap();
        let moved_service = service.clone();
        let moved_barrier = barrier.clone();
        let worker = thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let handle = core.handle();
            let listener =
                tokio::net::TcpListener::from_listener(moved_listener, &moved_addr, &handle)
                    .unwrap();
            let http = Http::new();
            let acceptor = listener
                .incoming()
                .for_each(|(io, addr)| {
                    http.bind_connection(&handle, io, addr, moved_service.clone());
                    Ok(())
                });
            println!("Worker #{} is started.", idx);
            moved_barrier.wait();
            core.run(acceptor).unwrap();
        });
        workers.push(worker);
    }

    barrier.wait();

    if block {
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
    start_service(addr, num_worker, true);
}

#[cfg(test)]
mod tests {
    use super::start_service;
    use futures::{Future, Stream};
    use hyper::Method::{Get, Post};
    use hyper::client::{Client, Request};
    use hyper::status::StatusCode;
    use regex::Regex;
    use std::str;
    use tokio::reactor::Core;

    fn spawn_server() -> String {
        let port = start_service("127.0.0.1:0".parse().unwrap(), 1, false).unwrap().port();
        format!("http://127.0.0.1:{}", port)
    }

    #[test]
    fn handle_new() {
        let prefix = &spawn_server();
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let mut req = Request::new(Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{}"#);
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                res.body()
                    .concat()
                    .and_then(|body| {
                        let flow_id = str::from_utf8(&body).unwrap();
                        assert!(Regex::new("[a-f0-9]{32}").unwrap().find(flow_id).is_some());
                        Ok(())
                    })
            }))
            .unwrap();

        let mut req = Request::new(Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{"size": 4096}"#);
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                res.body()
                    .concat()
                    .and_then(|body| {
                        let flow_id = str::from_utf8(&body).unwrap();
                        assert!(Regex::new("[a-f0-9]{32}").unwrap().find(flow_id).is_some());
                        Ok(())
                    })
            }))
            .unwrap();

        let mut req = Request::new(Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{"size": 4O96}"#);
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::BadRequest);
                Ok(())
            }))
            .unwrap();
    }

    #[test]
    fn handle_push_fetch() {
        let prefix = &spawn_server();
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let mut flow_id = String::new();
        let fake_id = "bdc62e9323003d0f5cb44c8c745a0470";
        let payload1: &[u8] = b"The quick brown fox jumps over the lazy dog";
        let payload2: &[u8] = b"The quick brown fox jumps over the lazy dog";

        let mut req = Request::new(Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body("{}");
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                res.body()
                    .concat()
                    .and_then(|body| {
                        flow_id = str::from_utf8(&body).unwrap().to_owned();
                        Ok(())
                    })
            }))
            .unwrap();

        // The empty chunk should be ignored.
        let req = Request::new(Post, format!("{}/{}/push", prefix, flow_id).parse().unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                res.body()
                    .concat()
                    .and_then(|body| {
                        assert_eq!(str::from_utf8(&body).unwrap(), "Ok");
                        Ok(())
                    })
            }))
            .unwrap();

        // There should be no chunk.
        let req = Request::new(Get, format!("{}/{}/fetch/0", prefix, flow_id).parse().unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::NotFound);
                Ok(())
            }))
            .unwrap();

        let mut req = Request::new(Post, format!("{}/{}/push", prefix, fake_id).parse().unwrap());
        req.set_body(payload1);
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::NotFound);
                Ok(())
            }))
            .unwrap();

        let mut req = Request::new(Post, format!("{}/{}/push", prefix, flow_id).parse().unwrap());
        req.set_body(payload1);
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                res.body()
                    .concat()
                    .and_then(|body| {
                        assert_eq!(str::from_utf8(&body).unwrap(), "Ok");
                        Ok(())
                    })
            }))
            .unwrap();

        let mut req = Request::new(Post, format!("{}/{}/push", prefix, flow_id).parse().unwrap());
        req.set_body(payload2);
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                res.body()
                    .concat()
                    .and_then(|body| {
                        assert_eq!(str::from_utf8(&body).unwrap(), "Ok");
                        Ok(())
                    })
            }))
            .unwrap();

        let req = Request::new(Get, format!("{}/{}/fetch/0", prefix, fake_id).parse().unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::NotFound);
                Ok(())
            }))
            .unwrap();

        let req = Request::new(Get, format!("{}/{}/fetch/10", prefix, flow_id).parse().unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::NotFound);
                Ok(())
            }))
            .unwrap();

        let req = Request::new(Get, format!("{}/{}/fetch/0", prefix, flow_id).parse().unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                res.body()
                    .concat()
                    .and_then(|body| {
                        assert_eq!(&body as &[u8], payload1);
                        Ok(())
                    })
            }))
            .unwrap();

        let req = Request::new(Get, format!("{}/{}/fetch/1", prefix, flow_id).parse().unwrap());
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::Ok);
                res.body()
                    .concat()
                    .and_then(|body| {
                        assert_eq!(&body as &[u8], payload2);
                        Ok(())
                    })
            }))
            .unwrap();
    }
}

#[macro_use]
extern crate lazy_static;
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
mod pool;

use dotenv::dotenv;
use flow::Flow;
use futures::{Future, Sink, Stream, future, stream};
use hyper::{Method, StatusCode};
use hyper::header::{ContentLength, ContentType};
use hyper::server::{Http, Request, Response, Service};
use pool::Pool;
use regex::Regex;
use std::{env, thread};
use std::sync::{Arc, Barrier, RwLock};
use tokio::reactor::{self, Core};

#[derive(Debug)]
pub enum Error {
    Invalid,
    Internal(hyper::Error),
}

#[derive(Clone)]
struct FluxService {
    remote: reactor::Remote,
    pool: Arc<RwLock<Pool>>,
}

#[derive(Serialize, Deserialize)]
struct FlowReqParam {
    pub size: Option<u64>,
}

type ResponseFuture = future::BoxFuture<Response, hyper::Error>;

impl FluxService {
    fn new(pool: Arc<RwLock<Pool>>, remote: reactor::Remote) -> Self {
        FluxService { remote, pool }
    }

    fn handle_new(&self, req: Request, _route: regex::Captures) -> ResponseFuture {
        if let Some(&ContentLength(length)) = req.headers().get() {
            if length == 0 {
                return future::ok(Response::new().with_status(StatusCode::BadRequest)).boxed();
            }
        } else {
            return future::ok(Response::new().with_status(StatusCode::LengthRequired)).boxed();
        }

        let pool_ptr = self.pool.clone();
        req.body()
            .concat()
            .map_err(|err| Error::Internal(err))
            .and_then(|body| {
                serde_json::from_slice::<FlowReqParam>(&body).map_err(|_| Error::Invalid)
            })
            .and_then(move |param| {
                let flow_ptr = Flow::new(param.size);
                let flow_id = flow_ptr.read().unwrap().id.to_owned();
                {
                    let mut pool = pool_ptr.write().unwrap();
                    pool.insert(flow_ptr);
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
            let pool = self.pool.read().unwrap();
            if let Some(flow) = pool.get(flow_id) {
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
                match flush_chunk {
                    Some(flush_chunk) => {
                        let mut flow = flow.write().unwrap();
                        flow.push(&flush_chunk)
                            .map(|_| ret_chunk)
                            .map_err(|_| hyper::error::Error::Incomplete)
                            .boxed()
                    }
                    None => future::ok(ret_chunk).boxed(),
                }
            })
            .and_then(|_| Ok("Ok"))
            .or_else(|_| Ok("NotReady"))
            .and_then(|body| {
                Ok(Response::new()
                       .with_header(ContentType::plaintext())
                       .with_header(ContentLength(body.len() as u64))
                       .with_body(body))
            })
            .boxed()
    }

    fn handle_eof(&self, _req: Request, route: regex::Captures) -> ResponseFuture {
        let flow_id = route.get(1).unwrap().as_str();
        let flow = {
            let pool = self.pool.read().unwrap();
            if let Some(flow) = pool.get(flow_id) {
                flow.clone()
            } else {
                return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed();
            }
        };
        {
            let mut flow = flow.write().unwrap();
            flow.close()
                .then(|result| match result {
                          Ok(_) => Ok("Ok"),
                          Err(flow::Error::Invalid) => Ok("Closed"),
                          Err(flow::Error::NotReady) => Ok("NotReady"),
                          Err(err) => Err(err),
                      })
                .and_then(|body| {
                    Ok(Response::new()
                           .with_header(ContentType::plaintext())
                           .with_header(ContentLength(body.len() as u64))
                           .with_body(body))
                })
                .or_else(|_| Ok(Response::new().with_status(StatusCode::InternalServerError)))
                .boxed()
        }
    }

    fn handle_fetch(&self, _req: Request, route: regex::Captures) -> ResponseFuture {
        let flow_id = route.get(1).unwrap().as_str();
        let chunk_index: u64 = if let Ok(index) = route.get(2).unwrap().as_str().parse() {
            index
        } else {
            return future::ok(Response::new().with_status(StatusCode::BadRequest)).boxed();
        };
        let flow = {
            let pool = self.pool.read().unwrap();
            if let Some(flow) = pool.get(flow_id) {
                flow.clone()
            } else {
                return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed();
            }
        };
        {
            let flow = flow.read().unwrap();
            flow.pull(chunk_index, None)
                .and_then(|chunk| {
                    future::ok(Response::new()
                                   .with_header(ContentType::octet_stream())
                                   .with_header(ContentLength(chunk.len() as u64))
                                   .with_body(chunk))
                })
                .or_else(|err| match err {
                             flow::Error::Eof | flow::Error::Dropped => {
                                 future::ok(Response::new().with_status(StatusCode::NotFound))
                             }
                             _ => {
                                 future::ok(Response::new()
                                                .with_status(StatusCode::InternalServerError))
                             }
                         })
                .boxed()
        }
    }

    fn handle_pull(&self, _req: Request, route: regex::Captures) -> ResponseFuture {
        let flow_id = route.get(1).unwrap().as_str();
        let flow = {
            let pool = self.pool.read().unwrap();
            if let Some(flow) = pool.get(flow_id) {
                flow.clone()
            } else {
                return future::ok(Response::new().with_status(StatusCode::NotFound)).boxed();
            }
        };

        let body_stream = stream::unfold(Some(0), move |chunk_index| {
            // Check if the flow is EOF.
            if let Some(chunk_index) = chunk_index {
                let flow = flow.read().unwrap();

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

impl Service for FluxService {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = ResponseFuture;

    fn call(&self, req: Request) -> Self::Future {
        lazy_static! {
            static ref PATTERN_NEW: Regex = Regex::new(r"/new").unwrap();
            static ref PATTERN_PUSH: Regex = Regex::new(r"/([a-f0-9]{32})/push").unwrap();
            static ref PATTERN_EOF: Regex = Regex::new(r"/([a-f0-9]{32})/eof").unwrap();
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
                } else if let Some(route) = PATTERN_EOF.captures(path) {
                    self.handle_eof(req, route)
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
                 block: bool)
                 -> Option<std::net::SocketAddr> {
    let upstream_listener = std::net::TcpListener::bind(&addr).unwrap();
    let pool_ptr = Pool::new();
    let mut workers = Vec::with_capacity(num_worker);
    let barrier = Arc::new(Barrier::new(num_worker.checked_add(1).unwrap()));

    for idx in 0..num_worker {
        let addr = addr.clone();
        let listener = upstream_listener.try_clone().unwrap();
        let barrier = barrier.clone();
        let pool_ptr = pool_ptr.clone();
        let worker = thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let handle = core.handle();
            let remote = core.remote();
            let listener = tokio::net::TcpListener::from_listener(listener, &addr, &handle)
                .unwrap();
            let http = Http::new();
            let acceptor = listener
                .incoming()
                .for_each(|(io, addr)| {
                    http.bind_connection(&handle,
                                         io,
                                         addr,
                                         FluxService::new(pool_ptr.clone(), remote.clone()));
                    Ok(())
                });
            println!("Worker #{} is started.", idx);
            barrier.wait();
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
    use flow;
    use futures::{Future, Sink, Stream, future, stream};
    use hyper;
    use hyper::Method::{Get, Post, Put};
    use hyper::client::{Client, Request};
    use hyper::header::ContentLength;
    use hyper::status::StatusCode;
    use regex::Regex;
    use std::{str, thread};
    use std::sync::mpsc;
    use std::time::Duration;
    use tokio::reactor::Core;

    fn spawn_server() -> String {
        let port = start_service("127.0.0.1:0".parse().unwrap(), 1, false).unwrap().port();
        format!("http://127.0.0.1:{}", port)
    }

    fn create_flow(prefix: &str, param: &str) -> String {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let mut req = Request::new(Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(param.to_owned());
        req.headers_mut().set(ContentLength(param.len() as u64));

        let mut flow_id = String::new();
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

        flow_id
    }

    fn req_push(prefix: &str, flow_id: &str, payload: &[u8]) -> (StatusCode, Option<String>) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let mut req = Request::new(Post, format!("{}/{}/push", prefix, flow_id).parse().unwrap());
        req.set_body(payload.to_vec());

        let mut status_code = StatusCode::ImATeapot;
        let mut response = None;
        core.run(client
                     .request(req)
                     .and_then(|res| {
                status_code = res.status();
                let fut = if status_code == StatusCode::Ok {
                    res.body()
                        .concat()
                        .and_then(|body| Ok(Some(String::from_utf8(body.to_vec()).unwrap())))
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

    fn req_close(prefix: &str, flow_id: &str) -> (StatusCode, Option<String>) {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let req = Request::new(Post, format!("{}/{}/eof", prefix, flow_id).parse().unwrap());

        let mut status_code = StatusCode::ImATeapot;
        let mut response = None;
        core.run(client
                     .request(req)
                     .and_then(|res| {
                status_code = res.status();
                let fut = if status_code == StatusCode::Ok {
                    res.body()
                        .concat()
                        .and_then(|body| Ok(Some(String::from_utf8(body.to_vec()).unwrap())))
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

    #[test]
    fn invalid_route() {
        let prefix = &spawn_server();
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle());

        let req = Request::new(Post, format!("{}/neo", prefix).parse().unwrap());
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
        let prefix = &spawn_server();
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
                        let flow_id = str::from_utf8(&body).unwrap();
                        assert!(Regex::new("[a-f0-9]{32}").unwrap().find(flow_id).is_some());
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
                        let flow_id = str::from_utf8(&body).unwrap();
                        assert!(Regex::new("[a-f0-9]{32}").unwrap().find(flow_id).is_some());
                        Ok(())
                    })
            }))
            .unwrap();

        let mut req = Request::new(Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{}"#);
        core.run(client
                     .request(req)
                     .and_then(|res| {
                assert_eq!(res.status(), StatusCode::LengthRequired);
                Ok(())
            }))
            .unwrap();

        let mut req = Request::new(Post, format!("{}/new", prefix).parse().unwrap());
        req.set_body(r#"{"size": 4O96}"#);
        req.headers_mut().set(ContentLength(14));
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

        let flow_id = &create_flow(prefix, r#"{}"#);
        let fake_id = "bdc62e9323003d0f5cb44c8c745a0470";
        let payload1: &[u8] = b"The quick brown fox jumps\nover the lazy dog";
        let payload2: &[u8] = b"The guick yellow fox jumps\nover the fast cat";

        // The empty chunk should be ignored.
        // No content length.
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
        // With 0 content length.
        assert_eq!(req_push(prefix, flow_id, b""), (StatusCode::Ok, Some(String::from("Ok"))));

        assert_eq!(req_push(prefix, fake_id, payload1), (StatusCode::NotFound, None));
        assert_eq!(req_push(prefix, flow_id, payload1), (StatusCode::Ok, Some(String::from("Ok"))));
        assert_eq!(req_push(prefix, flow_id, payload2), (StatusCode::Ok, Some(String::from("Ok"))));

        assert_eq!(req_fetch(prefix, fake_id, 0), (StatusCode::NotFound, None));
        assert_eq!(req_fetch(prefix, flow_id, 0), (StatusCode::Ok, Some(payload1.to_vec())));
        assert_eq!(req_fetch(prefix, flow_id, 1), (StatusCode::Ok, Some(payload2.to_vec())));

        let thd = {
            let prefix = prefix.clone();
            let flow_id = flow_id.clone();
            let payload1 = payload1.clone();
            thread::spawn(move || {
                let prefix = &prefix;
                let flow_id = &flow_id;
                thread::park();
                thread::sleep(Duration::from_millis(1000));
                assert_eq!(req_push(prefix, flow_id, payload1),
                           (StatusCode::Ok, Some(String::from("Ok"))));
            })
        };

        thd.thread().unpark();
        assert_eq!(req_fetch(prefix, flow_id, 2), (StatusCode::Ok, Some(payload1.to_vec())));
    }

    #[test]
    fn handle_push_pull() {
        let payload = vec![1u8; flow::MAX_SIZE * 10];
        let prefix = &spawn_server();
        let flow_id = &create_flow(prefix, &format!(r#"{{"size": {}}}"#, payload.len()));
        let fake_id = "bdc62e9323003d0f5cb44c8c745a0470";

        {
            let prefix = prefix.clone();
            let flow_id = flow_id.clone();
            let payload = payload.clone();
            thread::spawn(move || {
                let prefix = &prefix;
                let flow_id = &flow_id;

                for chunk in payload.chunks(flow::MAX_SIZE * 2 + 13) {
                    assert_eq!(req_push(prefix, flow_id, chunk),
                               (StatusCode::Ok, Some(String::from("Ok"))));
                }
                assert_eq!(req_close(prefix, flow_id), (StatusCode::Ok, Some(String::from("Ok"))));
            });
        }

        assert_eq!(req_pull(prefix, fake_id), (StatusCode::NotFound, None));
        assert_eq!(req_pull(prefix, flow_id), (StatusCode::Ok, Some(payload)));
    }

    #[test]
    fn handle_eof() {
        let prefix = &spawn_server();
        let flow_id = &create_flow(prefix, r#"{}"#);
        let fake_id = "bdc62e9323003d0f5cb44c8c745a0470";

        assert_eq!(req_close(prefix, fake_id), (StatusCode::NotFound, None));
        assert_eq!(req_close(prefix, flow_id), (StatusCode::Ok, Some(String::from("Ok"))));
        assert_eq!(req_push(prefix, flow_id, b"Hello"),
                   (StatusCode::Ok, Some(String::from("NotReady"))));
        assert_eq!(req_close(prefix, flow_id), (StatusCode::Ok, Some(String::from("Closed"))));
        assert_eq!(req_fetch(prefix, flow_id, 0), (StatusCode::NotFound, None));
    }

    #[test]
    fn recycle() {
        let prefix = &spawn_server();
        let flow_id = &create_flow(prefix, r#"{}"#);

        assert_eq!(req_close(prefix, flow_id), (StatusCode::Ok, Some(String::from("Ok"))));
        assert_eq!(req_close(prefix, flow_id), (StatusCode::Ok, Some(String::from("Closed"))));
        assert_eq!(req_fetch(prefix, flow_id, 0), (StatusCode::NotFound, None));
        assert_eq!(req_close(prefix, flow_id), (StatusCode::NotFound, None));
    }

    #[test]
    fn dropped() {
        let prefix = &spawn_server();
        let flow_id = &create_flow(prefix, r#"{}"#);
        let payload1: &[u8] = b"The quick brown fox jumps\nover the lazy dog";
        let payload2: &[u8] = b"The guick yellow fox jumps\nover the fast cat";

        assert_eq!(req_push(prefix, flow_id, payload1), (StatusCode::Ok, Some(String::from("Ok"))));
        assert_eq!(req_push(prefix, flow_id, payload2), (StatusCode::Ok, Some(String::from("Ok"))));
        assert_eq!(req_close(prefix, flow_id), (StatusCode::Ok, Some(String::from("Ok"))));
        assert_eq!(req_fetch(prefix, flow_id, 0), (StatusCode::Ok, Some(payload1.to_vec())));
        assert_eq!(req_fetch(prefix, flow_id, 0), (StatusCode::NotFound, None));
        assert_eq!(req_pull(prefix, flow_id), (StatusCode::Ok, Some(payload2.to_vec())));
    }

    #[test]
    fn full_push() {
        let prefix = &spawn_server();
        let flow_id = &create_flow(prefix, r#"{}"#);

        assert_eq!(req_push(prefix, flow_id, b"Hello"), (StatusCode::Ok, Some(String::from("Ok"))));

        let (tx, rx) = mpsc::channel();

        {
            let prefix = prefix.clone();
            let flow_id = flow_id.clone();
            let tx = tx.clone();
            thread::spawn(move || {
                let prefix = &prefix;
                let flow_id = &flow_id;
                req_push(prefix, flow_id, &vec![0u8; flow::MAX_CAPACITY as usize]);
                req_close(prefix, flow_id);
                tx.send(()).unwrap();
            });
        }

        {
            let prefix = prefix.clone();
            let flow_id = flow_id.clone();
            let tx = tx.clone();
            thread::spawn(move || {
                let prefix = &prefix;
                let flow_id = &flow_id;
                req_push(prefix, flow_id, &vec![0u8; flow::MAX_CAPACITY as usize]);
                req_close(prefix, flow_id);
                tx.send(()).unwrap();
            });
        }

        rx.recv().unwrap();
        assert_eq!(req_push(prefix, flow_id, b"Hello"),
                   (StatusCode::Ok, Some(String::from("NotReady"))));
        assert_eq!(req_close(prefix, flow_id), (StatusCode::Ok, Some(String::from("NotReady"))));

        req_pull(prefix, flow_id);
        rx.recv().unwrap();
    }

    #[test]
    fn racing_pull() {
        let prefix = &spawn_server();
        let flow_id = &create_flow(prefix, r#"{}"#);

        let (send_tx, send_rx) = mpsc::channel();

        {
            let prefix = prefix.clone();
            let flow_id = flow_id.clone();
            thread::spawn(move || {
                let prefix = prefix;
                let flow_id = flow_id;

                let mut core = Core::new().unwrap();
                let handle = core.handle();
                let client = Client::new(&handle);

                // Infinite flow.
                let body_stream = stream::unfold((), move |_| {
                    send_tx.send(()).unwrap();
                    Some(future::ok((Ok(hyper::Chunk::from(vec![0u8; flow::MAX_SIZE])), ())))
                });

                let mut req =
                    Request::new(Post, format!("{}/{}/push", &prefix, &flow_id).parse().unwrap());
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
                break;
            }
        }

        thd.thread().unpark();
        thd.join().unwrap();
    }
}

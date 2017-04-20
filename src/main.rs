extern crate dotenv;
extern crate futures;
extern crate hyper;
extern crate tokio_core as tokio;
extern crate uuid;
mod flow;

use dotenv::dotenv;
use flow::Flow;
use futures::{Future, Stream};
use futures::future;
use hyper::header::{ContentLength, ContentType};
use hyper::server::{Http, Service, Request, Response};
use hyper::{Method, StatusCode};
use std::collections::HashMap;
use std::sync::{Arc, Barrier, RwLock};
use std::{env, thread};
use tokio::reactor::Core;

#[derive(Clone)]
struct FluxService {
    flow_bucket: Arc<RwLock<HashMap<u64, u64>>>,
}

type ResponseFuture = Box<Future<Item = Response, Error = hyper::Error>>;

impl FluxService {
    fn new() -> Self {
        FluxService {
            flow_bucket: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn handle_new(&self) -> ResponseFuture {
        let flow = Flow::new();
        let body = flow.id.into_bytes();
        future::ok(Response::new()
                   .with_header(ContentType::plaintext())
                   .with_header(ContentLength(body.len() as u64))
                   .with_body(body)).boxed()
    }
}

impl Service for FluxService {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = ResponseFuture;

    fn call(&self, req: Request) -> Self::Future {
        match req.method() {
            &Method::Post => {
                if req.path() == "/new" {
                    self.handle_new()
                } else {
                    future::ok(Response::new().with_status(StatusCode::NotFound)).boxed()
                }
                /*
                let datalen = if let Some(datalen) = req.headers().get::<ContentLength>() {
                    std::cmp::min(datalen.0, 4194304) as usize
                } else {
                    4096
                };
                req.body().fold(Vec::<u8>::with_capacity(datalen), |mut data, chunk| {
                    data.extend_from_slice(&chunk);
                    Ok::<_, Self::Error>(data)
                }).and_then(|body| {
                    Ok(Response::new()
                       .with_header(ContentType::plaintext())
                       .with_header(ContentLength(body.len() as u64))
                       .with_body(body))
                }).boxed()
                */
            }
            _ => future::ok(Response::new().with_status(StatusCode::MethodNotAllowed)).boxed()
        }
    }
}

fn start_service(addr: std::net::SocketAddr, num_worker: usize, block: bool)
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
            let listener = tokio::net::TcpListener::from_listener(moved_listener,
                                                                  &moved_addr,
                                                                  &handle).unwrap();
            let http = Http::new();
            let acceptor = listener.incoming().for_each(|(io, addr)| {
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
    use futures::{Future, Stream};
    use hyper::Method::Post;
    use hyper::client::{Client, Request};
    use hyper::status::StatusCode;
    use std::str;
    use super::start_service;
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

        let req = Request::new(Post, format!("{}/new", prefix).parse().unwrap());
        core.run(client.request(req).and_then(|res| {
            assert_eq!(res.status(), StatusCode::Ok);
            res.body().concat().and_then(|body| {
                let flow_id = str::from_utf8(&body).unwrap();
                Ok(())
            })
        })).unwrap();
    }
}

extern crate dotenv;
extern crate futures;
extern crate hyper;
extern crate tokio_core as tokio;

use dotenv::dotenv;
use futures::{Future, Stream};
use futures::future;
use hyper::header::{ContentLength, ContentType};
use hyper::server::{Http, Service, Request, Response};
use hyper::{Method, StatusCode};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::{env, thread};
use tokio::reactor::Core;

#[derive(Clone)]
struct FluxService {
    flow_bucket: Arc<RwLock<HashMap<u64, u64>>>,
}

impl FluxService {
    fn new() -> Self {
        FluxService {
            flow_bucket: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Service for FluxService {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        match req.method() {
            &Method::Post => {
                let datalen = if let Some(datalen) = req.headers().get::<ContentLength>() {
                    std::cmp::min(datalen.0, 4194304) as usize
                } else {
                    4096
                };
                req.body().fold(Vec::<u8>::with_capacity(datalen), |mut data, chunk| {
                    data.extend_from_slice(chunk.as_ref());
                    Ok::<_, Self::Error>(data)
                }).and_then(|body| {
                    Ok(Response::new()
                       .with_header(ContentType::plaintext())
                       .with_header(ContentLength(body.len() as u64))
                       .with_body(body))
                }).boxed()
            }
            _ => future::ok(Response::new().with_status(StatusCode::MethodNotAllowed)).boxed()
        }
    }
}

fn main() {
    dotenv().ok();

    let service = FluxService::new();

    let addr: std::net::SocketAddr = env::var("SERVER_ADDRESS").unwrap().parse().unwrap();
    let upstream_listener = std::net::TcpListener::bind(&addr).unwrap();

    let mut threads = Vec::new();
    for idx in 0..2 {
        let shared_addr = addr.clone();
        let shared_listener = upstream_listener.try_clone().unwrap();
        let shared_service = service.clone();
        let thd = thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let handle = core.handle();
            let listener = tokio::net::TcpListener::from_listener(shared_listener,
                                                                  &shared_addr,
                                                                  &handle).unwrap();
            let http = Http::new();
            let acceptor = listener.incoming().for_each(|(stream, addr)| {
                http.bind_connection(&handle, stream, addr, shared_service.clone());
                Ok(())
            });
            println!("Thread #{} is started.", idx);
            core.run(acceptor).unwrap();
        });
        threads.push(thd);
    }
    for thd in threads {
        thd.join().unwrap();
    }
}

extern crate hyper;
extern crate futures;
extern crate tokio_core as tokio;

use futures::Stream;
use futures::future::{self, FutureResult};
use hyper::header::{ContentLength, ContentType};
use hyper::server::{Http, Service, Request, Response};
use std::thread;
use tokio::reactor::Core;

#[derive(Clone, Copy)]
struct FluxService;

impl Service for FluxService {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = FutureResult<Response, hyper::Error>;

    fn call(&self, _req: Request) -> Self::Future {
        let resp = [65u8; 65536].to_vec();
        future::ok(Response::new()
                   .with_header(ContentLength(resp.len() as u64))
                   .with_header(ContentType::plaintext())
                   .with_body(resp))
    }
}

fn main() {
    let addr: std::net::SocketAddr = "127.0.0.1:3000".parse().unwrap();
    let upstream_listener = std::net::TcpListener::bind(&addr).unwrap();

    let mut threads = Vec::new();
    for idx in 0..2 {
        let shared_addr = addr.clone();
        let shared_listener = upstream_listener.try_clone().unwrap();
        let thd = thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let handle = core.handle();
            let listener = tokio::net::TcpListener::from_listener(shared_listener,
                                                                  &shared_addr,
                                                                  &handle).unwrap();
            let http = Http::new();
            let acceptor = listener.incoming().for_each(|(stream, addr)| {
                http.bind_connection(&handle, stream, addr, FluxService);
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

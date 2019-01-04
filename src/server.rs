extern crate furakus;
extern crate futures;
extern crate hyper;
extern crate tokio;

use furakus::utils::*;
use futures::{future, prelude::*};
use hyper::{Body, Request, Response};
use tokio::runtime::TaskExecutor;

struct FlowService {
    executor: TaskExecutor,
}

impl FlowService {
    fn new(executor: TaskExecutor) -> FlowService {
        FlowService { executor }
    }
}

impl hyper::service::Service for FlowService {
    type ReqBody = Body;
    type ResBody = Body;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Box<dyn Future<Item = Response<Self::ResBody>, Error = Self::Error> + Send>;

    fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
        let task = futures::stream::iter_ok::<_, std::io::Error>(0..1024)
            .map(move |_| hyper::Chunk::from(&[0u8; 65536] as &[u8]));
        let body = Body::wrap_stream(task);
        future::ok(Response::builder().status(200).body(body).unwrap()).boxed2()
    }
}

fn main() {
    let runner = tokio::runtime::Builder::new()
        .core_threads(4)
        .build()
        .unwrap();
    let server = {
        let mut http = hyper::server::conn::Http::new();
        let executor = runner.executor();
        let addr = ([127, 0, 0, 1], 3000).into();
        let listener = tokio::net::TcpListener::bind(&addr).unwrap();
        listener
            .incoming()
            .for_each(move |stream| {
                let service = FlowService::new(executor.clone());
                executor.spawn(http.serve_connection(stream, service).map_err(|_| ()));
                future::ok(())
            })
            .map_err(|_| ())
    };
    runner.block_on_all(server).unwrap();
}

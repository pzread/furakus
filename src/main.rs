#![feature(async_await)]
extern crate futures;
extern crate hyper;
extern crate lazy_static;
extern crate regex;
extern crate tokio;
extern crate url;
mod server;
mod service;

use hyper::rt::Future;

fn main() {
    let service_factory = service::FlowServiceFactory::new();
    let runner = tokio::runtime::Builder::new()
        .core_threads(1)
        .build()
        .unwrap();
    server::spawn(
        runner.executor(),
        &"10.16.0.1:3000".parse().unwrap(),
        service_factory,
    );
    runner.shutdown_on_idle().wait().unwrap();
}

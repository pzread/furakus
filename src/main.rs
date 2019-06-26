#![feature(async_await)]

extern crate byteorder;
extern crate bytes;
extern crate futures;
extern crate hyper;
extern crate lazy_static;
extern crate parking_lot;
extern crate regex;
extern crate ring;
extern crate tokio;
extern crate url;
mod lib;
mod server;
mod service;
mod utils;

use hyper::rt::Future;

fn main() {
    let service_factory = service::FlowServiceFactory::new();
    let runner = tokio::runtime::Builder::new()
        .core_threads(2)
        .build()
        .unwrap();
    server::spawn(
        runner.executor(),
        &"127.0.0.1:3000".parse().unwrap(),
        service_factory,
    );
    runner.shutdown_on_idle().wait().unwrap();
}

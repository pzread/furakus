extern crate furakus;
extern crate futures;
extern crate hyper;
extern crate lazy_static;
extern crate native_tls;
extern crate regex;
extern crate serde;
extern crate serde_derive;
extern crate structopt;
extern crate tokio;
extern crate tokio_tls;
extern crate toml;

mod service;
mod stream_adapter;

use futures::{future, prelude::*};
use serde_derive::Deserialize;
use service::{FlowServiceFactory, ServiceFactory};
use std::{error::Error as StdError, io::Read, path::PathBuf};
use stream_adapter::{DummyStreamAdapter, StreamAdapter, TlsStreamAdapter};
use structopt::StructOpt;

#[derive(Deserialize)]
struct TlsConfig {
    pkcs12_path: PathBuf,
}

#[derive(Deserialize)]
struct ServerConfig {
    listen_addr: std::net::SocketAddr,
    num_worker: usize,
    tls: Option<TlsConfig>,
}

#[derive(Deserialize)]
struct Config {
    server: ServerConfig,
}

#[derive(StructOpt, Debug)]
#[structopt(name = "Furakus")]
struct Argument {
    #[structopt(short = "c", long = "config", parse(from_os_str))]
    config_path: PathBuf,
}

fn start_server<T: ServiceFactory + 'static + Send>(config: &ServerConfig, service_factory: T) {
    let adapter: Box<dyn StreamAdapter<InputStream = _> + Send> =
        if let Some(ref tls_config) = config.tls {
            Box::new(TlsStreamAdapter::new(&tls_config.pkcs12_path))
        } else {
            Box::new(DummyStreamAdapter::new())
        };
    let runner = tokio::runtime::Builder::new()
        .core_threads(config.num_worker)
        .build()
        .unwrap();
    let executor = runner.executor();
    let listener = tokio::net::TcpListener::bind(&config.listen_addr).unwrap();
    let server_fut = listener
        .incoming()
        .for_each(move |stream| {
            let service = service_factory.new_service();
            let fut = adapter
                .accept(stream)
                .and_then(move |stream| {
                    let http = hyper::server::conn::Http::new();
                    http.serve_connection(stream, service)
                        .map_err(|err| Box::new(err) as Box<StdError + Send>)
                })
                .map_err(|_| ());
            executor.spawn(fut);
            future::ok(())
        })
        .map_err(|_| ());
    runner.block_on_all(server_fut).unwrap();
}

fn main() {
    let config: Config = {
        let args = Argument::from_args();
        let mut config_file = std::fs::File::open(args.config_path).unwrap();
        let mut buf = Vec::new();
        config_file.read_to_end(&mut buf).unwrap();
        toml::from_slice(&buf).unwrap()
    };
    start_server(&config.server, FlowServiceFactory::new());
}

extern crate furakus;
extern crate futures;
extern crate hyper;
extern crate lazy_static;
extern crate native_tls;
extern crate regex;
extern crate ring;
extern crate serde;
extern crate serde_derive;
extern crate structopt;
extern crate tokio;
extern crate tokio_tls;
extern crate toml;

mod auth;
mod service;
mod stream_adapter;

use futures::{future, prelude::*};
use serde_derive::Deserialize;
use service::{Config as ServiceConfig, FlowServiceFactory, ServiceFactory};
use std::{error::Error as StdError, io::Read, net::SocketAddr, path::PathBuf};
use stream_adapter::{DummyStreamAdapter, StreamAdapter, TlsStreamAdapter};
use structopt::StructOpt;

#[derive(Deserialize)]
struct TlsConfig {
    pkcs12_path: PathBuf,
}

#[derive(Deserialize)]
struct ServerConfig {
    listen_addr: SocketAddr,
    num_worker: usize,
    tls: Option<TlsConfig>,
}

#[derive(Deserialize)]
struct Config {
    server: ServerConfig,
    service: ServiceConfig,
}

#[derive(StructOpt, Debug)]
#[structopt(name = "Furakus")]
struct Argument {
    #[structopt(short = "c", long = "config", parse(from_os_str))]
    config_path: PathBuf,
}

fn start_server<T: ServiceFactory + Send + 'static>(
    config: &ServerConfig,
    service_factory: T,
) -> (tokio::runtime::Runtime, SocketAddr) {
    let mut runner = tokio::runtime::Builder::new()
        .core_threads(config.num_worker)
        .build()
        .unwrap();
    let executor = runner.executor();

    let listener = tokio::net::TcpListener::bind(&config.listen_addr).unwrap();
    let bind_addr = listener.local_addr().unwrap();

    let adapter: Box<dyn StreamAdapter<_> + Send> = match config.tls {
        Some(ref tls_config) => Box::new(TlsStreamAdapter::new(&tls_config.pkcs12_path)),
        None => Box::new(DummyStreamAdapter::new()),
    };

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
    runner.spawn(server_fut);

    (runner, bind_addr)
}

fn main() {
    let config: Config = {
        let args = Argument::from_args();
        let mut config_file = std::fs::File::open(args.config_path).unwrap();
        let mut buf = Vec::new();
        config_file.read_to_end(&mut buf).unwrap();
        toml::from_slice(&buf).unwrap()
    };
    let (runner, _) = start_server(
        &config.server,
        FlowServiceFactory::new(&config.service, auth::HMACAuthorizer::new()),
    );
    runner.block_on_all(future::ok::<(), ()>(())).unwrap();
}

#[cfg(test)]
mod tests {
    extern crate reqwest;

    use self::reqwest::Certificate;
    use super::{service::ServiceFactory, start_server, ServerConfig, TlsConfig};
    use furakus::utils::*;
    use futures::{future, sync::oneshot, Future};
    use hyper::{service::Service as HyperService, Body, Request, Response, StatusCode};
    use std::{
        error::Error as StdError,
        fs::File,
        io::Read,
        net::{IpAddr, SocketAddr},
        thread,
    };

    const TEST_EXPECT_DATA: &[u8] = b"Hello World";
    const TEST_LISTEN_ADDR: &str = "127.0.0.1:0";
    const TEST_NUM_WORKER: usize = 1;
    const TEST_URI_PATH: &str = "/app/index.html";
    const TEST_ROOTCA_PATH: &str = "./tests/ca.der";
    const TEST_PKCS12_PATH: &str = "./tests/test.p12";

    struct CheckServiceFactory;

    impl ServiceFactory for CheckServiceFactory {
        type Error = <Self::Service as HyperService>::Error;
        type Future = <Self::Service as HyperService>::Future;
        type Service = CheckService;

        fn new_service(&self) -> Self::Service {
            CheckService
        }
    }

    struct CheckService;

    impl hyper::service::Service for CheckService {
        type ReqBody = Body;
        type ResBody = Body;
        type Error = Box<dyn StdError + Send + Sync>;
        type Future = Box<dyn Future<Item = Response<Self::ResBody>, Error = Self::Error> + Send>;

        fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
            assert_eq!(req.uri().path(), TEST_URI_PATH);
            future::ok(
                Response::builder()
                    .status(StatusCode::OK)
                    .body(TEST_EXPECT_DATA.into())
                    .unwrap(),
            )
            .into_box()
        }
    }

    fn build_host(addr: &SocketAddr) -> String {
        let ip = match addr.ip() {
            IpAddr::V4(ip) => {
                let [a, b, c, d] = ip.octets();
                format!("{:0}.{:0}.{:0}.{:0}", a, b, c, d)
            }
            _ => panic!(),
        };
        format!("{}:{:0}", ip, addr.port())
    }

    fn check_response(mut res: reqwest::Response) {
        assert_eq!(res.status(), StatusCode::OK);
        let mut buf: Vec<u8> = Vec::new();
        res.copy_to(&mut buf).unwrap();
        assert_eq!(buf.as_slice(), TEST_EXPECT_DATA);
    }

    #[test]
    fn http_server() {
        let config = ServerConfig {
            listen_addr: TEST_LISTEN_ADDR.parse().unwrap(),
            num_worker: TEST_NUM_WORKER,
            tls: None,
        };
        let (mut runner, bind_addr) = start_server(&config, CheckServiceFactory);
        let host = build_host(&bind_addr);
        let (tx, rx) = oneshot::channel();
        let thd = thread::spawn(move || {
            let res = reqwest::get(&format!("http://{}{}", host, TEST_URI_PATH)).unwrap();
            tx.send(()).unwrap();
            res
        });
        runner.block_on(rx).unwrap();
        check_response(thd.join().unwrap());
    }

    #[test]
    fn https_h2_server() {
        let config = ServerConfig {
            listen_addr: TEST_LISTEN_ADDR.parse().unwrap(),
            num_worker: TEST_NUM_WORKER,
            tls: Some(TlsConfig {
                pkcs12_path: TEST_PKCS12_PATH.into(),
            }),
        };
        let (mut runner, bind_addr) = start_server(&config, CheckServiceFactory);
        let host = build_host(&bind_addr);
        let (tx, rx) = oneshot::channel();
        let thd = thread::spawn(move || {
            let rootca = {
                let mut buf = Vec::new();
                File::open(TEST_ROOTCA_PATH)
                    .unwrap()
                    .read_to_end(&mut buf)
                    .unwrap();
                Certificate::from_der(&buf).unwrap()
            };
            let client = reqwest::Client::builder()
                .add_root_certificate(rootca)
                .h2_prior_knowledge()
                .build()
                .unwrap();
            let res = client
                .get(&format!("https://{}{}", host, TEST_URI_PATH))
                .send()
                .unwrap();
            tx.send(()).unwrap();
            res
        });
        runner.block_on(rx).unwrap();
        check_response(thd.join().unwrap());
    }
}

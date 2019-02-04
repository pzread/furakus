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

mod stream_adapter;

use furakus::utils::*;
use futures::{future, prelude::*, sync::mpsc as future_mpsc};
use hyper::{Body, Chunk, Method, Request, Response, StatusCode};
use lazy_static::lazy_static;
use regex::Regex;
use serde_derive::Deserialize;
use std::{
    collections::HashMap,
    error::Error as StdError,
    io::Read,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use stream_adapter::{DummyStreamAdapter, StreamAdapter, TlsStreamAdapter};
use structopt::StructOpt;

struct FlowService {
    pool: Arc<Mutex<HashMap<String, future_mpsc::Receiver<Chunk>>>>,
}

type ServiceRequest = Request<Body>;
type ServiceError = Box<dyn StdError + Send + Sync>;
type ResponseFuture = Box<dyn Future<Item = Response<Body>, Error = ServiceError> + Send>;

impl FlowService {
    fn new(pool: Arc<Mutex<HashMap<String, future_mpsc::Receiver<Chunk>>>>) -> FlowService {
        FlowService { pool }
    }

    fn response_status(status: StatusCode) -> ResponseFuture {
        future::ok(
            Response::builder()
                .status(status)
                .body(Body::empty())
                .unwrap(),
        )
        .into_box()
    }

    fn handle_push(&self, req: ServiceRequest, route: regex::Captures) -> ResponseFuture {
        let (tx, rx) = future_mpsc::channel(2);

        let flow_id = route.get(1).unwrap().as_str();
        {
            let mut pool = self.pool.lock().unwrap();
            pool.insert(flow_id.to_string(), rx);
        }

        tx.sink_map_err(|_| ())
            .send_all(req.into_body().map(|chunk| chunk).map_err(|_| ()))
            .then(|ret| {
                if ret.is_ok() {
                    Self::response_status(StatusCode::OK)
                } else {
                    Self::response_status(StatusCode::INTERNAL_SERVER_ERROR)
                }
            })
            .into_box()
    }

    fn handle_pull(&self, req: ServiceRequest, route: regex::Captures) -> ResponseFuture {
        let flow_id = route.get(1).unwrap().as_str();
        let rx = {
            let mut pool = self.pool.lock().unwrap();
            match pool.remove(flow_id) {
                Some(rx) => rx,
                None => return Self::response_status(StatusCode::NOT_FOUND),
            }
        };
        let body =
            Body::wrap_stream(rx.map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "")));
        future::ok(
            Response::builder()
                .status(StatusCode::OK)
                .body(body)
                .unwrap(),
        )
        .into_box()
    }
}

impl hyper::service::Service for FlowService {
    type ReqBody = Body;
    type ResBody = Body;
    type Error = ServiceError;
    type Future = ResponseFuture;

    fn call(&mut self, req: ServiceRequest) -> Self::Future {
        lazy_static! {
            static ref PATTERN_PUSH: Regex = Regex::new(r"^/flow/([a-f0-9]{32})/push$").unwrap();
            static ref PATTERN_PULL: Regex = Regex::new(r"^/flow/([a-f0-9]{32})/pull$").unwrap();
        }

        let path = &req.uri().path().to_string();
        match req.method() {
            &Method::PUT => {
                if let Some(route) = PATTERN_PUSH.captures(path) {
                    self.handle_push(req, route)
                } else {
                    Self::response_status(StatusCode::BAD_REQUEST)
                }
            }
            &Method::GET => {
                if let Some(route) = PATTERN_PULL.captures(path) {
                    self.handle_pull(req, route)
                } else {
                    Self::response_status(StatusCode::BAD_REQUEST)
                }
            }
            _ => Self::response_status(StatusCode::BAD_REQUEST),
        }
    }
}

#[derive(Deserialize)]
struct TlsConfig {
    pkcs12_path: PathBuf,
}

#[derive(Deserialize)]
struct Config {
    listen_addr: std::net::SocketAddr,
    num_worker: usize,
    tls: Option<TlsConfig>,
}

#[derive(StructOpt, Debug)]
#[structopt(name = "Furakus")]
struct Argument {
    #[structopt(short = "c", long = "config", parse(from_os_str))]
    config_path: PathBuf,
}

fn start_server(config: Config) {
    let runner = tokio::runtime::Builder::new()
        .core_threads(config.num_worker)
        .build()
        .unwrap();
    let server = {
        let executor = runner.executor();
        let pool = Arc::new(Mutex::new(HashMap::new()));
        let adapter: Box<dyn StreamAdapter<InputStream = _> + Send> =
            if let Some(tls_config) = config.tls {
                Box::new(TlsStreamAdapter::new(&tls_config.pkcs12_path))
            } else {
                Box::new(DummyStreamAdapter::new())
            };
        let listener = tokio::net::TcpListener::bind(&config.listen_addr).unwrap();
        listener
            .incoming()
            .for_each(move |stream| {
                let pool = pool.clone();
                let fut = adapter
                    .accept(stream)
                    .and_then(move |stream| {
                        let service = FlowService::new(pool);
                        let http = hyper::server::conn::Http::new();
                        http.serve_connection(stream, service)
                            .map_err(|err| Box::new(err) as Box<StdError + Send>)
                    })
                    .map_err(|_| ());
                executor.spawn(fut);
                future::ok(())
            })
            .map_err(|_| ())
    };
    runner.block_on_all(server).unwrap();
}

fn main() {
    let config: Config = {
        let args = Argument::from_args();
        let mut config_file = std::fs::File::open(args.config_path).unwrap();
        let mut buf = Vec::new();
        config_file.read_to_end(&mut buf).unwrap();
        toml::from_slice(&buf).unwrap()
    };
    start_server(config);
}

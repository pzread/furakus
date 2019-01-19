use furakus::utils::*;
use futures::{future, prelude::*};
use native_tls;
use std::{error::Error as StdError, fs::File, io::Read};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_tls;

pub trait AdapterStream: AsyncRead + AsyncWrite {}

impl<T: AsyncRead + AsyncWrite> AdapterStream for T {}

type AdapterFuture =
    Box<Future<Item = Box<dyn AdapterStream + Send>, Error = Box<dyn StdError + Send>> + Send>;

pub trait StreamAdapter {
    fn accept<T>(&self, stream: T) -> AdapterFuture
    where
        T: AsyncRead + AsyncWrite + Send + 'static;
}

pub struct TlsStreamAdapter {
    acceptor: tokio_tls::TlsAcceptor,
}

impl TlsStreamAdapter {
    pub fn new(pfx_path: &str) -> TlsStreamAdapter {
        let mut pfx_file = File::open(pfx_path).unwrap();
        let mut buf = Vec::new();
        pfx_file.read_to_end(&mut buf).unwrap();
        let identity = native_tls::Identity::from_pkcs12(&buf, "").unwrap();
        TlsStreamAdapter {
            acceptor: native_tls::TlsAcceptor::new(identity).unwrap().into(),
        }
    }
}

impl StreamAdapter for TlsStreamAdapter {
    fn accept<T>(&self, stream: T) -> AdapterFuture
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
    {
        self.acceptor
            .accept(stream)
            .map(|stream| Box::new(stream) as Box<dyn AdapterStream + Send>)
            .map_err(|err| Box::new(err) as Box<dyn StdError + Send>)
            .into_box()
    }
}

pub struct DummyStreamAdapter;

impl StreamAdapter for DummyStreamAdapter {
    fn accept<T>(&self, stream: T) -> AdapterFuture
    where
        T: AsyncRead + AsyncWrite + Send + 'static,
    {
        future::ok(Box::new(stream) as Box<AdapterStream + Send>).into_box()
    }
}

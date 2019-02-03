use furakus::utils::*;
use futures::{future, prelude::*};
use native_tls;
use std::{error::Error as StdError, fs::File, io::Read};
use tokio::io::{AsyncRead, AsyncWrite};
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
    pub fn new<P: AsRef<std::path::Path>>(pfx_path: P) -> TlsStreamAdapter {
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

#[cfg(test)]
mod tests {
    extern crate intra_pipe;

    use self::intra_pipe::{AsyncChannel, SyncChannel};
    use super::*;
    use native_tls::{Certificate, TlsConnector};
    use std::{
        io::{Cursor, Read, Write},
        thread,
    };
    use tokio::io::*;
    use tokio::runtime::Runtime;

    const TEST_INIT_DATA: &[u8] = b"Hello World";
    const TEST_WRITE_DATA: &[u8] = b"Ello";
    const TEST_EXPECT_DATA: &[u8] = b"o World";
    const TEST_ROOTCA_PATH: &str = "./tests/cert.der";
    const TEST_PKCS12_PATH: &str = "./tests/cert.p12";
    const TEST_DOMAIN: &str = "example.com";

    #[test]
    fn tls_stream_adapter() {
        let (server, client): (AsyncChannel, SyncChannel) = intra_pipe::channel();

        let thd = thread::spawn(move || {
            let rootca = {
                let mut cert_file = File::open(TEST_ROOTCA_PATH).unwrap();
                let mut buf = Vec::new();
                cert_file.read_to_end(&mut buf).unwrap();
                Certificate::from_der(&buf).unwrap()
            };
            let mut builder = TlsConnector::builder();
            let connector = builder.add_root_certificate(rootca).build().unwrap();
            let mut stream = connector.connect(TEST_DOMAIN, client).unwrap();
            stream.write_all(TEST_EXPECT_DATA).unwrap();
        });

        let adapter = TlsStreamAdapter::new(TEST_PKCS12_PATH);
        let fut = future::lazy(move || {
            adapter.accept(server).and_then(|stream| {
                read_exact(stream, vec![0u8; TEST_EXPECT_DATA.len()])
                    .map_err(|err| Box::new(err) as Box<dyn StdError + Send>)
            })
        });

        let mut runner = Runtime::new().unwrap();
        let (_, buf) = runner.block_on(fut).unwrap();
        assert_eq!(buf, TEST_EXPECT_DATA);
        thd.join().unwrap();
    }

    #[test]
    fn dummy_stream_adapter() {
        let source = Cursor::new(TEST_INIT_DATA.to_vec());
        let adapter = DummyStreamAdapter;
        let fut = adapter.accept(source);
        let mut runner = Runtime::new().unwrap();
        let stream = runner.block_on(fut).unwrap();

        let fut = write_all(stream, TEST_WRITE_DATA);
        let (stream, _) = runner.block_on(fut).unwrap();

        let fut = read_exact(stream, vec![0u8; TEST_EXPECT_DATA.len()]);
        let (_, res) = runner.block_on(fut).unwrap();
        assert_eq!(res.as_slice(), TEST_EXPECT_DATA);
    }
}

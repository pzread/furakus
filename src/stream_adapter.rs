use furakus::utils::*;
use futures::{future, prelude::*};
use native_tls;
use std::{fs::File, io::Read, marker::PhantomData};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tls;

pub trait AdapterStream: AsyncRead + AsyncWrite {}

impl<T: AsyncRead + AsyncWrite> AdapterStream for T {}

type BoxedStdError = Box<dyn std::error::Error + Send>;

type AdapterFuture =
    Box<Future<Item = Box<dyn AdapterStream + Send>, Error = BoxedStdError> + Send>;

pub trait StreamAdapter<T: AsyncRead + AsyncWrite + Send + 'static> {
    fn accept(&self, stream: T) -> AdapterFuture;
}

pub struct TlsStreamAdapter<T: AsyncRead + AsyncWrite + Send + 'static> {
    acceptor: tokio_tls::TlsAcceptor,
    phantom: PhantomData<T>,
}

impl<T: AsyncRead + AsyncWrite + Send + 'static> TlsStreamAdapter<T> {
    pub fn new<P: AsRef<std::path::Path>>(pfx_path: P) -> TlsStreamAdapter<T> {
        let mut pfx_file = File::open(pfx_path).unwrap();
        let mut buf = Vec::new();
        pfx_file.read_to_end(&mut buf).unwrap();
        let identity = native_tls::Identity::from_pkcs12(&buf, "").unwrap();
        let acceptor = native_tls::TlsAcceptor::builder(identity)
            .min_protocol_version(Some(native_tls::Protocol::Tlsv12))
            .build()
            .unwrap();
        TlsStreamAdapter {
            acceptor: acceptor.into(),
            phantom: PhantomData,
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Send + 'static> StreamAdapter<T> for TlsStreamAdapter<T> {
    fn accept(&self, stream: T) -> AdapterFuture {
        self.acceptor
            .accept(stream)
            .map(|stream| Box::new(stream) as Box<dyn AdapterStream + Send>)
            .map_err(|err| Box::new(err) as BoxedStdError)
            .into_box()
    }
}

pub struct DummyStreamAdapter<T: AsyncRead + AsyncWrite + Send + 'static> {
    phantom: PhantomData<T>,
}

impl<T: AsyncRead + AsyncWrite + Send + 'static> DummyStreamAdapter<T> {
    pub fn new() -> DummyStreamAdapter<T> {
        DummyStreamAdapter {
            phantom: PhantomData,
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Send + 'static> StreamAdapter<T> for DummyStreamAdapter<T> {
    fn accept(&self, stream: T) -> AdapterFuture {
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
    const TEST_ROOTCA_PATH: &str = "./tests/ca.der";
    const TEST_PKCS12_PATH: &str = "./tests/test.p12";
    const TEST_DOMAIN: &str = "example.com";

    #[test]
    fn tls_stream_adapter() {
        let (server, client): (AsyncChannel, SyncChannel) = intra_pipe::channel();

        let thd = thread::spawn(move || {
            let rootca = {
                let mut buf = Vec::new();
                File::open(TEST_ROOTCA_PATH)
                    .unwrap()
                    .read_to_end(&mut buf)
                    .unwrap();
                Certificate::from_der(&buf).unwrap()
            };
            let connector = TlsConnector::builder()
                .add_root_certificate(rootca)
                .build()
                .unwrap();
            let mut stream = connector.connect(TEST_DOMAIN, client).unwrap();
            stream.write_all(TEST_EXPECT_DATA).unwrap();
        });

        let adapter = TlsStreamAdapter::new(TEST_PKCS12_PATH);
        let fut = future::lazy(move || {
            adapter.accept(server).and_then(|stream| {
                read_exact(stream, vec![0u8; TEST_EXPECT_DATA.len()])
                    .map_err(|err| Box::new(err) as BoxedStdError)
            })
        });

        let mut runner = Runtime::new().unwrap();
        let (_, buf) = runner.block_on(fut).unwrap();
        assert_eq!(buf, TEST_EXPECT_DATA);
        thd.join().unwrap();
    }

    #[test]
    fn insecure_tls_connection() {
        let (server, client): (AsyncChannel, SyncChannel) = intra_pipe::channel();

        let thd = thread::spawn(move || {
            let connector = TlsConnector::builder()
                .max_protocol_version(Some(native_tls::Protocol::Tlsv10))
                .build()
                .unwrap();
            assert!(connector.connect(TEST_DOMAIN, client).is_err());
        });

        let adapter = TlsStreamAdapter::new(TEST_PKCS12_PATH);
        let fut = future::lazy(move || adapter.accept(server));
        let mut runner = Runtime::new().unwrap();
        assert!(runner.block_on(fut).is_err());
        thd.join().unwrap();
    }

    #[test]
    fn dummy_stream_adapter() {
        let source = Cursor::new(TEST_INIT_DATA.to_vec());
        let adapter = DummyStreamAdapter::new();
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

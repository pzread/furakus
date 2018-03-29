use native_tls::{Pkcs12, Protocol, TlsAcceptor};

pub fn build_acceptor_from_pfx(buf: &[u8]) -> TlsAcceptor {
    let pkcs12 = Pkcs12::from_der(&buf, "").unwrap();
    let mut builder = TlsAcceptor::builder(pkcs12).unwrap();
    builder.supported_protocols(&[Protocol::Tlsv12]).unwrap();
    builder.build().unwrap()
}

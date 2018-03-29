use native_tls::{Pkcs12, Protocol, TlsAcceptor};
use std::fs::File;
use std::io::Read;

pub fn build_tls_from_pfx(pfx_path: &str) -> TlsAcceptor {
    let mut pfx_file = File::open(pfx_path).unwrap();
    let mut buf = Vec::new();
    pfx_file.read_to_end(&mut buf).unwrap();
    let pkcs12 = Pkcs12::from_der(&buf, "").unwrap();
    let mut builder = TlsAcceptor::builder(pkcs12).unwrap();
    builder.supported_protocols(&[Protocol::Tlsv12]).unwrap();
    builder.build().unwrap()
}

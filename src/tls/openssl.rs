extern crate openssl;

use self::openssl::{
    pkcs12::Pkcs12,
    pkey::PKey,
    ssl::{SslAcceptorBuilder, SslMethod},
    x509::X509,
};
use native_tls::{backend::openssl::TlsAcceptorBuilderExt, TlsAcceptor, TlsAcceptorBuilder};
use std::{fs::File, io::Read};

fn build_acceptor(privkey: &PKey, cert: &X509, chain: &[X509]) -> TlsAcceptor {
    let builder =
        SslAcceptorBuilder::mozilla_modern(SslMethod::tls(), privkey, cert, chain).unwrap();
    TlsAcceptorBuilder::from_openssl(builder).build().unwrap()
}

pub fn build_acceptor_from_pfx(buf: &[u8]) -> TlsAcceptor {
    let depkcs12 = Pkcs12::from_der(buf).unwrap().parse("").unwrap();
    let chain = depkcs12.chain.into_iter().collect::<Vec<_>>();
    build_acceptor(&depkcs12.pkey, &depkcs12.cert, &chain)
}

#[allow(dead_code)]
pub fn build_tls_from_pem(cert_path: &str, priv_path: &str) -> TlsAcceptor {
    let fullchain = {
        let mut cert_file = File::open(cert_path).unwrap();
        let mut buf = Vec::new();
        cert_file.read_to_end(&mut buf).unwrap();
        X509::stack_from_pem(&buf).unwrap()
    };
    let privkey = {
        let mut priv_file = File::open(priv_path).unwrap();
        let mut buf = Vec::new();
        priv_file.read_to_end(&mut buf).unwrap();
        PKey::private_key_from_pem(&buf).unwrap()
    };
    let cert = fullchain.first().unwrap();
    build_acceptor(&privkey, cert, &fullchain[1..])
}

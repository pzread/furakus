extern crate openssl;
use native_tls::{TlsAcceptor, TlsAcceptorBuilder};
use native_tls::backend::openssl::TlsAcceptorBuilderExt;
use openssl::pkey::PKey;
use openssl::ssl::{SslAcceptorBuilder, SslMethod};
use openssl::x509::X509;

pub fn config_tls(cert_path: &str, priv_path: &str) -> TlsAcceptor {
    let fullchain = {
        let cert_file = File::open(cert_path).unwrap();
        let mut buf = Vec::new();
        BufReader::new(cert_file).read_to_end(&mut buf).unwrap();
        X509::stack_from_pem(&buf).unwrap()
    };
    let privkey = {
        let priv_file = File::open(priv_path).unwrap();
        let mut buf = Vec::new();
        BufReader::new(priv_file).read_to_end(&mut buf).unwrap();
        PKey::private_key_from_pem(&buf).unwrap()
    };
    let cert = fullchain.first().unwrap();
    let builder =
        SslAcceptorBuilder::mozilla_modern(SslMethod::tls(), &privkey, &cert, &fullchain[1..])
            .unwrap();
    TlsAcceptorBuilder::from_openssl(builder).build().unwrap()
}

#[cfg(target_os = "windows")]
#[path = "schannel.rs"]
mod imp;
#[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "ios")))]
#[path = "openssl.rs"]
mod imp;
#[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "ios")))]
pub use self::imp::build_tls_from_pem;
use native_tls::TlsAcceptor;
use std::{fs::File, io::Read};

#[allow(dead_code)]
pub fn build_tls_from_pfx(pfx_path: &str) -> TlsAcceptor {
    let mut pfx_file = File::open(pfx_path).unwrap();
    let mut buf = Vec::new();
    pfx_file.read_to_end(&mut buf).unwrap();
    imp::build_acceptor_from_pfx(&buf)
}

use crypto::digest::Digest;
use crypto;
use rand::{Rng, OsRng};
use rustc_serialize::hex::*;
use std::{fmt, ops};

/// Some default constants.
pub const SHORT_TIMEOUT: usize = 30;
pub const LONG_TIMEOUT: usize = 86400;
pub const MAX_CHUNKSIZE: usize = 4 * 1024 * 1024;

/// The `Hash` struct is the 256 bits SHA-3 hash of the string.
///
/// It implemented the `fmt::LowerHex` trait. This is useful for building a redis key since the
/// lowercase hexstring is safe for concatting with any redis key.
///
/// It coerces to `&str`. It implemented equivalence relation.
#[derive(Debug, PartialEq)]
pub struct Hash (String);

impl Hash {
    pub fn get(data: &str) -> Hash {
        let mut hasher = crypto::sha3::Sha3::sha3_256();
        hasher.input_str(data);
        Hash(hasher.result_str().to_owned())
    }
}

impl ops::Deref for Hash {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::LowerHex for Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.0)
    }
}

#[test]
fn test_hash() {
    let hash = Hash::get("hello world!");
    assert_eq!(&*hash,
               "9c24b06143c07224c897bac972e6e92b46cf18063f1a469ebe2f7a0966306105");
    assert_eq!(format!("{:x}", &hash),
               "9c24b06143c07224c897bac972e6e92b46cf18063f1a469ebe2f7a0966306105");
}

/// Return a unique 256 bits identifier in hex and its `Hash`.
pub fn generate_identifier() -> (String, Hash) {
    let mut rng = OsRng::new().unwrap();
    let mut id = [0u8; 32];
    rng.fill_bytes(&mut id);
    let id_string = id.to_hex();
    let id_hash = Hash::get(&id_string);
    (id_string, id_hash)
}

#[test]
fn test_generate_identifier() {
    let (id, id_hash) = generate_identifier();
    assert_eq!(Hash::get(&id), id_hash);
}

use ring::{digest, hmac, rand};
use ring::rand::SecureRandom;
use utils;

pub trait Authorizer: Send + Sync + 'static {
    fn sign(&self, flow_id: &str) -> String;
    fn verify(&self, flow_id: &str, token: &str) -> Result<(), ()>;
}

pub struct HMACAuthorizer {
    signkey: hmac::SigningKey,
}

impl HMACAuthorizer {
    pub fn new() -> Self {
        let rng = rand::SystemRandom::new();
        let mut seckey = vec![0u8; hmac::recommended_key_len(&digest::SHA256)];
        rng.fill(&mut seckey).unwrap();
        HMACAuthorizer { signkey: hmac::SigningKey::new(&digest::SHA256, &seckey) }
    }
}

impl Authorizer for HMACAuthorizer {
    fn sign(&self, flow_id: &str) -> String {
        let signature = {
            hmac::sign(&self.signkey, &flow_id.as_bytes())
        };
        utils::hex(signature.as_ref())
    }

    fn verify(&self, flow_id: &str, token: &str) -> Result<(), ()> {
        utils::unhex(token).map_err(|_| ()).and_then(|sig| {
            hmac::verify_with_own_key(&self.signkey, flow_id.as_bytes(), &sig).map_err(|_| ())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac() {
        let auth = HMACAuthorizer::new();
        let flow_id = "bdc62e9323003d0f5cb44c8c745a0470";
        let fake_id = "bdc62e9323O03d0f5cbAAc8c745a047O";
        let mal_token = "jlc(84c84w47wq87a";
        let fake_token = "bdc62e9323003d0f5cb44c8c745a0470bdc62e9323003d0f5cb44c8c745a0470";
        let token = &auth.sign(flow_id);
        assert_eq!(auth.verify(flow_id, token), Ok(()));
        assert_eq!(auth.verify(fake_id, token), Err(()));
        assert_eq!(auth.verify(flow_id, mal_token), Err(()));
        assert_eq!(auth.verify(flow_id, fake_token), Err(()));
    }
}

use byteorder::{ByteOrder, LittleEndian};
use ring::{self, rand::SecureRandom};

pub struct Flow {
    id: u128,
    token: u128,
}

impl Flow {
    pub fn new() -> Flow {
        let rand = ring::rand::SystemRandom::new();
        let mut id_buf = [0u8; 16];
        rand.fill(&mut id_buf).unwrap();
        let mut token_buf = [0u8; 16];
        rand.fill(&mut token_buf).unwrap();
        Flow {
            id: LittleEndian::read_u128(&id_buf),
            token: LittleEndian::read_u128(&token_buf),
        }
    }

    pub fn get_id(&self) -> u128 {
        self.id
    }

    pub fn get_token(&self) -> u128 {
        self.token
    }
}

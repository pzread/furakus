use std::collections::HashMap;
use std::result::Result as StdResult;
use std::time::Instant;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub enum Error {
    TooLarge,
    NotFound,
    Other,
}

pub type Result<T> = StdResult<T, Error>;

const MAX_SIZE: usize = 65536;

struct Chunk {
    data: Vec<u8>,
}

pub struct Flow {
    pub id: String,
    active_stamp: Instant,
    next_index: u64,
    tail_index: u64,
    chunk_bucket: HashMap<u64, Chunk>,
}

impl Flow {
    pub fn new() -> Self {
        Flow {
            id: Uuid::new_v4().simple().to_string(),
            active_stamp: Instant::now(),
            next_index: 0,
            tail_index: 0,
            chunk_bucket: HashMap::new(),
        }
    }

    pub fn push(&mut self, data: &[u8]) -> Result<u64> {
        if data.len() > MAX_SIZE {
            return Err(Error::TooLarge);
        }

        let chunk_index = self.next_index;
        self.next_index += 1;

        self.chunk_bucket
            .insert(chunk_index, Chunk { data: data.to_vec() });

        Ok(chunk_index)
    }

    pub fn fetch(&mut self, index: u64) -> Result<&[u8]> {
        Err(Error::NotFound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_chunk() {
        let mut flow = Flow::new();

        assert_eq!(flow.push(&[1u8; 1234]), Ok(0));
        assert_eq!(flow.push(&[2u8; MAX_SIZE]), Ok(1));
        assert_eq!(flow.push(&[2u8; MAX_SIZE + 1]), Err(Error::TooLarge));
    }
}

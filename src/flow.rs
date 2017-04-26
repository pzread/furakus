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

pub const MAX_SIZE: usize = 65536;

struct Chunk {
    data: Vec<u8>,
}

pub struct Flow {
    pub id: String,
    pub size: Option<u64>,
    active_stamp: Instant,
    next_index: u64,
    tail_index: u64,
    chunk_bucket: HashMap<u64, Chunk>,
    stat_size: u64,
}

impl Flow {
    pub fn new(size: Option<u64>) -> Self {
        Flow {
            id: Uuid::new_v4().simple().to_string(),
            size: size,
            active_stamp: Instant::now(),
            next_index: 0,
            tail_index: 0,
            chunk_bucket: HashMap::new(),
            stat_size: 0,
        }
    }

    pub fn push(&mut self, data: &[u8]) -> Result<u64> {
        if data.len() > MAX_SIZE {
            return Err(Error::TooLarge);
        }
        if let Some(size) = self.size {
            if (data.len() as u64) + self.stat_size > size {
                return Err(Error::TooLarge);
            }
        }

        let chunk_index = self.next_index;
        self.next_index += 1;
        self.chunk_bucket
            .insert(chunk_index, Chunk { data: data.to_vec() });

        self.stat_size += data.len() as u64;
        Ok(chunk_index)
    }

    pub fn fetch(&self, chunk_index: u64) -> Result<&[u8]> {
        if let Some(chunk) = self.chunk_bucket.get(&chunk_index) {
            Ok(&chunk.data)
        } else {
            Err(Error::NotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_chunk() {
        let mut flow = Flow::new(None);

        assert_eq!(flow.push(&[1u8; 1234]), Ok(0));
        assert_eq!(flow.push(&[2u8; MAX_SIZE]), Ok(1));
        assert_eq!(flow.push(b"hello"), Ok(2));
        assert_eq!(flow.push(&[2u8; MAX_SIZE + 1]), Err(Error::TooLarge));

        assert_eq!(flow.fetch(2), Ok(b"hello" as &[u8]));
        assert_eq!(flow.fetch(100), Err(Error::NotFound));
    }

    #[test]
    fn flow_fixed_size() {
        let mut flow = Flow::new(Some(10));

        assert_eq!(flow.push(b"hello"), Ok(0));
        assert_eq!(flow.push(b"world"), Ok(1));
        assert_eq!(flow.push(b"!"), Err(Error::TooLarge));
    }
}

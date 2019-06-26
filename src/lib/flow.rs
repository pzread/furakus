use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use futures::channel::oneshot;
use parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use ring::{self, rand::SecureRandom};
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

const EMPTY_BLOCK_INDEX: u64 = std::u64::MAX;
const RING_BUFFER_LENGTH: u64 = 16;

#[derive(Debug)]
pub enum Error {
    Occupied,
    Invalid,
    Dropped,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        "Flow Error"
    }
}

struct Block {
    index: u64,
    chunks: VecDeque<Bytes>,
    waits: Vec<oneshot::Sender<()>>,
}

struct Buffer {
    ring_buffer: Vec<RwLock<Block>>,
    next_index: AtomicU64,
    push_state: AtomicBool,
}

impl Buffer {
    fn new() -> Self {
        let mut ring_buffer = Vec::new();
        for _ in 0..RING_BUFFER_LENGTH {
            ring_buffer.push(RwLock::new(Block {
                index: EMPTY_BLOCK_INDEX,
                chunks: VecDeque::new(),
                waits: Vec::new(),
            }));
        }
        Buffer {
            ring_buffer,
            next_index: AtomicU64::new(0),
            push_state: AtomicBool::new(false),
        }
    }

    async fn fetch_write_block(target: &RwLock<Block>) -> RwLockWriteGuard<Block> {
        let rx = {
            let mut block = target.write();
            if block.index == EMPTY_BLOCK_INDEX {
                return block;
            }
            let (tx, rx) = oneshot::channel();
            block.waits.push(tx);
            rx
        };
        rx.await.unwrap();
        let block = target.write();
        assert_eq!(block.index, EMPTY_BLOCK_INDEX);
        block
    }

    async fn push(&self, chunks: VecDeque<Bytes>) -> Result<VecDeque<Bytes>, Error> {
        if self.push_state.swap(true, Ordering::SeqCst) {
            return Err(Error::Occupied);
        }
        let block_index = self.next_index.fetch_add(1, Ordering::SeqCst);
        let position = (block_index % RING_BUFFER_LENGTH) as usize;
        let mut block = Self::fetch_write_block(&self.ring_buffer[position]).await;
        block.index = block_index;
        let new_chunks = std::mem::replace::<VecDeque<Bytes>>(&mut block.chunks, chunks);
        while block.waits.len() > 0 {
            #[allow(unused_must_use)]
            {
                block.waits.pop().unwrap().send(());
            }
        }
        self.push_state.store(false, Ordering::SeqCst);
        Ok(new_chunks)
    }

    async fn fetch_read_block(
        target: &RwLock<Block>,
        block_index: u64,
    ) -> Option<RwLockUpgradableReadGuard<Block>> {
        loop {
            let rx = {
                let block = target.upgradable_read();
                if block.index == block_index {
                    return Some(block);
                } else if block.index > block_index && block.index != EMPTY_BLOCK_INDEX {
                    return None;
                }
                let (tx, rx) = oneshot::channel();
                let mut block = parking_lot::RwLockUpgradableReadGuard::upgrade(block);
                block.waits.push(tx);
                rx
            };
            rx.await.unwrap();
        }
    }

    async fn pull(&self, block_index: u64) -> Result<VecDeque<Bytes>, Error> {
        if block_index == EMPTY_BLOCK_INDEX {
            return Err(Error::Invalid);
        }
        let position = (block_index % RING_BUFFER_LENGTH) as usize;
        let block = match Self::fetch_read_block(&self.ring_buffer[position], block_index).await {
            Some(block) => block,
            None => return Err(Error::Dropped),
        };
        let chunks = block.chunks.clone();
        {
            let mut block = parking_lot::RwLockUpgradableReadGuard::upgrade(block);
            block.index = EMPTY_BLOCK_INDEX;
            block.chunks.clear();
            while block.waits.len() > 0 {
                #[allow(unused_must_use)]
                {
                    block.waits.pop().unwrap().send(());
                }
            }
        }
        Ok(chunks)
    }
}

pub struct Flow {
    id: u128,
    token: u128,
    buffer: Buffer,
}

impl Flow {
    pub fn new() -> Self {
        let rand = ring::rand::SystemRandom::new();
        let mut id_buf = [0u8; 16];
        rand.fill(&mut id_buf).unwrap();
        let mut token_buf = [0u8; 16];
        rand.fill(&mut token_buf).unwrap();
        Flow {
            id: LittleEndian::read_u128(&id_buf),
            token: LittleEndian::read_u128(&token_buf),
            buffer: Buffer::new(),
        }
    }

    pub fn get_id(&self) -> u128 {
        self.id
    }

    pub fn get_token(&self) -> u128 {
        self.token
    }

    pub async fn push(&self, chunks: VecDeque<Bytes>) -> Result<VecDeque<Bytes>, Error> {
        self.buffer.push(chunks).await
    }

    pub async fn pull(&self, block_index: u64) -> Result<VecDeque<Bytes>, Error> {
        self.buffer.pull(block_index).await
    }
}

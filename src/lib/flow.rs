use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use futures::channel::oneshot;
use parking_lot::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};
use ring::{self, rand::SecureRandom};
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering},
};

const EMPTY_BLOCK_INDEX: u64 = std::u64::MAX;
const RING_BUFFER_LENGTH: u64 = 16;

#[derive(Debug)]
pub enum Error {
    Closed,
    Dropped,
    Invalid,
    NotReady,
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

const BUFFER_STATE_IDLE: u8 = 0;
const BUFFER_STATE_PUSHING: u8 = 1;
const BUFFER_STATE_CLOSING: u8 = 2;

struct Buffer {
    ring_buffer: Vec<RwLock<Block>>,
    next_index: AtomicU64,
    length: AtomicUsize,
    state: AtomicU8,
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
            length: AtomicUsize::new(0),
            state: AtomicU8::new(BUFFER_STATE_IDLE),
        }
    }

    fn close(&self) {
        self.state.store(BUFFER_STATE_CLOSING, Ordering::SeqCst);
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
        if self
            .state
            .compare_and_swap(BUFFER_STATE_IDLE, BUFFER_STATE_PUSHING, Ordering::SeqCst)
            != BUFFER_STATE_IDLE
        {
            return Err(Error::Invalid);
        }
        let block_index = self.next_index.fetch_add(1, Ordering::SeqCst);
        let position = (block_index % RING_BUFFER_LENGTH) as usize;
        let mut block = Self::fetch_write_block(&self.ring_buffer[position]).await;
        block.index = block_index;
        let new_chunks = std::mem::replace::<VecDeque<Bytes>>(&mut block.chunks, chunks);
        self.length.fetch_add(1, Ordering::SeqCst);
        while block.waits.len() > 0 {
            #[allow(unused_must_use)]
            {
                block.waits.pop().unwrap().send(());
            }
        }
        self.state
            .compare_and_swap(BUFFER_STATE_PUSHING, BUFFER_STATE_IDLE, Ordering::SeqCst);
        Ok(new_chunks)
    }

    async fn fetch_read_block(
        target: &RwLock<Block>,
        block_index: u64,
        nonblocking: bool,
    ) -> Result<RwLockUpgradableReadGuard<Block>, Error> {
        loop {
            let rx = {
                let block = target.upgradable_read();
                if block.index == block_index {
                    return Ok(block);
                } else if block.index > block_index && block.index != EMPTY_BLOCK_INDEX {
                    return Err(Error::Dropped);
                } else if nonblocking {
                    return Err(Error::NotReady);
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
        let is_closing = self.state.load(Ordering::SeqCst) == BUFFER_STATE_CLOSING;
        let position = (block_index % RING_BUFFER_LENGTH) as usize;
        let block = match Self::fetch_read_block(
            &self.ring_buffer[position],
            block_index,
            is_closing,
        )
        .await
        {
            Ok(block) => block,
            Err(Error::NotReady) => return Err(Error::Closed),
            Err(err) => return Err(err),
        };
        let chunks = block.chunks.clone();
        {
            let mut block = parking_lot::RwLockUpgradableReadGuard::upgrade(block);
            block.index = EMPTY_BLOCK_INDEX;
            block.chunks.clear();
            self.length.fetch_sub(1, Ordering::SeqCst);
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

    pub fn close(&self) {
        self.buffer.close()
    }
}

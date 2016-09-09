extern crate crypto;
extern crate dotenv;
extern crate iron;
#[macro_use]
extern crate mime;
extern crate persistent;
extern crate r2d2;
extern crate r2d2_redis;
extern crate rand;
extern crate redis;
#[macro_use]
extern crate router;
extern crate rustc_serialize;

use crypto::digest::Digest;
use dotenv::dotenv;
use iron::prelude::*;
use iron::response::{ResponseBody, WriteBody};
use iron::{headers, status};
use r2d2_redis::RedisConnectionManager;
use rand::{Rng, OsRng};
use redis::{Commands, Connection as RedisConn, FromRedisValue, PipelineCommands, PubSub};
use router::*;
use rustc_serialize::hex::*;
use std::collections::{HashMap, VecDeque};
use std::io::{Error as IoError, ErrorKind as IoErrorKind, Read, Write};
use std::result::Result as StdResult;
use std::time::{Duration, Instant};
use std::{env, cmp, u64};

struct RedisPool;

impl iron::typemap::Key for RedisPool {
    type Value = r2d2::Pool<RedisConnectionManager>;
}

macro_rules! redis_conn {
    ($req:expr) => { &*$req.get::<persistent::Read<RedisPool>>().unwrap().get().unwrap() }
}

enum Error {
    Eof,
    Timeout,
    Again,
    Other,
}

type Result<T> = StdResult<T, Error>;

const MIN_SIZE: usize = 4096;
const MAX_SIZE: usize = 4 * 1024 * 1024;
const CHUNK_TIMEOUT: usize = 60;
const LONG_TIMEOUT: usize = 86400;
const STATE_EOF: i64 = -1;
const STATE_ERR: i64 = -2;

fn hash(data: &str) -> String {
    let mut hasher = crypto::sha3::Sha3::sha3_256();
    hasher.input_str(data);
    hasher.result_str().to_owned()
}

fn gen_channel() -> String {
    let mut rng = OsRng::new().unwrap();
    let mut id = [0u8; 32];
    rng.fill_bytes(&mut id);
    id.to_hex()
}

fn require_handler(req: &mut Request) -> IronResult<Response> {
    let rs = redis_conn!(req);
    let token_hash = hash(req.extensions.get::<Router>().unwrap().find("token").unwrap());
    let channel = gen_channel();
    let channel_hash = hash(&channel);
    let file_rskey = &format!("FILE@{}", token_hash);
    let require_rskey = &format!("REQUIRE@{}", channel_hash);

    if rs.hset_nx::<_, _, _, i64>(file_rskey, "channel", &channel_hash).unwrap() != 1 {
        return Ok(Response::with(status::Conflict));
    }

    redis::pipe()
        .hset(file_rskey, "consumer", 1)
        .hset(file_rskey, "offset", 0)
        .set(require_rskey, &token_hash)
        .expire(require_rskey, LONG_TIMEOUT)
        .expire(file_rskey, LONG_TIMEOUT)
        .execute(rs);

    Ok(Response::with((status::Ok, channel)))
}

fn balance_chunk_size(cur_size: usize, full_size: usize, len: usize, rate: u64) -> (usize, usize) {
    enum TransProfile { Balance, Under, Over }

    let profile =  match cur_size - len {
        0 if (rate / 2) > cur_size as u64  => TransProfile::Under,
        delta if delta > MIN_SIZE => TransProfile::Over,
        _ if rate < cur_size as u64 => TransProfile::Over,
        _ => TransProfile::Balance
    };

    match profile {
        TransProfile::Balance => (cur_size, full_size),
        TransProfile::Under => (cmp::min(MAX_SIZE, cur_size + full_size), cur_size),
        TransProfile::Over => {
            if cur_size == full_size {
                (full_size, MIN_SIZE)
            } else {
                ((cur_size + full_size) / 2, full_size)
            }
        },
    }
}

fn retrieve_token_hash(rs: &RedisConn, channel_hash: &str) -> Option<String> {
    let require_rskey = &format!("REQUIRE@{}", channel_hash);

    redis::pipe()
        .get(require_rskey)
        .del(require_rskey)
        .query::<Vec<redis::Value>>(rs)
        .ok()
        .and_then(|rets| {
            if i64::from_redis_value(&rets[1]).unwrap() == 1 {
                Some(String::from_redis_value(&rets[0]).unwrap())
            } else {
                None
            }
        })
}

fn read_from_request(req: &mut Request, buf: &mut [u8]) -> Result<usize> {
    match req.body.read(buf) {
        Ok(0) => Err(Error::Eof),
        Ok(len) => Ok(len),
        Err(_) => Err(Error::Other),
    }
}

fn wait_receiver_ack(rs: &RedisConn,
                     channel_hash: &str,
                     timeout: usize,
                     offset: u64,
                     consumer: u64) -> Result<()> {
    let send_rskey = format!("SEND@{}", channel_hash);
    let mut counter: u64 = consumer;

    while counter > 0 {
        let result = rs.brpop::<_, Vec<i64>>(&send_rskey, timeout).unwrap().first()
            .ok_or(Error::Timeout)
            .and_then(|ret| {
                let off: i64 = *ret;
                if off >= 0 {
                    if (off as u64) == offset {
                        counter -= 1;
                    }
                    Ok(())
                } else {
                    Err(Error::Other)
                }
            });

        if let Err(err) = result {
            return Err(err);
        }
    }
    Ok(())
}

fn push_handler(req: &mut Request) -> IronResult<Response> {
    let rs = redis_conn!(req);
    let channel_hash = hash(req.extensions.get::<Router>().unwrap().find("channel").unwrap());

    let token_hash = match retrieve_token_hash(&rs, &channel_hash) {
        Some(hash) => hash,
        None => return Ok(Response::with(status::NotFound)),
    };

    let total_size: u64 = match req.headers.get::<headers::ContentLength>() {
        Some(content_length) => content_length.0,
        None => 0,
    };

    let file_rskey = &format!("FILE@{}", token_hash);
    redis::pipe()
        // Extend file lifetime.
        .expire(file_rskey, LONG_TIMEOUT)
        // Set file size.
        .hset(file_rskey, "size", total_size)
        .execute(rs);
    let consumer: u64 = rs.hget(file_rskey, "consumer").unwrap();

    let recv_rskey = format!("RECV@{}", channel_hash);
    let mut rate_record = VecDeque::<(usize, Instant)>::new();
    let mut rate_accumlator = 0u64;
    let mut full_size = MIN_SIZE;
    let mut buf = Vec::<u8>::new();

    buf.resize(MIN_SIZE, 0);

    for index in 0.. {
        // Record start time.
        let start_instant = Instant::now();

        let result = read_from_request(req, &mut buf)
            .and_then(|read_len| {
                let timeout = match index { 0 => LONG_TIMEOUT, _ => CHUNK_TIMEOUT };
                wait_receiver_ack(rs, &channel_hash, timeout, index, consumer)
                    .map(|_| {
                        let chunk_rskey = &format!("CHUNK@{}@{}", channel_hash, index);
                        redis::pipe()
                            .set_ex(chunk_rskey, &buf[0..read_len], CHUNK_TIMEOUT)
                            .hincr(file_rskey, "offset", 1)
                            // Ack receivers.
                            .cmd("PUBLISH").arg(&recv_rskey).arg(index)
                            .execute(rs);
                        if index >= 1 {
                            let old_chunk_rskey = &format!("CHUNK@{}@{}", channel_hash, index - 1);
                            rs.del::<_, i64>(old_chunk_rskey).unwrap();
                        }
                        read_len
                    })
            })
            .map(|read_len| {
                // Rate measurement.
                rate_record.push_back((read_len, start_instant));
                rate_accumlator += read_len as u64;

                let (prev_read_len, prev_instant) = rate_record.front().unwrap().clone();
                let trust_interval = Duration::from_secs(2);
                let rate = if rate_record.len() < 2 || prev_instant.elapsed() < trust_interval {
                    0
                } else {
                    let mean_rate = rate_accumlator / prev_instant.elapsed().as_secs();
                    if rate_record[1].1.elapsed() >= trust_interval {
                        rate_record.pop_back().unwrap();
                        rate_accumlator -= prev_read_len as u64;
                    }
                    mean_rate
                };

                let (next_size, next_full_size) = balance_chunk_size(buf.len(),
                                                                     full_size,
                                                                     read_len,
                                                                     rate);
                full_size = next_full_size;
                buf.resize(next_size, 0);
            });

        if let Err(err) = result {
            return match err {
                Error::Eof => {
                    // Notify receivers of EOF.
                    redis::cmd("PUBLISH").arg(&recv_rskey).arg(STATE_EOF).execute(rs);
                    Ok(Response::with((status::Ok, "ok")))
                },
                Error::Timeout => Ok(Response::with((status::Ok, "timeout"))),
                _ => {
                    // Notify receivers of error.
                    redis::cmd("PUBLISH").arg(&recv_rskey).arg(STATE_ERR).execute(rs);
                    Ok(Response::with(status::InternalServerError))
                }
            };
        }
    }
    unreachable!();
}

struct PullWriter {
    redis_pool: r2d2::Pool<RedisConnectionManager>,
    channel: String,
}

impl PullWriter {
    fn get_chunk(&self, rs: &RedisConn, subscriber: &PubSub, index: u64) -> Result<Vec<u8>> {
        let chunk_rskey = &format!("CHUNK@{}@{}", self.channel, index);
        loop {
            let result = rs.get::<_, Option<Vec<u8>>>(chunk_rskey)
                .or(Err(Error::Other))
                .and_then(|ret| {
                    if let Some(data) = ret {
                        Ok(data)
                    } else {
                        subscriber.get_message()
                            .or(Err(Error::Timeout))
                            .and_then(|msg| {
                                match msg.get_payload::<i64>().unwrap() {
                                    off if off >= 0 => Err(Error::Again),
                                    STATE_EOF => Err(Error::Eof),
                                    _ => Err(Error::Other)
                                }
                            })
                    }
                });
            match result {
                Err(Error::Again) => (),
                _ => return result
            }
        }
    }
}

impl WriteBody for PullWriter {
    fn write_body(&mut self, res: &mut ResponseBody) -> std::io::Result<()> {
        let rs = &*self.redis_pool.get().unwrap();
        let send_rskey = &format!("SEND@{}", self.channel);
        let recv_rskey = &format!("RECV@{}", self.channel);

        let redis_url: &str = &env::var("REDIS_URL").unwrap();
        let subrs = redis::Client::open(redis_url).unwrap();
        let mut subscriber = subrs.get_pubsub().unwrap();
        subscriber.subscribe(recv_rskey).unwrap();
        subscriber.set_read_timeout(Some(Duration::from_secs(CHUNK_TIMEOUT as u64))).unwrap();

        // Notify the sender.
        redis::pipe()
            .lpush(send_rskey, 0)
            .expire(send_rskey, LONG_TIMEOUT)
            .execute(rs);

        for index in 0.. {
            let result = self.get_chunk(&rs, &subscriber, index)
                .and_then(|mut buf| {
                    // Ack the sender.
                    redis::pipe()
                        .lpush(send_rskey, index + 1)
                        .expire(send_rskey, LONG_TIMEOUT)
                        .execute(rs);

                    res.write_all(&mut buf)
                        .and(Ok(()))
                        .or(Err(Error::Other))
                });

            if let Err(err) = result {
                return match err {
                    Error::Eof => Ok(()),
                    Error::Timeout => Err(IoError::new(IoErrorKind::TimedOut, "timeout")),
                    _ => {
                        // Notify the sender of error.
                        redis::pipe()
                            .lpush(send_rskey, STATE_ERR)
                            .expire(send_rskey, LONG_TIMEOUT)
                            .execute(rs);
                        Err(IoError::new(IoErrorKind::BrokenPipe, "error"))
                    }
                };
            }
        }
        unreachable!();
    }
}

fn retrieve_metadata(rs: &RedisConn, token_hash: &str) -> Option<HashMap<String, redis::Value>> {
    let file_rskey = &format!("FILE@{}", token_hash);
    let metadata: HashMap<String, redis::Value> = rs.hgetall(file_rskey).unwrap();
    if metadata.is_empty() {
        None
    } else {
        Some(metadata)
    }
}

fn pull_handler(req: &mut Request) -> IronResult<Response> {
    let rs = redis_conn!(req);
    let token_hash = hash(req.extensions.get::<Router>().unwrap().find("token").unwrap());

    let metadata = match retrieve_metadata(&rs, &token_hash) {
        Some(metadata) => metadata,
        None => return Ok(Response::with(status::NotFound)),
    };
    if u64::from_redis_value(metadata.get("consumer").unwrap()).unwrap() == 0 ||
        u64::from_redis_value(metadata.get("offset").unwrap()).unwrap() > 0 {
        return Ok(Response::with(status::NotFound))
    }

    let channel_hash = String::from_redis_value(metadata.get("channel").unwrap()).unwrap();
    let total_size = u64::from_redis_value(metadata.get("size").unwrap()).unwrap();

    let writer: Box<WriteBody> = Box::new(PullWriter {
        redis_pool: (&*req.get::<persistent::Read<RedisPool>>().unwrap()).clone(),
        channel: channel_hash,
    });
    let mut resp = Response::with((status::Ok, mime!(Application/OctetStream), writer));
    if total_size > 0 {
        resp.headers.set(headers::ContentLength(total_size));
    }
    Ok(resp)
}

fn main() {
    dotenv().ok();

    let host: &str = &env::var("SERVER").expect("SERVER must be set");

    let redis_pool = {
        let redis_url: &str = &env::var("REDIS_URL").expect("REDIS_URL must be set");
        let manager = RedisConnectionManager::new(redis_url).unwrap();
        r2d2::Pool::new(Default::default(), manager).expect("Redis connection error")
    };

    let router = router!(require: post "/require/:token" => require_handler,
                         push: post "/push/:channel" => push_handler,
                         pull: get "/pull/:token" => pull_handler);

    let mut chain = Chain::new(router);
    chain.link_before(persistent::Read::<RedisPool>::one(redis_pool));

    Iron::new(chain).listen_with(host, 64, iron::Protocol::Http, Some(iron::Timeouts{
        keep_alive: None,
        read: Some(Duration::from_secs(CHUNK_TIMEOUT as u64)),
        write: Some(Duration::from_secs(CHUNK_TIMEOUT as u64)),
    })).expect("Server failed to start");
}

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
use redis::{Commands, FromRedisValue};
use router::*;
use rustc_serialize::hex::*;
use std::collections::HashMap;
use std::io::{Error as IoError, ErrorKind as IoErrorKind, Read, Write};
use std::result::Result as StdResult;
use std::time::{Duration, SystemTime};
use std::{env, cmp, u64};

struct RedisPool;

impl iron::typemap::Key for RedisPool {
    type Value = r2d2::Pool<RedisConnectionManager>;
}

type RedisConn = r2d2::PooledConnection<RedisConnectionManager>;

macro_rules! redis_conn {
    ($req:expr) => { $req.get::<persistent::Read<RedisPool>>().unwrap().get().unwrap() }
}

enum Error {
    Eof,
    Timeout,
    Other,
}

type Result<T> = StdResult<T, Error>;

const ALIGN_SIZE: usize = 16384;
const MAX_SIZE: usize = 4 * 1024 * 1024;
const CHUNK_TIMEOUT: usize = 30;
const LONG_TIMEOUT: usize = 86400;
const STATE_CONTINUE: u64 = 0;
const STATE_EOF: u64 = 1;
const STATE_ERR: u64 = 2;

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
    if rs.hset_nx::<_, _, _, u64>(file_rskey, "channel", &channel_hash).unwrap() != 1 {
        return Ok(Response::with(status::Conflict));
    }

    let require_rskey = &format!("REQUIRE@{}", channel_hash);
    rs.set::<_, _, String>(require_rskey, &token_hash).unwrap();
    rs.expire::<_, u64>(require_rskey, LONG_TIMEOUT).unwrap();
    rs.expire::<_, u64>(file_rskey, LONG_TIMEOUT).unwrap();

    Ok(Response::with((status::Ok, channel)))
}

fn balance_chunk_size(cur_size: usize,
                      full_size: usize,
                      len: usize,
                      duration: &Duration) -> (usize, usize) {
    enum TransPerf { Balance, Over, Under }

    let perf = if duration > &Duration::from_secs(2) {
        TransPerf::Over
    } else {
        match cur_size - len {
            0 => TransPerf::Under,
            delta if delta >= ALIGN_SIZE => TransPerf::Over,
            _ => TransPerf::Balance
        }
    };

    let align_level = (len + ALIGN_SIZE - 1) / ALIGN_SIZE;
    let cur_level = (cur_size + ALIGN_SIZE - 1) / ALIGN_SIZE;
    let full_level = (full_size + ALIGN_SIZE - 1) / ALIGN_SIZE;

    let (next_level, next_full_level) = match perf {
        TransPerf::Balance => (cur_level, full_level),
        TransPerf::Over => {
            if align_level < full_level {
                (full_level, full_level / 2)
            } else {
                ((cur_level + full_level) / 2, full_level)
            }
        },
        TransPerf::Under => (cur_level * 2, cur_level),
    };

    let next_balance = (next_level * ALIGN_SIZE, next_full_level * ALIGN_SIZE);
    cmp::max((ALIGN_SIZE, ALIGN_SIZE), cmp::min((MAX_SIZE, MAX_SIZE), next_balance))
}

fn retrieve_token_hash(rs: &RedisConn, channel_hash: &str) -> Option<String> {
    let require_rskey = &format!("REQUIRE@{}", channel_hash);
    String::from_redis_value(&rs.get::<_, redis::Value>(require_rskey).unwrap())
        .ok()
        .and_then(|hash| {
            if rs.del::<_, i64>(require_rskey).unwrap() == 1 {
                Some(hash)
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

fn push_handler(req: &mut Request) -> IronResult<Response> {
    let rs = redis_conn!(req);
    let channel_hash = hash(req.extensions.get::<Router>().unwrap().find("channel").unwrap());

    let total_size: u64 = match req.headers.get::<headers::ContentLength>() {
        Some(content_length) => content_length.0,
        None => 0,
    };

    let token_hash = match retrieve_token_hash(&rs, &channel_hash) {
        Some(hash) => hash,
        None => return Ok(Response::with(status::NotFound)),
    };

    rs.hset::<_, _, _, u64>(&format!("FILE@{}", token_hash), "size", total_size).unwrap();

    let send_rskey = format!("SEND@{}", channel_hash);
    let recv_rskey = format!("RECV@{}", channel_hash);
    let mut buf = Vec::<u8>::new();
    let mut full_size = ALIGN_SIZE;
    buf.resize(ALIGN_SIZE, 0);

    for index in 0.. {
        // Start time measurement.
        let stopwatch = SystemTime::now();

        let result = read_from_request(req, &mut buf)
            .and_then(|read_len| {
                let timeout = match index { 0 => LONG_TIMEOUT, _ => CHUNK_TIMEOUT };
                rs.brpop::<_, Vec<u64>>(&send_rskey, timeout).unwrap().first()
                    .ok_or(Error::Timeout)
                    .and_then(|state| {
                        if *state == STATE_CONTINUE {
                            Ok(())
                        } else {
                            println!("stop");
                            Err(Error::Other)
                        }
                    })
                    .map(|_| {
                        let chunk_rskey = &format!("CHUNK@{}@{}", channel_hash, index);
                        rs.set_ex::<_, _, String>(chunk_rskey,
                                                  &buf[0..read_len],
                                                  CHUNK_TIMEOUT).unwrap();
                        // Ack the receiver;
                        rs.lpush::<_, u64, u64>(&recv_rskey, STATE_CONTINUE).unwrap();
                        rs.expire::<_, u64>(&recv_rskey, LONG_TIMEOUT).unwrap();

                        read_len
                    })
            })
            .map(|read_len| {
                // End time measurement.
                let duration = stopwatch.elapsed().unwrap();

                let (next_size, next_full_size) = balance_chunk_size(buf.len(),
                                                                     full_size,
                                                                     read_len,
                                                                     &duration);
                full_size = next_full_size;
                buf.resize(next_size, 0);
            });

        if let Err(err) = result {
            match err {
                Error::Eof => println!("eof"),
                Error::Timeout => println!("timeout"),
                Error::Other => println!("other"),
            }
            return match err {
                Error::Eof => {
                    // Notify the receiver of EOF.
                    rs.lpush::<_, u64, u64>(&recv_rskey, STATE_EOF).unwrap();
                    rs.expire::<_, u64>(&recv_rskey, LONG_TIMEOUT).unwrap();
                    Ok(Response::with((status::Ok, "ok")))
                },
                Error::Timeout => Ok(Response::with(status::InternalServerError)),
                Error::Other => {
                    // Notify the receiver of error.
                    rs.lpush::<_, u64, u64>(&recv_rskey, STATE_ERR).unwrap();
                    rs.expire::<_, u64>(&recv_rskey, LONG_TIMEOUT).unwrap();
                    Ok(Response::with(status::InternalServerError))
                },
            };
        }
    }
    unreachable!();
}

struct PullWriter {
    redis: r2d2::Pool<RedisConnectionManager>,
    channel: String,
}

impl WriteBody for PullWriter {
    fn write_body(&mut self, res: &mut ResponseBody) -> std::io::Result<()> {
        let rs = self.redis.get().unwrap();
        let send_rskey = &format!("SEND@{}", self.channel);
        let recv_rskey = &format!("RECV@{}", self.channel);

        // Notify the sender.
        for _ in 0..3 {
            rs.lpush::<_, u64, u64>(send_rskey, STATE_CONTINUE).unwrap();
            rs.expire::<_, u64>(send_rskey, LONG_TIMEOUT).unwrap();
        }

        for index in 0.. {
            let result = rs.brpop::<_, Vec<u64>>(recv_rskey, CHUNK_TIMEOUT).unwrap().first()
                .ok_or(Error::Timeout)
                .and_then(|state| {
                    match *state {
                        STATE_CONTINUE => Ok(()),
                        STATE_EOF => Err(Error::Eof),
                        _ => Err(Error::Other)
                    }
                })
                .and_then(|_| {
                    let chunk_rskey = &format!("CHUNK@{}@{}", self.channel, index);
                    let mut buf: Vec<u8> = rs.get(chunk_rskey).unwrap();
                    rs.del::<_, u64>(chunk_rskey).unwrap();

                    res.write_all(&mut buf)
                        .and(Ok(()))
                        .or_else(|err| {println!("{:?}", err); Err(Error::Other)})
                })
                .map(|_| {
                    // Ack the sender;
                    rs.lpush::<_, _, u64>(send_rskey, STATE_CONTINUE).unwrap();
                    rs.expire::<_, u64>(send_rskey, LONG_TIMEOUT).unwrap();
                });

            if let Err(err) = result {
                match err {
                    Error::Eof => println!("reof"),
                    Error::Timeout => println!("rtimeout"),
                    Error::Other => println!("rother"),
                }

                return match err {
                    Error::Eof => Ok(()),
                    Error::Timeout => Err(IoError::new(IoErrorKind::TimedOut, "timeout")),
                    Error::Other => {
                        // Notify the sender of error.
                        rs.lpush::<_, _, u64>(send_rskey, STATE_ERR).unwrap();
                        rs.expire::<_, u64>(send_rskey, LONG_TIMEOUT).unwrap();
                        Err(IoError::new(IoErrorKind::BrokenPipe, "error"))
                    },
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
        if rs.del::<_, u64>(file_rskey).unwrap() == 0 {
            None
        } else {
            Some(metadata)
        }
    }
}

fn pull_handler(req: &mut Request) -> IronResult<Response> {
    let rs = redis_conn!(req);
    let token_hash = hash(req.extensions.get::<Router>().unwrap().find("token").unwrap());

    let metadata = match retrieve_metadata(&rs, &token_hash) {
        Some(metadata) => metadata,
        None => return Ok(Response::with(status::NotFound)),
    };

    let channel_hash = String::from_redis_value(metadata.get("channel").unwrap()).unwrap();
    let total_size = u64::from_redis_value(metadata.get("size").unwrap()).unwrap();

    let writer: Box<WriteBody> = Box::new(PullWriter {
        redis: (&*req.get::<persistent::Read<RedisPool>>().unwrap()).clone(),
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

extern crate core;

use std::cell::UnsafeCell;

#[cfg(feature = "cache")]
pub mod cache;
#[cfg(feature = "checksum")]
pub mod checksum;
#[cfg(feature = "concurrency")]
pub mod concurrency;
#[cfg(feature = "crypto")]
pub mod crypto;
#[cfg(feature = "dbx")]
pub mod dbx;
#[cfg(feature = "elasticx")]
pub mod elasticx;
pub mod error;
#[cfg(feature = "fsx")]
pub mod fsx;
#[cfg(feature = "kafkax")]
pub mod kafkax;
pub mod lang;
#[cfg(feature = "logx")]
pub mod logx;
#[cfg(feature = "pgx")]
pub mod pgx;
#[cfg(feature = "redisx")]
pub mod redisx;
#[cfg(feature = "rpcx")]
pub mod rpcx;
pub mod socketx;
pub mod times;
pub mod utils;

#[cfg(feature = "iox")]
pub mod iox;
mod tasks;

#[derive(serde::Deserialize, Clone, PartialEq, Eq, Debug, Default)]
pub enum Env {
    #[default]
    #[serde(rename(deserialize = "dev"))]
    Dev,
    #[serde(rename(deserialize = "test"))]
    Test,
    #[serde(rename(deserialize = "prod"))]
    Prod,
}

impl Env {
    pub fn from_str(val: &str) -> Option<Env> {
        match val {
            "dev" => Some(Env::Dev),
            "test" => Some(Env::Test),
            "prod" => Some(Env::Prod),
            _ => None,
        }
    }
}

pub const LOG_TRACE_ID: &str = "X-Trace-Id";
pub const LOG_SPAN_ID: &str = "X-Span-Id";
pub const GRPC_TRACE_ID: &str = "x-trace-id";
pub const SPAN_TRACE_ID: &str = "trace_id";
pub const SOCKET_BUF_SIZE: usize = 1 << 20;
pub const GRPC_SPAN_ID: &str = "x-span-id";

thread_local! {
    pub static LOCAL_IP_NUM: UnsafeCell<i32> = UnsafeCell::new(0);
    pub static LOCAL_IP: UnsafeCell<String> = UnsafeCell::new("".to_string());
}

pub const ALPHABET: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

pub static KAFKA_BROKERS_ENV: &str = "KAFKA_BROKERS";

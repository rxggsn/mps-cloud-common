use std::cell::UnsafeCell;

pub mod checksum;
pub mod concurrency;
pub mod crypto;
#[cfg(feature = "elasticx")]
pub mod elasticx;
pub mod error;
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

#[derive(serde::Deserialize, Clone, PartialEq, Eq, Debug)]
pub enum Env {
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
pub const GRPC_TRACE_ID: &str = "x-trace-id";
pub const SPAN_TRACE_ID: &str = "trace_id";
pub const SOCKET_BUF_SIZE: usize = 1 << 20;

thread_local! {
    pub static LOCAL_IP_NUM: UnsafeCell<i32> = UnsafeCell::new(0);
    pub static LOCAL_IP: UnsafeCell<String> = UnsafeCell::new("".to_string());
}

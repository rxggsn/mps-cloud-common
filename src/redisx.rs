use std::io::Error;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::{pin, Pin};
use std::task::{Context, Poll};
use std::{io, time::Duration};

use futures::executor::block_on;
use redis::aio::MultiplexedConnection;
use redis::{
    AsyncCommands, AsyncIter, ConnectionAddr, ConnectionInfo, FromRedisValue, IntoConnectionInfo,
    RedisError, ToRedisArgs,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{lookup_host, TcpStream};

#[derive(serde::Deserialize, Clone, Debug)]
pub struct RedisConf {
    pub host: String,
    pub tls: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub db: i64,
    pub connect_timeout: u64,
}

impl Default for RedisConf {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            tls: false,
            username: Default::default(),
            password: Default::default(),
            db: 0,
            connect_timeout: 3,
        }
    }
}

#[derive(Clone)]
pub struct Redis {
    inner: redis::aio::ConnectionManager,
}

impl Redis {
    pub async fn set<K: ToRedisArgs + Send + Sync, V: ToRedisArgs + Send + Sync>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<(), RedisError> {
        self.inner.set(key, value).await
    }

    pub async fn get<K: ToRedisArgs + Send + Sync, V: FromRedisValue + Send + Sync>(
        &mut self,
        key: K,
    ) -> Result<V, RedisError> {
        self.inner.get(key).await
    }

    pub async fn hset<
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync,
    >(
        &mut self,
        key: K,
        field: F,
        val: V,
    ) -> Result<(), RedisError> {
        self.inner.hset(key, field, val).await
    }

    pub async fn hget<
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + FromRedisValue + Send + Sync,
    >(
        &mut self,
        key: K,
        field: F,
    ) -> Result<V, RedisError> {
        self.inner.hget(key, field).await
    }

    pub async fn hscan<
        'a,
        K: ToRedisArgs + Send + Sync,
        F: FromRedisValue + Send + Sync + 'a,
        V: FromRedisValue + Send + Sync + 'a,
    >(
        &'a mut self,
        key: K,
    ) -> Result<AsyncIter<'a, (F, V)>, RedisError> {
        self.inner.hscan(key).await
    }

    pub async fn hdel<K: ToRedisArgs + Send + Sync, F: ToRedisArgs + Send + Sync>(
        &mut self,
        key: K,
        field: F,
    ) -> Result<(), RedisError> {
        self.inner.hdel(key, field).await
    }

    pub async fn del<K: ToRedisArgs + Send + Sync>(&mut self, key: K) -> Result<(), RedisError> {
        self.inner.del(key).await
    }

    pub async fn hvals<K: ToRedisArgs + Send + Sync, V: FromRedisValue + Send + Sync>(
        &mut self,
        key: K,
    ) -> Result<Vec<V>, RedisError> {
        self.inner.hvals(key).await
    }

    pub async fn hkeys<K: ToRedisArgs + Send + Sync, F: FromRedisValue + Send + Sync>(
        &mut self,
        key: K,
    ) -> Result<Vec<F>, RedisError> {
        self.inner.hkeys(key).await
    }

    pub async fn hset_multiple<
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync,
    >(
        &mut self,
        key: K,
        items: &[(F, V)],
    ) -> Result<(), RedisError> {
        self.inner.hset_multiple(key, items).await
    }

    pub async fn sadd<K: ToRedisArgs + Send + Sync, V: ToRedisArgs + Send + Sync>(
        &mut self,
        key: K,
        val: V,
    ) -> Result<(), RedisError> {
        self.inner.sadd(key, val).await
    }

    pub async fn smembers<K: ToRedisArgs + Send + Sync, V: FromRedisValue + Send + Sync>(
        &mut self,
        key: K,
    ) -> Result<Vec<V>, RedisError> {
        self.inner.smembers(key).await
    }

    pub async fn srem<K: ToRedisArgs + Send + Sync, V: ToRedisArgs + Send + Sync>(
        &mut self,
        key: K,
        val: V,
    ) -> Result<(), RedisError> {
        self.inner.srem(key, val).await
    }

    pub async fn hset_nx<
        K: ToRedisArgs + Send + Sync,
        F: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync,
    >(
        &mut self,
        key: K,
        field: F,
        val: V,
    ) -> Result<bool, RedisError> {
        self.inner.hset_nx(key, field, val).await
    }

    pub async fn expire<K: ToRedisArgs + Send + Sync>(
        &mut self,
        key: K,
        seconds: usize,
    ) -> Result<(), RedisError> {
        self.inner.expire(key, seconds as i64).await
    }

    pub async fn set_nx<K: ToRedisArgs + Send + Sync, V: ToRedisArgs + Send + Sync>(
        &mut self,
        key: K,
        val: V,
    ) -> Result<bool, RedisError> {
        self.inner.set_nx(key, val).await
    }

    pub async fn set_ex<K: ToRedisArgs + Send + Sync, V: ToRedisArgs + Send + Sync>(
        &mut self,
        key: K,
        val: V,
        seconds: usize,
    ) -> Result<(), RedisError> {
        self.inner.set_ex(key, val, seconds as u64).await
    }

    pub async fn incr<
        K: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + FromRedisValue + Send + Sync,
    >(
        &mut self,
        key: K,
        delta: V,
    ) -> Result<V, RedisError> {
        self.inner.incr(key, delta).await
    }

    pub async fn lrange<
        K: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + FromRedisValue + Send + Sync,
    >(
        &mut self,
        key: K,
        start: isize,
        end: isize,
    ) -> Result<Option<Vec<V>>, RedisError> {
        self.inner.lrange(key, start, end).await
    }

    pub async fn rpush<K: ToRedisArgs + Send + Sync, V: ToRedisArgs + Send + Sync>(
        &mut self,
        key: K,
        val: V,
    ) -> Result<usize, RedisError> {
        self.inner.rpush(key, val).await
    }

    pub async fn rpop_back<K: ToRedisArgs + Send + Sync, V: FromRedisValue + Send + Sync>(
        &mut self,
        key: K,
    ) -> Result<Option<V>, RedisError> {
        self.inner.rpop(key, None).await
    }

    pub async fn llen<K: ToRedisArgs + Send + Sync>(
        &mut self,
        key: K,
    ) -> Result<usize, RedisError> {
        self.inner.llen(key).await
    }
}

impl RedisConf {
    pub async fn create(&self) -> Redis {
        let split: Vec<_> = self.host.split(":").collect();
        if split.len() > 2 || split.len() == 0 {
            panic!("invalid redis host. the format should be [host:port]")
        }

        let host = split[0].to_string();
        let port = if split.len() == 2 {
            u16::from_str_radix(split[1], 10).expect("invalid redis port")
        } else {
            6379
        };
        let addr = if self.tls {
            ConnectionAddr::TcpTls {
                host: host.clone(),
                port,
                insecure: false,
                tls_params: None,
            }
        } else {
            ConnectionAddr::Tcp(host.clone(), port)
        };

        let info = ConnectionInfo {
            addr,
            redis: redis::RedisConnectionInfo {
                db: self.db,
                username: self.username.clone(),
                password: self.password.clone(),
                protocol: Default::default(),
            },
        };
        let conn = redis::Client::open(info)
            .expect("create redis client failed")
            .get_connection_manager()
            .await
            .expect("get redis multiplexed connection failed");
        Redis { inner: conn }
    }
}

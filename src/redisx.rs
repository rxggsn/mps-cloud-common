use std::sync::Arc;

use futures::TryFutureExt;
use redis::{
    AsyncCommands, AsyncIter, ConnectionAddr, ConnectionInfo, FromRedisValue, PushInfo, RedisError,
    ToRedisArgs, aio::ConnectionManagerConfig,
};
use tokio::sync::broadcast;

pub type Pipeline = redis::Pipeline;
pub type Error = RedisError;
pub type PushKind = redis::PushKind;

pub const PUSH_QUEUE_SIZE: usize = 1024;

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

pub struct PMessage {
    pub channel: String,
    pub value: String,
}

fn from_redis_value(val: &redis::Value) -> Result<String, RedisError> {
    redis::from_redis_value(val)
}

pub fn parse_pmessage(msg: &PushInfo) -> Result<PMessage, RedisError> {
    let data = msg
        .data
        .iter()
        .map(|data| from_redis_value(data))
        .collect::<Result<Vec<String>, RedisError>>()?;
    let channel_name = data[1].clone();
    let message = data[2].clone();
    Ok(PMessage {
        channel: channel_name,
        value: message,
    })
}

#[derive(Clone)]
pub struct Redis {
    inner: redis::aio::ConnectionManager,
    stream: Arc<broadcast::Receiver<PushInfo>>,
}

impl Redis {
    pub async fn new(url: &str) -> Self {
        let cli = redis::Client::open(url).expect("invalid redis url");
        let (tx, rx) = broadcast::channel::<PushInfo>(PUSH_QUEUE_SIZE);
        let inner = redis::aio::ConnectionManager::new_with_config(
            cli,
            ConnectionManagerConfig::new()
                .set_push_sender(tx)
                .set_automatic_resubscription(),
        )
        .await
        .expect("connect redis failed");
        Self {
            inner,
            stream: Arc::new(rx),
        }
    }

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

    pub async fn exists<K: ToRedisArgs + Send + Sync>(
        &mut self,
        key: K,
    ) -> Result<bool, RedisError> {
        self.inner.exists(key).await
    }

    pub async fn publish<K: ToRedisArgs + Send + Sync, V: ToRedisArgs + Send + Sync>(
        &mut self,
        channel: K,
        message: V,
    ) -> Result<i64, RedisError> {
        self.inner.publish(channel, message).await
    }

    pub async fn lpush<K: ToRedisArgs + Send + Sync, V: ToRedisArgs + Send + Sync>(
        &mut self,
        key: K,
        val: V,
    ) -> Result<usize, RedisError> {
        self.inner.lpush(key, val).await
    }

    pub fn pipeline() -> redis::Pipeline {
        redis::pipe()
    }

    pub async fn execute_pipeline<T: FromRedisValue + Send + Sync>(
        &mut self,
        pipe: &redis::Pipeline,
    ) -> Result<T, RedisError> {
        pipe.query_async(&mut self.inner).await
    }

    pub async fn psubscribe(
        &mut self,
        pattern: &str,
    ) -> Result<broadcast::Receiver<PushInfo>, RedisError> {
        self.inner.psubscribe(pattern).await?;
        Ok(self.stream.resubscribe())
    }

    pub async fn ltrim<K: ToRedisArgs + Send + Sync>(
        &mut self,
        key: K,
        start: isize,
        stop: isize,
    ) -> Result<(), RedisError> {
        self.inner.ltrim(key, start, stop).await
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
        let cli = redis::Client::open(info).expect("create redis client failed");
        let (tx, rx) = broadcast::channel::<PushInfo>(PUSH_QUEUE_SIZE);

        let conn = redis::aio::ConnectionManager::new_with_config(
            cli,
            ConnectionManagerConfig::new()
                .set_push_sender(tx)
                .set_automatic_resubscription(),
        )
        .await
        .expect("get redis multiplexed connection failed");
        Redis {
            inner: conn,
            stream: Arc::new(rx),
        }
    }
}

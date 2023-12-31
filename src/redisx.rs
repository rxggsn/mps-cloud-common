use std::{io, time::Duration};

use redis::{AsyncCommands, AsyncIter, FromRedisValue, RedisError, ToRedisArgs};

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
    inner: redis::aio::MultiplexedConnection,
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
            redis::ConnectionAddr::TcpTls {
                host: host.clone(),
                port,
                insecure: false,
            }
        } else {
            redis::ConnectionAddr::Tcp(host.clone(), port)
        };

        let info = redis::ConnectionInfo {
            addr,
            redis: redis::RedisConnectionInfo {
                db: self.db,
                username: self.username.clone(),
                password: self.password.clone(),
            },
        };
        let cli = redis::Client::open(info).expect("open redis client failed");
        match tokio::time::timeout(
            Duration::from_secs(self.connect_timeout),
            cli.get_multiplexed_tokio_connection(),
        )
        .await
        {
            Ok(r) => match r {
                Ok(inner) => Redis { inner },
                Err(err) => panic!("{}", err),
            },
            Err(_) => panic!(
                "{}",
                io::Error::new(io::ErrorKind::TimedOut, "connect redis timeout",)
            ),
        }
    }
}

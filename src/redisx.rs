use std::{io, time::Duration};
use std::io::Error;
use std::net::SocketAddr;
use std::pin::{pin, Pin};
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::TryStreamExt;
use redis::{
    AsyncCommands, AsyncIter, ConnectionAddr, ConnectionInfo, FromRedisValue, IntoConnectionInfo,
    RedisError, ToRedisArgs,
};
use redis::aio::MultiplexedConnection;
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
    inner: MultiplexedConnection,
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
        self.inner.expire(key, seconds).await
    }

    pub async fn set_nx<K: ToRedisArgs + Send + Sync, V: ToRedisArgs + Send + Sync>(
        &mut self,
        key: K,
        val: V,
    ) -> Result<bool, RedisError> {
        self.inner.set_nx(key, val).await
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
            },
        };
        let connection_info = info
            .into_connection_info()
            .expect("parse redis connection info failed");
        let stream = ReconnectTokioStream::connect(&connection_info)
            .await
            .expect("connect redis failed");
        match tokio::time::timeout(
            Duration::from_secs(self.connect_timeout),
            MultiplexedConnection::new(&connection_info.redis, stream),
        )
        .await
        {
            Ok(r) => match r {
                Ok((inner, driver)) => {
                    tokio::spawn(driver);
                    Redis { inner }
                }
                Err(err) => panic!("{}", err),
            },
            Err(_) => panic!(
                "{}",
                Error::new(io::ErrorKind::TimedOut, "connect redis timeout",)
            ),
        }
    }
}

struct ReconnectTokioStream {
    connection_addr: SocketAddr,
    stream: TcpStream,
}

impl AsyncRead for ReconnectTokioStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let stream = self.get_mut();
        let pinned = pin!(&mut stream.stream);
        match pinned.poll_read(cx, buf) {
            Poll::Ready(r) => match r {
                Ok(size) => Poll::Ready(Ok(size)),
                Err(err) => match err.kind() {
                    io::ErrorKind::BrokenPipe => {
                        if let Err(err) = block_on(stream.reconnect()) {
                            Poll::Ready(Err(err))
                        } else {
                            let pinned = pin!(&mut stream.stream);
                            pinned.poll_read(cx, buf)
                        }
                    }
                    _ => Poll::Ready(Err(err)),
                },
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for ReconnectTokioStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        let stream = self.get_mut();
        let pinned = pin!(&mut stream.stream);
        match pinned.poll_write(cx, buf) {
            Poll::Ready(r) => match r {
                Ok(size) => Poll::Ready(Ok(size)),
                Err(err) => match err.kind() {
                    io::ErrorKind::BrokenPipe => {
                        if let Err(err) = block_on(stream.reconnect()) {
                            Poll::Ready(Err(err))
                        } else {
                            let pinned: Pin<&mut _> = pin!(&mut stream.stream);
                            pinned.poll_write(cx, buf)
                        }
                    }
                    _ => Poll::Ready(Err(err)),
                },
            },
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let stream = self.get_mut();
        let pinned = pin!(&mut stream.stream);
        match pinned.poll_flush(cx) {
            Poll::Ready(r) => match r {
                Ok(_) => Poll::Ready(Ok(())),
                Err(err) => match err.kind() {
                    io::ErrorKind::BrokenPipe => {
                        if let Err(err) = block_on(stream.reconnect()) {
                            Poll::Ready(Err(err))
                        } else {
                            let pinned: Pin<&mut _> = pin!(&mut stream.stream);
                            pinned.poll_flush(cx)
                        }
                    }
                    _ => Poll::Ready(Err(err)),
                },
            },
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let stream = self.get_mut();
        let pinned = pin!(&mut stream.stream);
        pinned.poll_shutdown(cx)
    }
}

impl ReconnectTokioStream {
    async fn connect(addr: &ConnectionInfo) -> io::Result<Self> {
        match addr.addr {
            ConnectionAddr::Tcp(ref hostname, port) => {
                let socketaddr = get_socket_addrs(hostname.as_str(), port).await?;
                let stream = TcpStream::connect(&socketaddr).await?;
                Ok(Self {
                    stream,
                    connection_addr: socketaddr,
                })
            }
            _ => Err(Error::new(
                io::ErrorKind::Unsupported,
                "unsupported connection type",
            )),
        }
    }

    async fn reconnect(&mut self) -> io::Result<()> {
        self.stream = TcpStream::connect(&self.connection_addr).await?;
        Ok(())
    }
}

async fn get_socket_addrs(host: &str, port: u16) -> io::Result<SocketAddr> {
    let mut socket_addrs = lookup_host((host, port)).await?;
    match socket_addrs.next() {
        Some(socket_addr) => Ok(socket_addr),
        None => Err(Error::new(
            io::ErrorKind::AddrNotAvailable,
            "No address found for host",
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use redis::{ConnectionAddr, ConnectionInfo};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    use crate::redisx::{ReconnectTokioStream, RedisConf};

    const PORT: u16 = 6379;

    enum Command {
        ShutdownService,
        ShutdownAll,
        Restart,
    }
    struct ControlPingPang {
        cmd: mpsc::Receiver<Command>,
    }

    impl ControlPingPang {
        async fn start(mut self) {
            let mut buf = [0u8; 128];
            'outside: loop {
                let listener = TcpListener::bind(format!("localhost:{}", PORT))
                    .await
                    .expect("msg");
                let r = listener.accept().await;
                println!("reconnected");

                let (stream, _) = r.unwrap();
                let (mut reader, mut writer) = stream.into_split();
                'inside: loop {
                    tokio::select! {
                        cmd_result = self.cmd.recv() => {
                            match cmd_result {
                                 Some(Command::ShutdownService) => {
                                    break  'inside;
                                },
                                Some(Command::ShutdownAll) => {
                                    break 'outside;
                                },
                                Some(Command::Restart) => {
                                    break  'inside;
                                },
                                None=> {
                                    break 'outside;
                                }
                            }
                        },
                        message_result = reader.read(&mut buf) => {
                            match message_result {
                                Ok(size) => {
                                    if size == 0 {
                                        break 'inside;
                                    }
                                    let val = String::from_utf8(buf.to_vec()).expect("");
                                    writer.write_all(val.as_bytes()).await.unwrap();
                                },
                                Err(_) => {
                                    break 'inside;
                                }
                            }
                        }
                    }
                    buf.fill(0u8)
                }
                drop(reader);
                writer.forget();
            }
        }
    }

    #[tokio::test]
    async fn test_redis_operations() {
        let conf = RedisConf::default();
        let mut redis = conf.create().await;

        redis.set("key", "val").await.unwrap();
        let val: String = redis.get("key").await.unwrap();
        assert_eq!(val, "val");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[cfg_attr(target_os = "macos")]
    async fn test_redis_stream_reconnect() {
        let (tx, rx) = mpsc::channel(10);
        let control = ControlPingPang { cmd: rx };

        tokio::spawn(control.start());
        tokio::time::sleep(Duration::from_millis(100)).await;

        let connection_info = ConnectionInfo {
            addr: ConnectionAddr::Tcp("localhost".to_string(), PORT),
            redis: Default::default(),
        };

        let mut stream = ReconnectTokioStream::connect(&connection_info)
            .await
            .expect("msg");

        stream.write_all("ping".as_bytes()).await.unwrap();
        let mut buf = [0u8; 64];
        stream.read(&mut buf).await.unwrap();
        let val = String::from_utf8(buf.to_vec()).expect("");
        assert_eq!(val.replace("\0", ""), "ping");

        stream.reconnect().await.expect("msg");
        tokio::time::sleep(Duration::from_millis(100)).await;

        stream.write_all("ping".as_bytes()).await.unwrap();
        let mut buf = [0u8; 64];
        stream.read(&mut buf).await.unwrap();
        let val = String::from_utf8(buf.to_vec()).expect("");
        assert_eq!(val.replace("\0", ""), "ping");
    }
}

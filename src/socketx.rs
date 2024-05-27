use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tonic::async_trait;

use std::{fmt::Display, ops::ControlFlow};

use tokio::{
    io::{self, AsyncReadExt as _, AsyncWriteExt as _},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

use crate::utils;
use crate::LOG_TRACE_ID;

const TCP_BUF_SIZE: usize = 512;

#[async_trait]
pub trait SocketService {
    async fn handle(
        &mut self,
        mut conn: TcpStream,
        trace_id: String,
        span: tracing::Span,
    ) -> std::io::Result<()>;
}

#[derive(Clone, serde::Deserialize, Debug)]
pub struct ServerBuilder {
    pub port: i16,
}

pub struct Server<T: SocketService> {
    listener: TcpListener,
    svc: T,
    port: i16,
}

impl<T: SocketService> Server<T> {
    pub async fn bind(port: i16, svc: T) -> std::io::Result<Server<T>> {
        TcpListener::bind(format!("0.0.0.0:{}", port))
            .await
            .map(|listener| Self {
                listener,
                svc,
                port,
            })
    }

    pub async fn start(mut self) {
        let this = &mut self;
        tracing::info!("Socket Service start at port [{}]", this.port);
        loop {
            let result = this.listener.accept().await;
            let trace_id = utils::new_trace_id();
            let span = tracing::info_span!(LOG_TRACE_ID, trace_id = trace_id.as_str());
            match result {
                Ok((conn, _)) => match this.svc.handle(conn, trace_id, span).await {
                    Ok(_) => {}
                    Err(err) => tracing::error!("{}", err),
                },
                Err(err) => tracing::error!("{}", err),
            }
        }
    }
}

pub struct TcpPackage {
    pub(crate) _start: usize,
    pub end: usize,
    pub data: bytes::Bytes,
}

fn merge_package(buf: &mut bytes::BytesMut, read_buf: &mut bytes::BytesMut) {
    let need_capacity = buf.len() + read_buf.len();

    if buf.capacity() < need_capacity {
        buf.reserve(need_capacity - buf.capacity())
    }

    buf.extend_from_slice(&read_buf);
    read_buf.clear();
}

fn next_package(buf: &[u8], start_byte: u8, end_byte: u8) -> TcpPackage {
    let mut is_start = false;
    let mut is_end = false;
    let mut end_idx = 0;
    let mut start_idx = 0;
    let mut idx = 0;
    buf.iter().try_for_each(|b| {
        if start_byte == *b && !is_start {
            start_idx = idx;
            idx += 1;
            is_start = true;
            ControlFlow::Continue(())
        } else if end_byte == *b && is_start && !is_end {
            end_idx = idx + 1;
            is_end = true;
            ControlFlow::Break(())
        } else if is_start && !is_end {
            idx += 1;
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    });

    let data = &buf[start_idx..end_idx];

    TcpPackage {
        _start: start_idx,
        end: end_idx,
        data: bytes::Bytes::copy_from_slice(data),
    }
}

fn truncate_package(buf: &mut bytes::BytesMut, package: &TcpPackage) {
    let len = buf.len();
    buf.reverse();
    buf.truncate(len - package.end);
    buf.reverse();
}

#[async_trait]
pub trait PackageReaderX: Send + Sync {
    async fn read_next(&mut self) -> Result<TcpPackage, MpsPackageReaderErr>;
}

#[async_trait]
pub trait WriterX: Send + Sync {
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), RxLightWriterErr>;
}

#[derive(Debug)]
pub enum MpsPackageReaderErr {
    IoErr(io::Error),
    EOF,
    EmptyBuf,
}

#[derive(Debug)]
pub enum RxLightWriterErr {
    Io(io::Error),
}

impl Display for RxLightWriterErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RxLightWriterErr::Io(err) => write!(f, "{}", err),
        }
    }
}

pub struct TcpReaderX {
    buf: bytes::BytesMut,
    read_buf: bytes::BytesMut,
    reader: OwnedReadHalf,
    start_byte: u8,
    end_byte: u8,
}

impl TcpReaderX {
    pub fn new(reader: OwnedReadHalf, start_byte: u8, end_byte: u8) -> Self {
        Self {
            buf: bytes::BytesMut::with_capacity(TCP_BUF_SIZE),
            read_buf: bytes::BytesMut::with_capacity(TCP_BUF_SIZE),
            reader,
            start_byte,
            end_byte,
        }
    }
}

#[async_trait]
impl PackageReaderX for TcpReaderX {
    async fn read_next(&mut self) -> Result<TcpPackage, MpsPackageReaderErr> {
        let n = self
            .reader
            .read_buf(&mut self.buf)
            .await
            .map_err(|err| MpsPackageReaderErr::IoErr(err))?;
        if n == 0 {
            return Err(MpsPackageReaderErr::EOF);
        }

        merge_package(&mut self.read_buf, &mut self.buf);

        if self.read_buf.is_empty() {
            return Err(MpsPackageReaderErr::EmptyBuf);
        }

        let package = next_package(&self.read_buf, self.start_byte, self.end_byte);
        truncate_package(&mut self.read_buf, &package);

        Ok(package)
    }
}

pub struct TcpWriterX {
    writer: OwnedWriteHalf,
}

impl TcpWriterX {
    pub fn new(writer: OwnedWriteHalf) -> Self {
        Self { writer }
    }
}

#[async_trait]
impl WriterX for TcpWriterX {
    async fn write_all(&mut self, buf: &[u8]) -> Result<(), RxLightWriterErr> {
        self.writer
            .write_all(buf)
            .await
            .map_err(|err| RxLightWriterErr::Io(err))
    }
}

#[cfg(test)]
mod tests {
    use crate::socketx::{merge_package, next_package, truncate_package};
    use bytes::BufMut;

    #[test]
    fn test_merge_package() {
        let buf = &mut bytes::BytesMut::with_capacity(10);
        let read_buf = &mut bytes::BytesMut::with_capacity(10);
        buf.put_slice(&[0x7c, 0x89, 0x72, 0x75]);
        read_buf.put_slice(&[0x7c, 0x89, 0x72, 0x75]);
        merge_package(buf, read_buf);

        let buf: &[u8] = &buf;

        assert_eq!(&[0x7c, 0x89, 0x72, 0x75, 0x7c, 0x89, 0x72, 0x75], buf);
        assert!(read_buf.is_empty())
    }

    #[test]
    fn test_truncate_package() {
        let buf = &mut bytes::BytesMut::with_capacity(10);
        buf.put_slice(&[0x7c, 0x89, 0x72, 0x75, 0x79, 0xff, 0x3e]);
        let package = super::TcpPackage {
            _start: 0,
            end: 4,
            data: bytes::Bytes::new(),
        };
        truncate_package(buf, &package);
        assert_eq!(buf.to_vec(), vec![0x79, 0xff, 0x3e]);
    }

    #[test]
    fn test_next_package() {
        let buf = &mut bytes::BytesMut::with_capacity(10);
        buf.put_slice(&[0x7c, 0x89, 0x72, 0x75, 0x79, 0xff, 0x3e, 0x7c, 0x98, 0x97]);
        let package = next_package(buf, 0x7c, 0x7c);
        assert_eq!(package._start, 0);
        assert_eq!(package.end, 8);
    }
}

use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::{Stream, StreamExt};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::net::tcp::OwnedReadHalf;

use crate::iox;
use crate::iox::DataPack;

pub struct TimeoutStream<VAL, T> {
    reader: T,
    pub timeout: Duration,
    _phantom: PhantomData<VAL>,
}

impl<VAL, T> TimeoutStream<VAL, T> {
    pub fn new(reader: T, timeout: Duration) -> Self {
        Self {
            reader,
            timeout,
            _phantom: PhantomData,
        }
    }
}

impl<VAL, T> TimeoutStream<VAL, T>
where
    T: Stream<Item = io::Result<VAL>> + Unpin,
    VAL: Unpin,
{
    pub async fn try_fetch_next(&mut self) -> Option<T::Item> {
        tokio::time::timeout(self.timeout, self.reader.next())
            .await
            .unwrap_or_else(|err| {
                Some(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    err.to_string(),
                )))
            })
    }
}
//
// impl<VAL, T> Stream for TimeoutStream<VAL, T>
// where
//     T: Stream<Item = super::Result<VAL>> + Unpin,
//     VAL: Unpin,
// {
//     type Item = super::Result<VAL>;
//
//     fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         let this = self.get_mut();
//         let mut pinned = std::pin::pin!(tokio::time::timeout(this.timeout, this.reader.next()));
//         match pinned.as_mut().poll(cx) {
//             Poll::Ready(Ok(Some(v))) => Poll::Ready(Some(v)),
//             Poll::Ready(Ok(None)) => Poll::Ready(None),
//             Poll::Ready(Err(err)) => Poll::Ready(Some(Err(io::Error::new(
//                 io::ErrorKind::TimedOut,
//                 format!("Timeout: {:?}", err),
//             )))),
//             Poll::Pending => Poll::Pending,
//         }
//     }
// }

pub struct TcpReader {
    buf: bytes::BytesMut,
    read_buf: bytes::BytesMut,
    reader: OwnedReadHalf,
    start_byte: u8,
    end_byte: u8,
}

impl TcpReader {
    pub fn new(
        reader: OwnedReadHalf,
        start_byte: u8,
        end_byte: u8,
        buf: bytes::BytesMut,
        read_buf: bytes::BytesMut,
    ) -> Self {
        Self {
            buf,
            read_buf,
            reader,
            start_byte,
            end_byte,
        }
    }
}

impl Stream for TcpReader {
    type Item = io::Result<DataPack>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let ref mut buf = ReadBuf::new(&mut this.buf);

        let mut pinned = std::pin::pin!(&mut this.reader);
        match pinned.as_mut().poll_read(cx, buf) {
            Poll::Ready(err) => {
                if let Err(err) = err.map_err(|err| match err.kind() {
                    io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::NotConnected
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::UnexpectedEof => {
                        io::Error::new(io::ErrorKind::UnexpectedEof, "EOF")
                    }
                    _ => err,
                }) {
                    return Poll::Ready(Some(Err(err)));
                }
            }
            Poll::Pending => return Poll::Pending,
        }

        iox::merge_package(&mut this.read_buf, &mut this.buf);

        if this.read_buf.is_empty() {
            return Poll::Ready(None);
        }

        let package = iox::next_package(&this.read_buf, this.start_byte, this.end_byte);
        iox::truncate_package(&mut this.read_buf, &package);

        Poll::Ready(Some(Ok(package)))
    }
}


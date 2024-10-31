use std::io::Error;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::AsyncWrite;
use tokio::net::tcp::OwnedWriteHalf;

pub struct DelayedWriter {
    delay: Duration,
}

impl AsyncWrite for DelayedWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        todo!()
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        todo!()
    }
}

pub struct TcpWriter {
    writer: OwnedWriteHalf,
}

impl TcpWriter {
    pub fn new(writer: OwnedWriteHalf) -> Self {
        Self { writer }
    }
}

impl AsyncWrite for TcpWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        Pin::new(&mut this.writer).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.writer).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.writer).poll_shutdown(cx)
    }
}

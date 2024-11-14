use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;

use crate::times::now_timestamp;

pub struct DelayedWriter<W: AsyncWrite> {
    delay: Duration,
    writer: W,
    last_sent_at: i64,
    window: Duration,
}

impl<W> DelayedWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub fn new(writer: W, delay: Duration, window: Duration) -> Self {
        Self {
            delay,
            writer,
            last_sent_at: now_timestamp(),
            window,
        }
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let ts = now_timestamp();
        if Duration::from_millis((ts - self.last_sent_at) as u64) <= self.window {
            tokio::time::sleep(self.delay).await;
            self.last_sent_at = now_timestamp();
        }
        self.writer.write_all(buf).await
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

#[cfg(test)]
mod tests {
    use std::io;
    use std::time::Duration;

    use crate::iox::writer::DelayedWriter;
    use crate::times::now_timestamp;

    #[tokio::test]
    async fn test_delay_writer() {
        let cursor = io::Cursor::new(vec![]);
        let mut writer = DelayedWriter::new(
            cursor,
            Duration::from_millis(100),
            Duration::from_millis(100),
        );

        let ts = now_timestamp();
        writer.write_all(b"hello").await.unwrap();
        let elapsed = now_timestamp() - ts;
        assert!(elapsed >= 100);

        tokio::time::sleep(Duration::from_millis(130)).await;
        let ts = now_timestamp();
        writer.write_all(b"world").await.unwrap();
        let elapsed = now_timestamp() - ts;

        assert!(elapsed < 100);

        let buf = writer.writer.into_inner();
        assert_eq!(buf, b"helloworld");
    }
}

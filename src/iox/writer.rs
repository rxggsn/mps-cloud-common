use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

#[cfg(feature = "s3x")]
use crate::socketx::ws_err_to_io;
use crate::times::now_timestamp;
use futures::stream::SplitSink;
use tokio::io::{AsyncWrite, AsyncWriteExt};
#[cfg(feature = "s3x")]
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;

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

#[cfg(feature = "s3x")]
pub struct WsWriter {
    sink: SplitSink<
        tokio_tungstenite::WebSocketStream<TcpStream>,
        tokio_tungstenite::tungstenite::Message,
    >,
}

#[cfg(feature = "s3x")]
impl WsWriter {
    pub fn new(
        sink: SplitSink<
            tokio_tungstenite::WebSocketStream<TcpStream>,
            tokio_tungstenite::tungstenite::Message,
        >,
    ) -> Self {
        Self { sink }
    }
}

#[cfg(feature = "s3x")]
impl AsyncWrite for WsWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        use futures::Sink as _;
        let this = self.get_mut();
        let msg =
            tokio_tungstenite::tungstenite::Message::binary(bytes::Bytes::copy_from_slice(buf));
        match Pin::new(&mut this.sink).poll_ready(cx) {
            Poll::Ready(Ok(())) => match Pin::new(&mut this.sink).start_send(msg) {
                Ok(()) => Poll::Ready(Ok(buf.len())),
                Err(err) => Poll::Ready(Err(ws_err_to_io(err))),
            },
            Poll::Ready(Err(err)) => Poll::Ready(Err(ws_err_to_io(err))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        use futures::Sink as _;
        let this = self.get_mut();
        match Pin::new(&mut this.sink).poll_flush(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(err)) => Poll::Ready(Err(ws_err_to_io(err))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        use futures::Sink as _;
        let this = self.get_mut();
        match Pin::new(&mut this.sink).poll_close(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(err)) => Poll::Ready(Err(ws_err_to_io(err))),
            Poll::Pending => Poll::Pending,
        }
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

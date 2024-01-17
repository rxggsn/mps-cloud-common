use futures::StreamExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::codec;

// LocalStream will fetch from locality with the assumption that only one consumer is fetching data from this stream
pub type LocalStream<T> = Receiver<Result<T, tonic::Status>>;
pub type RemoteStream<T> = codec::Streaming<T>;

pub type StreamProducer<T> = Sender<Result<T, tonic::Status>>;

pub enum RpcStream<T> {
    Local(LocalStream<T>),
    Empty,
    Remote(RemoteStream<T>),
}

impl<T> tonic::codegen::tokio_stream::Stream for RpcStream<T> {
    type Item = Result<T, tonic::Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.get_mut() {
            RpcStream::Local(receiver) => receiver.poll_recv(cx),
            RpcStream::Empty => std::task::Poll::Ready(None),
            RpcStream::Remote(stream) => stream.poll_next_unpin(cx),
        }
    }
}

unsafe impl<T> Send for RpcStream<T> {}

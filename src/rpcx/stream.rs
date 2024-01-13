use tokio::sync::mpsc::Receiver;

pub enum RpcStream<T> {
    Receiver(Receiver<Result<T, tonic::Status>>),
    Empty,
}

impl<T> tonic::codegen::tokio_stream::Stream for RpcStream<T> {
    type Item = Result<T, tonic::Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.get_mut() {
            RpcStream::Receiver(receiver) => receiver.poll_recv(cx),
            RpcStream::Empty => std::task::Poll::Ready(None),
        }
    }
}

unsafe impl<T> Send for RpcStream<T> {}

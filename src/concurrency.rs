use futures::{Future, FutureExt};
use tokio::sync::watch::{Receiver, Ref};
use tonic::async_trait;

#[async_trait]
pub trait Subscriber<T>: Sync + Send {
    async fn next(&self) -> Option<Ref<'_, T>>;
}

pub struct Watcher<T>
where
    T: Sync + Send,
{
    inner: Receiver<T>,
}

impl<T> Watcher<T>
where
    T: Sync + Send,
{
    pub fn new(inner: Receiver<T>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl<T> Subscriber<T> for Watcher<T>
where
    T: Sync + Send,
{
    async fn next(&self) -> Option<Ref<'_, T>> {
        let result = self.inner.clone().changed().await;
        match result {
            Ok(_) => Some(self.inner.borrow()),
            Err(err) => {
                tracing::error!("watcher error: {}", err);
                None
            }
        }
    }
}

pub enum SubscriberError {
    Closed,
}

pub fn wait<R, FnFut>(cx: &mut std::task::Context<'_>, fut: FnFut) -> R
where
    FnFut: Future<Output = R> + Send,
    R: Send + 'static,
{
    let mut future = Box::pin(fut);
    loop {
        match future.poll_unpin(cx) {
            std::task::Poll::Ready(result) => return result,
            std::task::Poll::Pending => continue,
        }
    }
}

pub async fn join<R, FnFut>(futures: Vec<FnFut>) -> Vec<R>
where
    FnFut: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    let mut count = 0;
    let mut result = vec![];
    let total = futures.len();
    let handlers = futures
        .into_iter()
        .map(|fut| tokio::spawn(async move { fut.await }))
        .collect::<Vec<_>>();

    loop {
        handlers.iter().for_each(|handler| {
            if handler.is_finished() {
                count += 1;
            }
        });
        if count == total {
            break;
        }
    }

    for handler in handlers {
        let r = handler.await;
        r.map_err(|err| {
            tracing::error!("join error: {}", err);
        })
        .into_iter()
        .for_each(|r| {
            result.push(r);
        });
    }

    result
}

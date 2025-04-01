use std::{
    fmt::Debug,
    sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use futures::{Future, FutureExt, Stream};
use pin_project_lite::pin_project;
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

pub fn read<T>(rwlock: &RwLock<T>) -> RwLockReadGuard<T> {
    rwlock.read().unwrap_or_else(|e| {
        rwlock.clear_poison();
        e.into_inner()
    })
}

pub fn write<T>(rwlock: &RwLock<T>) -> RwLockWriteGuard<T> {
    rwlock.write().unwrap_or_else(|e| {
        rwlock.clear_poison();
        e.into_inner()
    })
}

pub fn mutex<T>(mutex: &Mutex<T>) -> MutexGuard<T> {
    mutex.lock().unwrap_or_else(|e| {
        mutex.clear_poison();
        e.into_inner()
    })
}
pin_project! {
    /// A stream that delays the execution of a future until the stream is polled.
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct DelayStream<T, E, S: Stream<Item = T>, F: Future<Output = Result<S,E>>> {
        #[pin]
        inner: F,
        #[pin]
        result: Option<S>,
    }
}

pub fn delay_stream<T, E, S: Stream<Item = T>, F: Future<Output = Result<S, E>>>(
    future: F,
) -> DelayStream<T, E, S, F> {
    DelayStream {
        inner: future,
        result: None,
    }
}

impl<T, E, S, F> Stream for DelayStream<T, E, S, F>
where
    E: Debug,
    S: Stream<Item = T>,
    F: Future<Output = Result<S, E>>,
{
    type Item = T;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        if this.result.is_none() {
            let inner = this.inner.as_mut();
            match inner.poll(cx) {
                std::task::Poll::Ready(Ok(result)) => {
                    this.result.set(Some(result));
                }
                std::task::Poll::Ready(Err(err)) => {
                    tracing::error!("delay stream error: {:?}", err);
                    return std::task::Poll::Ready(None);
                }
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
        let result = this.result.as_mut().as_pin_mut();
        match result {
            Some(result) => match result.poll_next(cx) {
                std::task::Poll::Ready(Some(item)) => std::task::Poll::Ready(Some(item)),
                std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
                std::task::Poll::Pending => std::task::Poll::Pending,
            },
            None => std::task::Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Mutex;
    use std::{
        sync::{Arc, RwLock},
        thread,
    };

    #[test]
    fn test_read() {
        let rwlock = Arc::new(RwLock::new(1));
        let c_rwlock = Arc::clone(&rwlock);
        let val = super::read(&rwlock);
        assert_eq!(*val, 1);
        assert_eq!(rwlock.is_poisoned(), false);
        drop(val);

        thread::spawn(move || {
            let mut val = super::write(&c_rwlock);
            *val = 2;
            panic!("")
        })
        .join()
        .unwrap_err();

        assert_eq!(rwlock.is_poisoned(), true);
        let val = super::read(&rwlock);
        assert_eq!(*val, 2);
        assert_eq!(rwlock.is_poisoned(), false);
    }

    #[test]
    fn test_write() {
        let rwlock = Arc::new(RwLock::new(1));
        let c_rwlock = Arc::clone(&rwlock);
        {
            let mut val = super::write(&c_rwlock);
            *val = 2;
        }

        {
            let val = super::read(&rwlock);
            assert_eq!(*val, 2);
            assert_eq!(rwlock.is_poisoned(), false);
        }

        thread::spawn(move || {
            let mut val = super::write(&c_rwlock);
            *val = 3;
            panic!("")
        })
        .join()
        .unwrap_err();

        {
            assert_eq!(rwlock.is_poisoned(), true);
            let val = super::read(&rwlock);
            assert_eq!(*val, 3);
            assert_eq!(rwlock.is_poisoned(), false);
        }
    }

    #[test]
    fn test_mutex() {
        let lock = Arc::new(Mutex::new(1));
        let c_lock = Arc::clone(&lock);

        {
            let val = super::mutex(&lock);
            assert_eq!(*val, 1);
        }

        {
            thread::spawn(move || {
                let mut val = super::mutex(&c_lock);
                *val = 2;
                panic!("");
            })
            .join()
            .unwrap_err();
        }

        {
            let val = super::mutex(&lock);
            assert_eq!(*val, 2);
        }
    }
}

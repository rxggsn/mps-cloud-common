use std::{
    ops::{AddAssign, SubAssign},
    sync::Arc,
};

use crossbeam_skiplist::SkipMap;
use tokio::sync::mpsc::Receiver;
use tonic::{
    body::BoxBody,
    transport::{self, channel::ResponseFuture, Endpoint},
};
use tower::discover::Change;
use tower_service::Service;

use super::PodName;

#[derive(Clone, Debug)]
pub enum LoadBalancePolicy {
    RoundRobin { last_idx: usize },
}
impl LoadBalancePolicy {
    fn get_next_pod(&mut self, replicas: usize) -> usize {
        match self {
            LoadBalancePolicy::RoundRobin { last_idx } => {
                let mut next_idx = *last_idx;
                while next_idx == *last_idx && replicas > 1 {
                    next_idx = (next_idx + 1) % replicas;
                }
                last_idx.sub_assign(*last_idx);
                last_idx.add_assign(next_idx);
                next_idx
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct PodLoadBalancer {
    inner: Vec<Pod>,
    pod_change_stream: Arc<SkipMap<PodName, Change<PodName, Endpoint>>>,
    policy: LoadBalancePolicy,
}

impl PodLoadBalancer {
    pub fn call(&mut self, req: http::Request<BoxBody>) -> ResponseFuture {
        let next_idx = self.policy.get_next_pod(self.inner.len());

        let pod = self.inner.get_mut(next_idx).expect("no pod is ready");
        pod.call(req)
    }

    pub fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), transport::Error>> {
        self.update_pods_status();

        let replicas = self.inner.len();
        let ready_num = self.inner.iter().filter(|pod| pod.ready).count();

        if ready_num >= replicas / 2 {
            std::task::Poll::Ready(Ok(()))
        } else {
            std::task::Poll::Pending
        }
    }

    fn update_pods_status(&mut self) {
        self.pod_change_stream.iter().for_each(|entry| {
            let change = entry.value();
            match change {
                Change::Insert(podname, endpoint) => {
                    let channel = endpoint.connect_lazy();
                    self.inner.push(Pod {
                        name: podname.clone(),
                        inner: channel,
                        ready: true,
                    });
                }
                Change::Remove(podname) => self
                    .inner
                    .iter()
                    .position(|pod| &pod.name == podname)
                    .into_iter()
                    .for_each(|idx| {
                        self.inner.remove(idx);
                    }),
            }
        });

        self.pod_change_stream.clear();
    }

    pub async fn watch_replica_change(
        mut change_rx: Receiver<Change<PodName, Endpoint>>,
        pod_change_stream: Arc<SkipMap<PodName, Change<PodName, Endpoint>>>,
    ) {
        while let Some(change) = change_rx.recv().await {
            let podname = match &change {
                Change::Insert(pod_name, ..) => pod_name.clone(),
                Change::Remove(pod_name) => pod_name.clone(),
            };

            pod_change_stream.insert(podname, change);
        }
    }

    pub fn get_mut(&mut self, podname: &str) -> Option<&mut Pod> {
        self.inner.iter_mut().find(|pod| &pod.name == podname)
    }

    pub(crate) fn new(change_rx: Receiver<Change<PodName, Endpoint>>) -> PodLoadBalancer {
        let pod_change_stream = Arc::new(SkipMap::default());
        let cloned_change_stream = pod_change_stream.clone();
        tokio::spawn(Self::watch_replica_change(change_rx, cloned_change_stream));

        Self {
            inner: Default::default(),
            pod_change_stream,
            policy: LoadBalancePolicy::RoundRobin { last_idx: 0 },
        }
    }
}

#[derive(Clone, Debug)]
pub struct Pod {
    name: PodName,
    inner: transport::Channel,
    ready: bool,
}
impl Pod {
    pub fn call(&mut self, req: http::Request<BoxBody>) -> ResponseFuture {
        self.inner.call(req)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use crossbeam_skiplist::SkipMap;
    use futures::FutureExt;
    use tokio::sync::mpsc;
    use tonic::transport::Endpoint;
    use tower::discover::Change;

    use crate::rpcx::PodName;

    use super::LoadBalancePolicy;

    #[test]
    fn test_load_balance_policy() {
        let mut policy = LoadBalancePolicy::RoundRobin { last_idx: 0 };
        assert_eq!(policy.get_next_pod(1), 0);

        assert!(policy.get_next_pod(2) == 1);
        match &policy {
            LoadBalancePolicy::RoundRobin { last_idx } => assert_eq!(*last_idx, 1),
        }

        assert!(policy.get_next_pod(3) == 2);
        match &policy {
            LoadBalancePolicy::RoundRobin { last_idx } => assert_eq!(*last_idx, 2),
        }

        assert!(policy.get_next_pod(3) == 0);
        match &policy {
            LoadBalancePolicy::RoundRobin { last_idx } => assert_eq!(*last_idx, 0),
        }

        assert!(policy.get_next_pod(3) == 1);
        match &policy {
            LoadBalancePolicy::RoundRobin { last_idx } => assert_eq!(*last_idx, 1),
        }
    }

    #[tokio::test]
    async fn test_change_event_process() {
        let (tx, rx) = mpsc::channel(10);
        let pod_change_stream: Arc<SkipMap<PodName, Change<PodName, Endpoint>>> =
            Arc::new(Default::default());
        tokio::spawn(super::PodLoadBalancer::watch_replica_change(
            rx,
            pod_change_stream.clone(),
        ));

        tx.send(Change::Insert(
            "pod-0".to_string(),
            Endpoint::from_static("http://pod-0"),
        ))
        .then(|_| {
            tx.send(Change::Insert(
                "pod-1".to_string(),
                Endpoint::from_static("http://pod-1"),
            ))
        })
        .then(|r| async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            r
        })
        .await
        .expect("msg");

        assert!(pod_change_stream.len() == 2);
        assert!(pod_change_stream.contains_key(&"pod-0".to_string()));
        assert!(pod_change_stream.contains_key(&"pod-1".to_string()));
    }
}

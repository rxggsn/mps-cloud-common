use std::{
    hash::Hasher,
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

use crate::times::now_timestamp;

use super::PodName;

#[derive(Debug)]
pub struct PodChange {
    pub event: Change<PodName, Endpoint>,
    pub timestamp: i64,
}
impl PodChange {
    fn has_been_processed(&self, last_pod_status_updated_at: i64) -> bool {
        self.timestamp < last_pod_status_updated_at
    }
}

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

// PodLoadBalance pod states consistency:
// -----------------
// PodLoadBalance guarentee inner pod states is eventually consistent in concurrency environment.

// Listener-thread-0                 |Thread-0                          | Thread-1
// ----------------------------------|----------------------------------|----------
// // here, listener send pod change |                                  |
// let _  = tx.send(..)                // update inner states
//                                   | thread_0.update_pod_status()       // concurrently update inner states
//                                   |                                  | thread_1.update_pod_status()
//                                     // thread-0 and thread-1 will share same inner states
//                                   | thread_0.inner.len() == 2        | thread_1.inner.len() == 2            |
#[derive(Clone, Debug)]
pub struct PodLoadBalancer {
    inner: Vec<Pod>,
    pod_change_stream: Arc<SkipMap<PodName, PodChange>>,
    policy: LoadBalancePolicy,
    last_pod_status_updated_at: i64,
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
        self.update_inner_states();
        self.poll_ready_pods(cx);

        let replicas = self.inner.len();
        let ready_num = self.inner.iter().filter(|pod| pod.ready).count();

        if ready_num >= replicas / 2 {
            std::task::Poll::Ready(Ok(()))
        } else {
            std::task::Poll::Pending
        }
    }

    fn update_inner_states(&mut self) {
        let mut has_event_update = false;
        self.pod_change_stream.iter().for_each(|entry| {
            let change = entry.value();
            if !change.has_been_processed(self.last_pod_status_updated_at) {
                match &change.event {
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
                };

                has_event_update = true;
            }
        });

        if has_event_update {
            self.last_pod_status_updated_at = now_timestamp();
        }
    }

    pub async fn watch_replica_change(
        mut change_rx: Receiver<Change<PodName, Endpoint>>,
        pod_change_stream: Arc<SkipMap<PodName, PodChange>>,
    ) {
        while let Some(change) = change_rx.recv().await {
            let podname = match &change {
                Change::Insert(pod_name, ..) => pod_name.clone(),
                Change::Remove(pod_name) => pod_name.clone(),
            };

            pod_change_stream.insert(
                podname,
                PodChange {
                    event: change,
                    timestamp: now_timestamp(),
                },
            );
        }
    }

    pub fn get_mut(&mut self, podname: &str) -> Option<&mut Pod> {
        self.inner
            .iter_mut()
            .position(|pod| &pod.name == podname)
            .and_then(|idx| self.inner.get_mut(idx))
    }

    pub(crate) fn new(change_rx: Receiver<Change<PodName, Endpoint>>) -> PodLoadBalancer {
        let pod_change_stream = Arc::new(SkipMap::default());
        let cloned_change_stream = pod_change_stream.clone();
        tokio::spawn(Self::watch_replica_change(change_rx, cloned_change_stream));

        Self {
            inner: Default::default(),
            pod_change_stream,
            policy: LoadBalancePolicy::RoundRobin { last_idx: 0 },
            last_pod_status_updated_at: now_timestamp(),
        }
    }

    fn poll_ready_pods(&mut self, cx: &mut std::task::Context<'_>) {
        self.inner.iter_mut().for_each(|pod| pod.poll_ready(cx));
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

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) {
        match self.inner.poll_ready(cx) {
            std::task::Poll::Ready(_) => self.ready = true,
            std::task::Poll::Pending => self.ready = false,
        }
    }
}

impl std::hash::Hash for Pod {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for Pod {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Pod {}

impl PartialOrd for Pod {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.name.partial_cmp(&other.name)
    }
}

impl Ord for Pod {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
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

    use crate::rpcx::{lb::PodChange, PodName};

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
        let pod_change_stream: Arc<SkipMap<PodName, PodChange>> = Arc::new(Default::default());
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

    #[tokio::test]
    async fn test_control_plane_inner_states_eventually_consistant() {
        let (tx, rx) = mpsc::channel(10);
        let mut thread_0_control_plane = super::PodLoadBalancer::new(rx);

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
            tokio::time::sleep(Duration::from_millis(20)).await;
            r
        })
        .await
        .expect("msg");

        // a thread clone the control plane
        let mut thread_1_control_plane = thread_0_control_plane.clone();
        // thread-0 update the inner states of control plane
        thread_0_control_plane.update_inner_states();
        assert!(thread_0_control_plane.inner.len() == 2);
        // thread-1 update the inner states of control plane
        thread_1_control_plane.update_inner_states();
        // the inner state of control plane in thread-0 and thread-1 should be eventually consistent
        assert!(thread_1_control_plane.inner.len() == 2);

        let _ = tx
            .send(Change::Insert(
                "pod-2".to_string(),
                Endpoint::from_static("http://pod-2"),
            ))
            .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        // now, thread-2 clone a control plane
        let mut thread_2_control_plane = thread_0_control_plane.clone();

        // and then, the inner states of all control planes in thread-0, thread-1 and thread-2 should be eventually consistent
        thread_0_control_plane.update_inner_states();
        assert!(thread_0_control_plane.inner.len() == 3);
        thread_1_control_plane.update_inner_states();
        assert!(thread_1_control_plane.inner.len() == 3);
        thread_2_control_plane.update_inner_states();
        assert!(thread_2_control_plane.inner.len() == 3);

        let _ = tx.send(Change::Remove("pod-2".to_string())).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        thread_0_control_plane.update_inner_states();
        assert!(thread_0_control_plane.inner.len() == 2);
        thread_1_control_plane.update_inner_states();
        assert!(thread_1_control_plane.inner.len() == 2);
        thread_2_control_plane.update_inner_states();
        assert!(thread_2_control_plane.inner.len() == 2);
    }
}

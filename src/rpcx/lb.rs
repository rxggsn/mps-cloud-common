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

#[derive(Clone, Debug)]
pub struct PodLoadBalancer {
    inner: Vec<Pod>,
    pod_change_stream: Arc<SkipMap<PodName, Change<PodName, Endpoint>>>,
    policy: LoadBalancePolicy,
}

impl PodLoadBalancer {
    pub fn call(&mut self, req: http::Request<BoxBody>) -> ResponseFuture {
        match &mut self.policy {
            LoadBalancePolicy::RoundRobin { last_idx } => {
                let mut next_idx = *last_idx;
                let replicas = self.inner.len();
                while next_idx == *last_idx && replicas > 1 {
                    next_idx = (*last_idx + 1) % replicas;
                }
                last_idx.sub_assign(*last_idx);
                last_idx.add_assign(next_idx);
                let pod = self.inner.get_mut(next_idx).expect("no pod is ready");
                pod.call(req)
            }
        }
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
        latest_change_events: Arc<SkipMap<PodName, Change<PodName, Endpoint>>>,
    ) {
        while let Some(change) = change_rx.recv().await {
            let podname = match &change {
                Change::Insert(pod_name, ..) => pod_name.clone(),
                Change::Remove(pod_name) => pod_name.clone(),
            };

            latest_change_events.insert(podname, change);
        }
    }

    pub fn get_mut(&mut self, podname: &String) -> Option<&mut Pod> {
        self.inner.iter_mut().find(|pod| &pod.name == podname)
    }

    pub(crate) fn new(change_rx: Receiver<Change<PodName, Endpoint>>) -> PodLoadBalancer {
        let latest_change_events = Arc::new(SkipMap::default());
        let cloned_change_events = latest_change_events.clone();
        tokio::spawn(PodLoadBalancer::watch_replica_change(
            change_rx,
            cloned_change_events,
        ));

        Self {
            inner: Default::default(),
            pod_change_stream: latest_change_events,
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

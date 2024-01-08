use std::{
    collections::{BTreeMap, HashSet},
    ops::{AddAssign, SubAssign},
    sync::Arc,
};

use crossbeam_skiplist::SkipMap;
use futures::{TryFuture, TryFutureExt};
use http::header::HOST;
use hyper::Body;
use tokio::sync::mpsc::Receiver;
use tonic::{
    body::BoxBody,
    transport::{self, channel::ResponseFuture, Endpoint},
    Code,
};
use tower::discover::Change;
use tower_service::Service;

use super::PodName;

pub async fn retry_rpc<Req, Resp, RetryRpcFut>(
    req: &Req,
    mut rpc: impl FnMut(Req) -> RetryRpcFut,
) -> Result<Resp, tonic::Status>
where
    Req: Clone,
    RetryRpcFut: TryFuture<Ok = Resp, Error = tonic::Status>,
{
    let mut retry_count = 0;
    loop {
        match rpc(req.clone()).into_future().await {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                if e.code() != Code::Unknown || retry_count >= 3 {
                    return Err(e);
                }
                retry_count += 1;
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum MpsCloudChannel {
    Single(transport::Channel),
    Cluster {
        cluster: BTreeMap<PodName, transport::Channel>,
        ready_num: usize,
        latest_change_events: Arc<SkipMap<PodName, Change<PodName, Endpoint>>>,
        replicas: usize,
        host_sets: HashSet<PodName>,
    },
}

impl Service<http::Request<BoxBody>> for MpsCloudChannel {
    type Response = http::Response<Body>;
    type Error = transport::Error;
    type Future = ResponseFuture;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.update_cluster_status();
        match self {
            MpsCloudChannel::Single(channel) => channel.poll_ready(cx),
            MpsCloudChannel::Cluster {
                ready_num,
                replicas,
                ..
            } => {
                if *ready_num >= (*replicas) / 2 {
                    std::task::Poll::Ready(Ok(()))
                } else {
                    std::task::Poll::Pending
                }
            }
        }
    }

    fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
        match self {
            MpsCloudChannel::Single(channel) => channel.call(req),
            MpsCloudChannel::Cluster { cluster, .. } => match req.headers().get(HOST) {
                Some(host) => {
                    let podname = host.to_str().unwrap();
                    if let Some(entry) = cluster.get_mut(podname) {
                        entry.call(req)
                    } else {
                        panic!("pod {} is not ready in cluster", podname)
                    }
                }
                None => cluster.first_entry().unwrap().get_mut().call(req),
            },
        }
    }
}

impl MpsCloudChannel {
    pub fn new_cluster(replicas: usize, change_rx: Receiver<Change<PodName, Endpoint>>) -> Self {
        let cluster = BTreeMap::new();
        let latest_change_events = Arc::new(SkipMap::default());
        let cloned_change_events = latest_change_events.clone();
        tokio::spawn(Self::watch_replica_change(change_rx, cloned_change_events));
        MpsCloudChannel::Cluster {
            cluster,
            ready_num: 0,
            latest_change_events,
            replicas,
            host_sets: Default::default(),
        }
    }

    fn update_cluster_status(&mut self) {
        match self {
            MpsCloudChannel::Single(_) => {}
            MpsCloudChannel::Cluster {
                cluster,
                ready_num,
                latest_change_events,
                replicas,
                host_sets,
            } => {
                latest_change_events.iter().for_each(|entry| {
                    let change = entry.value();
                    match change {
                        Change::Insert(podname, endpoint) => {
                            let channel = endpoint.connect_lazy();
                            if !host_sets.contains(podname) && host_sets.len() == *replicas {
                                replicas.add_assign(1);
                            }
                            cluster.insert(podname.clone(), channel);
                            host_sets.insert(podname.clone());
                            ready_num.add_assign(1);
                        }
                        Change::Remove(podname) => {
                            host_sets.remove(podname);
                            cluster.remove(podname);
                            ready_num.sub_assign(1);
                        }
                    }
                });

                latest_change_events.clear();
            }
        }
    }

    async fn watch_replica_change(
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
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use tonic::transport::Endpoint;
    use tower::discover::Change;

    use crate::rpcx::client::MpsCloudChannel;

    #[tokio::test]
    async fn test_cluster_channel_update_cluster_status() {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        let mut cluster_channel = super::MpsCloudChannel::new_cluster(3, rx);

        {
            tx.send(Change::Insert(
                "pod-0".to_string(),
                Endpoint::from_static("http://192.168.0.1:8888"),
            ))
            .await
            .expect("msg");
            tokio::time::sleep(Duration::from_millis(10)).await;
            cluster_channel.update_cluster_status();

            match &cluster_channel {
                MpsCloudChannel::Cluster {
                    cluster,
                    ready_num,
                    host_sets,
                    replicas,
                    ..
                } => {
                    assert_eq!(cluster.len(), 1);
                    assert!(cluster.contains_key(&"pod-0".to_string()));
                    assert_eq!(*ready_num, 1);
                    assert_eq!(*replicas, 3);
                    assert_eq!(host_sets.len(), 1);
                    assert!(host_sets.contains(&"pod-0".to_string()))
                }
                _ => panic!("cluster channel is not cluster type"),
            }
        }

        {
            tx.send(Change::Remove("pod-0".to_string()))
                .await
                .expect("msg");

            tokio::time::sleep(Duration::from_millis(10)).await;
            cluster_channel.update_cluster_status();

            match &cluster_channel {
                MpsCloudChannel::Cluster {
                    cluster,
                    ready_num,
                    host_sets,
                    replicas,
                    ..
                } => {
                    assert_eq!(cluster.len(), 0);
                    assert_eq!(*ready_num, 0);
                    assert_eq!(host_sets.len(), 0);
                    assert_eq!(*replicas, 3);
                }
                _ => panic!("cluster channel is not cluster type"),
            }
        }

        {
            for x in 0..4 {
                tx.send(Change::Insert(
                    format!("pod-{}", x),
                    Endpoint::from_str(&format!("http://192.168.0.{}:8888", x)).expect("msg"),
                ))
                .await
                .expect("msg");
            }

            tokio::time::sleep(Duration::from_millis(10)).await;

            cluster_channel.update_cluster_status();

            match &cluster_channel {
                MpsCloudChannel::Cluster {
                    cluster,
                    ready_num,
                    host_sets,
                    replicas,
                    latest_change_events,
                } => {
                    assert_eq!(cluster.len(), 4);
                    for x in 0..4 {
                        assert!(cluster.contains_key(&format!("pod-{}", x)));
                        assert!(host_sets.contains(&format!("pod-{}", x)));
                    }

                    assert_eq!(*ready_num, 4);
                    assert_eq!(*replicas, 4);
                    assert_eq!(host_sets.len(), 4);
                    assert_eq!(latest_change_events.len(), 0);
                }
                _ => panic!("cluster channel is not cluster type"),
            }
        }
    }
}

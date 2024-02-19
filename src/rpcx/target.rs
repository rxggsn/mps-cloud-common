use crate::utils::look_up;
use futures::StreamExt;
use k8s_openapi::api::core::v1::{Pod, PodStatus, Service};
use kube::{
    api::{ListParams, WatchParams},
    core::WatchEvent,
    Api,
};
use std::{collections::HashSet, fmt};
use tokio::{
    io,
    sync::mpsc::{self, Sender},
};
use tonic::transport;
use tower::discover::Change;

use super::{client::ControlPlane, PodName, PodUid, DEFAULT_BUFFER_SIZE};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Target {
    Http(String),
    LB {
        namespace: String,
        service_name: String,
        port: Option<u16>,
    },
    DNS {
        namespace: String,
        service: String,
        hostname: String,
        port: Option<u16>,
    },
    Cluster {
        namespace: String,
        port: Option<u16>,
        service_name: String,
    },
    Unknown,
}

impl Target {
    pub async fn connect(&self) -> Result<ControlPlane, TargetError> {
        match self {
            Target::Http(endpoint) => {
                let channel = transport::Endpoint::new(endpoint.clone())
                    .map_err(|err| TargetError::Transport(err))?
                    .connect_lazy();

                Ok(ControlPlane::Single(channel))
            }
            Target::LB {
                namespace,
                service_name,
                port,
            } => {
                let (channel, change_tx) =
                    transport::Channel::balance_channel::<PodName>(DEFAULT_BUFFER_SIZE);
                let mut watcher = PodWatcher {
                    port,
                    service_name,
                    namespace,
                    selectors: None,
                };

                watcher
                    .watch_pod_change(change_tx)
                    .await
                    .map(|_| ControlPlane::Single(channel))
            }
            Target::DNS {
                namespace,
                service,
                hostname,
                port,
            } => {
                let dns_addr = format!(
                    "{}.{}.{}.svc.cluster.local:{}",
                    hostname,
                    service,
                    namespace,
                    port.unwrap_or(80)
                );
                match look_up(dns_addr)
                    .await
                    .map_err(|err| TargetError::Io(err))?
                {
                    Some(endpoint) => Ok(ControlPlane::Single(
                        transport::Endpoint::new(format!("http://{}", endpoint))
                            .map_err(|err| TargetError::Transport(err))?
                            .connect_lazy(),
                    )),
                    None => Err(TargetError::Unknown),
                }
            }
            Target::Unknown => Err(TargetError::Unknown),
            Target::Cluster {
                namespace,
                port,
                service_name,
            } => {
                let (change_tx, change_rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);

                let mut watcher = PodWatcher {
                    port,
                    service_name,
                    namespace,
                    selectors: None,
                };

                watcher
                    .watch_pod_change(change_tx)
                    .await
                    .map(|_| ControlPlane::new_cluster(change_rx))
            }
        }
    }
}

struct PodWatcher<'a> {
    port: &'a Option<u16>,
    service_name: &'a String,
    namespace: &'a String,
    selectors: Option<String>,
}

impl<'a> PodWatcher<'a> {
    const PENDING: &str = "Pending";
    const RUNNING: &str = "Running";
    const SUCCEEDED: &str = "Succeeded";
    const FAILED: &str = "Failed";

    async fn watch_pod_change(
        &mut self,
        change_tx: Sender<Change<PodName, transport::Endpoint>>,
    ) -> Result<usize, TargetError> {
        let k8s = kube::Client::try_default()
            .await
            .map_err(|err| TargetError::K8S(err))?;

        let replicas = self.init(&k8s, &change_tx).await?;

        let api = Api::<Pod>::namespaced(k8s, &self.namespace);
        let mut wp = WatchParams::default();
        if let Some(selectors) = &self.selectors {
            wp = wp.labels(&selectors);
        }

        let port = self.port.clone();
        tokio::spawn(async move {
            let ref mut pending_pods = HashSet::new();
            loop {
                match api
                    .watch(&wp, "0")
                    .await
                    .map_err(|err| TargetError::K8SWatchApi(err.to_string()))
                    .map(|s| s.boxed())
                {
                    Ok(mut stream) => {
                        while let Some(event) = stream.next().await {
                            match event {
                                Ok(event) => {
                                    Self::handle_pod_event(&port, event, &change_tx, pending_pods)
                                        .await;
                                }
                                Err(err) => {
                                    tracing::error!("watch pod error: {}", err);
                                }
                            }
                        }
                    }
                    Err(err) => {
                        tracing::error!("watch pod error: {}", err);
                    }
                }
            }
        });

        Ok(replicas)
    }

    async fn handle_pod_event(
        port: &Option<u16>,
        event: WatchEvent<Pod>,
        change_tx: &Sender<Change<PodName, transport::Endpoint>>,
        pending_pods: &mut HashSet<PodUid>,
    ) {
        async fn insert_new_pod(
            port: &Option<u16>,
            pod_status: PodStatus,
            uid: PodUid,
            pod_name: PodName,
            change_tx: &Sender<Change<PodName, transport::Endpoint>>,
            pending_pods: &mut HashSet<PodUid>,
        ) {
            if let Some(pod_ip) = &pod_status.pod_ip {
                let endpoint =
                    transport::Endpoint::new(format!("http://{}:{}", pod_ip, port.unwrap_or(80)))
                        .map_err(|err| TargetError::Transport(err))
                        .unwrap();
                match change_tx
                    .send(Change::Insert(pod_name.clone(), endpoint))
                    .await
                {
                    Ok(_) => {
                        pending_pods.remove(&uid);
                        tracing::debug!("insert new pod: {}", pod_name)
                    }
                    Err(err) => {
                        tracing::error!("insert new pod failed: {}", err)
                    }
                }
            }
        }

        match event {
            WatchEvent::Added(pod) => {
                let pod_name = pod.metadata.name.unwrap_or_default();
                let uid = pod.metadata.uid.unwrap_or_default();
                let status = pod.status.unwrap_or_default();
                let pod_phase = status.phase.clone().unwrap_or(Self::PENDING.to_string());
                if pod_phase == Self::RUNNING.to_string() {
                    insert_new_pod(port, status, uid, pod_name, change_tx, pending_pods).await;
                } else {
                    pending_pods.insert(uid);
                }
            }
            WatchEvent::Modified(pod) => {
                let pod_name = pod.metadata.name.unwrap_or_default();
                let uid = pod.metadata.uid.unwrap_or_default();
                let status = pod.status.unwrap_or_default();
                let pod_phase = status.phase.clone().unwrap_or(Self::PENDING.to_string());

                if pod_phase == Self::RUNNING && pending_pods.contains(&uid) {
                    insert_new_pod(port, status, uid, pod_name, change_tx, pending_pods).await
                } else if pod_phase == Self::SUCCEEDED || pod_phase == Self::FAILED {
                    pending_pods.remove(&uid);
                }
            }
            WatchEvent::Deleted(pod) => {
                let uid = pod.metadata.uid.unwrap_or_default();
                let pod_name = pod.metadata.name.unwrap_or_default();
                pending_pods.remove(&uid);

                match change_tx.send(Change::Remove(pod_name.clone())).await {
                    Ok(_) => tracing::info!("remove pod: {}", pod_name),
                    Err(err) => tracing::debug!("remove pod failed: {}", err),
                }
            }
            _ => {}
        }
    }

    async fn init(
        &mut self,
        k8s: &kube::Client,
        change_tx: &Sender<Change<PodName, transport::Endpoint>>,
    ) -> Result<usize, TargetError> {
        let svc_api = Api::<Service>::namespaced(k8s.clone(), &self.namespace);
        let svc = svc_api
            .get(&self.service_name)
            .await
            .map_err(|err| TargetError::K8S(err))?;
        let mut selectors = String::new();
        if let Some(svc_spec) = svc.spec {
            svc_spec.selector.iter().for_each(|selector| {
                selector.iter().for_each(|(k, v)| {
                    selectors.push_str(&format!("{}={},", k, v));
                })
            })
        } else {
            return Err(TargetError::Unavailable);
        }

        selectors.pop();
        let api = Api::<Pod>::namespaced(k8s.clone(), &self.namespace);
        let pods = api
            .list(&ListParams::default().labels(&selectors))
            .await
            .map_err(|err| TargetError::K8S(err))?;
        let replicas = pods.items.len();
        let endpoints: Vec<_> = pods
            .into_iter()
            .filter(|pod| {
                if let Some(status) = &pod.status {
                    if let Some(_) = &status.pod_ip {
                        return true;
                    }
                }
                false
            })
            .map(|pod| {
                let pod_name = pod.metadata.name.unwrap_or_default();
                let status = pod.status.unwrap();
                let pod_ip = status.pod_ip.unwrap();
                (
                    pod_name,
                    transport::Endpoint::new(format!(
                        "http://{}:{}",
                        pod_ip,
                        self.port.unwrap_or(80)
                    ))
                    .map_err(|err| TargetError::Transport(err)),
                )
            })
            .filter(|r| r.1.is_ok())
            .map(|r| (r.0, r.1.unwrap()))
            .collect();

        if endpoints.is_empty() {
            return Err(TargetError::Unavailable);
        }

        endpoints.iter().for_each(|(pod_name, endpoint)| {
            change_tx
                .try_send(Change::Insert(pod_name.clone(), endpoint.clone()))
                .unwrap();
        });

        self.selectors = Some(selectors);
        Ok(replicas)
    }
}

pub fn parse_target(dst: &str) -> Target {
    if dst.starts_with("http://") {
        Target::Http(dst.to_string())
    } else if dst.starts_with("lb://") || dst.starts_with("cluster://") {
        dst.split("://")
            .nth(1)
            .map(|s| {
                let mut parts = s.split("/");
                let namespace = parts
                    .next()
                    .map(|namespace| {
                        if namespace.is_empty() {
                            "default".to_string()
                        } else {
                            namespace.to_string()
                        }
                    })
                    .unwrap_or("default".to_string());
                let service_name = match parts.next() {
                    Some(s) => s.to_string(),
                    None => Default::default(),
                };

                let mut service_part = service_name.split(":");
                let service_name = service_part
                    .next()
                    .map(|name| name.to_string())
                    .unwrap_or_default();
                let port = service_part.next().and_then(|s| s.parse::<u16>().ok());

                if dst.starts_with("cluster://") {
                    Target::Cluster {
                        namespace,
                        port,
                        service_name,
                    }
                } else {
                    Target::LB {
                        namespace,
                        service_name,
                        port,
                    }
                }
            })
            .unwrap_or(Target::Unknown)
    } else if dst.starts_with("dns://") {
        dst.split("://")
            .nth(1)
            .map(|s| {
                let mut parts = s.split("/");
                let namespace = parts
                    .next()
                    .map(|ns| {
                        if ns.is_empty() {
                            "default".to_string()
                        } else {
                            ns.to_string()
                        }
                    })
                    .unwrap_or("default".to_string());
                let service = parts.next().map(|svc| svc.to_string()).unwrap_or_default();
                let endpoint = parts.next().map(|ep| ep.to_string()).unwrap_or_default();
                let mut parts = endpoint.split(":");
                let hostname = parts
                    .next()
                    .map(|hostname| hostname.to_string())
                    .unwrap_or_default();
                let port = parts.next().and_then(|s| s.parse::<u16>().ok());
                Target::DNS {
                    namespace,
                    service,
                    hostname,
                    port,
                }
            })
            .unwrap_or(Target::Unknown)
    } else {
        Target::Unknown
    }
}

#[derive(Debug)]
pub enum TargetError {
    Transport(transport::Error),
    K8S(kube::error::Error),
    Io(io::Error),
    Unknown,
    Unavailable,
    K8SWatchApi(String),
}

impl fmt::Display for TargetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TargetError::Transport(err) => write!(f, "transport error: {}", err),
            TargetError::K8S(err) => write!(f, "k8s error: {}", err),
            TargetError::Io(err) => write!(f, "io error: {}", err),
            TargetError::Unknown => write!(f, "unknown target"),
            TargetError::Unavailable => write!(f, "unavailable target"),
            TargetError::K8SWatchApi(message) => write!(f, "k8s watch api error: {}", message),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use http::Uri;
    use k8s_openapi::api::core::v1::Pod;
    use kube::core::WatchEvent;
    use tokio::sync::mpsc;
    use tonic::transport;
    use tower::discover::Change;

    use crate::rpcx::{PodName, DEFAULT_BUFFER_SIZE};

    use super::{parse_target, PodWatcher};

    #[test]
    fn test_parse_target() {
        let target = parse_target("http://service:80");
        assert_eq!(target, super::Target::Http("http://service:80".to_string()));

        {
            let target = parse_target("lb://test/service:80");
            assert_eq!(
                target,
                super::Target::LB {
                    namespace: "test".to_string(),
                    service_name: "service".to_string(),
                    port: Some(80),
                }
            );

            let target = parse_target("lb://test/service");
            assert_eq!(
                target,
                super::Target::LB {
                    namespace: "test".to_string(),
                    service_name: "service".to_string(),
                    port: None,
                }
            );

            let target = parse_target("lb://test/service:8888");
            assert_eq!(
                target,
                super::Target::LB {
                    namespace: "test".to_string(),
                    service_name: "service".to_string(),
                    port: Some(8888),
                }
            );

            let target = parse_target("lb:///service:8888");
            assert_eq!(
                target,
                super::Target::LB {
                    namespace: "default".to_string(),
                    service_name: "service".to_string(),
                    port: Some(8888),
                }
            );
        }
        {
            let target = parse_target("dns://test-ns/testsvc/test:8888");
            assert_eq!(
                target,
                super::Target::DNS {
                    namespace: "test-ns".to_string(),
                    service: "testsvc".to_string(),
                    hostname: "test".to_string(),
                    port: Some(8888)
                }
            );

            let target = parse_target("dns://test-ns/testsvc/test");
            assert_eq!(
                target,
                super::Target::DNS {
                    namespace: "test-ns".to_string(),
                    service: "testsvc".to_string(),
                    hostname: "test".to_string(),
                    port: None
                }
            );

            let target = parse_target("dns:///testsvc/test");
            assert_eq!(
                target,
                super::Target::DNS {
                    namespace: "default".to_string(),
                    service: "testsvc".to_string(),
                    hostname: "test".to_string(),
                    port: None
                }
            );
        }
    }

    #[tokio::test]
    async fn test_pod_watcher_handle_pod_event() {
        let port = Some(8888);
        let (change_tx, mut change_rx) =
            mpsc::channel::<Change<PodName, transport::Endpoint>>(DEFAULT_BUFFER_SIZE);
        let mut pending_pods = HashSet::new();

        {
            let mut pod = Pod::default();
            pod.metadata.name = Some("pod-0".to_string());
            pod.metadata.uid = Some("0000".to_string());
            pod.status = Some(Default::default());
            pod.status.iter_mut().for_each(|status| {
                status.pod_ip = Some("192.168.0.1".to_string());
                status.phase = Some(PodWatcher::PENDING.to_string());
            });
            let event = WatchEvent::Added(pod);
            PodWatcher::handle_pod_event(&port, event, &change_tx, &mut pending_pods).await;
            let e = tokio::time::timeout(Duration::from_millis(20), change_rx.recv()).await;

            assert!(e.is_err());

            pending_pods.clear();
        }

        {
            let mut pod = Pod::default();
            pod.metadata.name = Some("pod-0".to_string());
            pod.metadata.uid = Some("0000".to_string());
            pod.status = Some(Default::default());
            pod.status.iter_mut().for_each(|status| {
                status.pod_ip = Some("192.168.0.1".to_string());
                status.phase = Some(PodWatcher::RUNNING.to_string());
            });
            let event = WatchEvent::Added(pod);
            PodWatcher::handle_pod_event(&port, event, &change_tx, &mut pending_pods).await;

            let e = tokio::time::timeout(Duration::from_millis(20), change_rx.recv())
                .await
                .expect("msg");
            assert!(e.is_some());
            let e = e.unwrap();

            match e {
                Change::Insert(k, v) => {
                    assert_eq!(k, "pod-0".to_string());
                    assert_eq!(v.uri(), &Uri::from_static("http://192.168.0.1:8888"));
                    assert!(pending_pods.is_empty())
                }
                _ => panic!("unexpected change event"),
            }
        }

        {
            let mut pod = Pod::default();
            pod.metadata.name = Some("pod-0".to_string());
            pod.metadata.uid = Some("0000".to_string());
            pod.status = Some(Default::default());
            pod.status.iter_mut().for_each(|status| {
                status.pod_ip = Some("192.168.0.1".to_string());
                status.phase = Some(PodWatcher::PENDING.to_string());
            });
            let event = WatchEvent::Added(pod.clone());
            PodWatcher::handle_pod_event(&port, event, &change_tx, &mut pending_pods).await;

            pod.status.iter_mut().for_each(|status| {
                status.phase = Some(PodWatcher::RUNNING.to_string());
            });

            tokio::time::sleep(Duration::from_millis(10)).await;
            assert_eq!(pending_pods.len(), 1);

            let event = WatchEvent::Modified(pod.clone());
            PodWatcher::handle_pod_event(&port, event, &change_tx, &mut pending_pods).await;

            let e = tokio::time::timeout(Duration::from_millis(20), change_rx.recv())
                .await
                .expect("msg");
            assert!(e.is_some());
            let e = e.unwrap();

            match e {
                Change::Insert(k, v) => {
                    assert_eq!(k, "pod-0".to_string());
                    assert_eq!(v.uri(), &Uri::from_static("http://192.168.0.1:8888"));
                    assert!(pending_pods.is_empty())
                }
                _ => panic!("unexpected change event"),
            }

            let event = WatchEvent::Deleted(pod.clone());
            PodWatcher::handle_pod_event(&port, event, &change_tx, &mut pending_pods).await;

            let e = tokio::time::timeout(Duration::from_millis(20), change_rx.recv())
                .await
                .expect("msg");
            assert!(e.is_some());
            let e = e.unwrap();

            match e {
                Change::Remove(k) => {
                    assert_eq!(k, "pod-0".to_string());
                    assert!(pending_pods.is_empty())
                }
                _ => panic!("unexpected change event"),
            }
        }
    }
}

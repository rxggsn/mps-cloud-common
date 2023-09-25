use crate::utils::look_up;
use k8s_openapi::api::core::v1::Endpoints;
use kube::Api;
use std::str::FromStr;
use tokio::io;
use tonic::transport;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Target {
    Http(String),
    Service {
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
    Unknown,
}

impl Target {
    pub async fn connect(&self) -> Result<transport::Channel, TargetError> {
        match self {
            Target::Http(endpoint) => transport::Endpoint::new(endpoint.clone())
                .map_err(|err| TargetError::Transport(err))?
                .connect()
                .await
                .map_err(|err| TargetError::Transport(err)),
            Target::Service {
                namespace,
                service_name,
                port,
            } => {
                let k8s = kube::Client::try_default()
                    .await
                    .map_err(|err| TargetError::K8S(err))?;

                let api = Api::<Endpoints>::namespaced(k8s.clone(), &namespace);
                let endpoints = api
                    .get(&service_name)
                    .await
                    .map_err(|err| TargetError::K8S(err))?;
                let urls = endpoints
                    .subsets
                    .into_iter()
                    .map(|subsets| {
                        subsets
                            .into_iter()
                            .map(|subset| {
                                subset
                                    .addresses
                                    .into_iter()
                                    .map(|addresses| {
                                        addresses.into_iter().map(|addr| {
                                            format!("http://{}:{}", addr.ip, port.unwrap_or(80))
                                        })
                                    })
                                    .flatten()
                                    .collect::<Vec<_>>()
                            })
                            .flatten()
                            .collect::<Vec<_>>()
                    })
                    .flatten()
                    .collect::<Vec<_>>();

                let mut endpoints = vec![];

                for url in urls {
                    let endpoint = transport::Endpoint::from_str(&url)
                        .map_err(|err| TargetError::Transport(err))?;
                    endpoints.push(endpoint);
                }

                if endpoints.is_empty() {
                    return Err(TargetError::Unavailable);
                }

                Ok(transport::Channel::balance_list(endpoints.into_iter()))
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
                    Some(endpoint) => transport::Endpoint::new(format!("http://{}", endpoint))
                        .map_err(|err| TargetError::Transport(err))?
                        .connect()
                        .await
                        .map_err(|err| TargetError::Transport(err)),
                    None => Err(TargetError::Unknown),
                }
            }
            Target::Unknown => Err(TargetError::Unknown),
        }
    }
}

pub fn parse_target(dst: &str) -> Target {
    if dst.starts_with("http://") {
        Target::Http(dst.to_string())
    } else if dst.starts_with("k8s://") {
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

                Target::Service {
                    namespace,
                    service_name,
                    port,
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
}

#[cfg(test)]
mod tests {
    use super::parse_target;

    #[test]
    fn test_parse_target() {
        let target = parse_target("http://service:80");
        assert_eq!(target, super::Target::Http("http://service:80".to_string()));

        {
            let target = parse_target("k8s://test/service:80");
            assert_eq!(
                target,
                super::Target::Service {
                    namespace: "test".to_string(),
                    service_name: "service".to_string(),
                    port: Some(80),
                }
            );

            let target = parse_target("k8s://test/service");
            assert_eq!(
                target,
                super::Target::Service {
                    namespace: "test".to_string(),
                    service_name: "service".to_string(),
                    port: None,
                }
            );

            let target = parse_target("k8s://test/service:8888");
            assert_eq!(
                target,
                super::Target::Service {
                    namespace: "test".to_string(),
                    service_name: "service".to_string(),
                    port: Some(8888),
                }
            );

            let target = parse_target("k8s:///service:8888");
            assert_eq!(
                target,
                super::Target::Service {
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
}

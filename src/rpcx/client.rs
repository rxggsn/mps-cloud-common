use std::sync::Arc;

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

use super::{lb::PodLoadBalancer, PodName};

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

// Mps cloud plans to support three kinds of control planes:
// 1. Single: an single channel, it may be one node's gRPC connection or a load balancer.
// 2. Cluster: multiple channels, which represents a statefulset.
// 3. Lease: lease also represents a statefulset, but it has a leader pod and follower pods in a lease duration.
// -----------------
// Routing Rules:
// 1. Single: request routing will be handled by gRPC channel or load balancer.
// 2. Cluster:
//     [1] if request has a host header, then it will be routed to the specified pod.
//     [2] otherwise it will be routed to the first available node chosen by load balancer.
//     [3] if ready pod number < total pods number / 2, request will be not ready for routing.
// 3. Lease: request will be routed to the leader pod.
#[derive(Clone, Debug)]
pub enum ControlPlane {
    Single(transport::Channel),
    Cluster { load_balancer: PodLoadBalancer },
}

impl Service<http::Request<BoxBody>> for ControlPlane {
    type Response = http::Response<Body>;
    type Error = transport::Error;
    type Future = ResponseFuture;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        match self {
            ControlPlane::Single(channel) => channel.poll_ready(cx),
            ControlPlane::Cluster { load_balancer, .. } => load_balancer.poll_ready(cx),
        }
    }

    fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
        match self {
            ControlPlane::Single(channel) => channel.call(req),
            ControlPlane::Cluster { load_balancer, .. } => match req.headers().get(HOST) {
                Some(host) => {
                    let podname = host.to_str().unwrap();
                    if let Some(pod) = load_balancer.get_mut(&podname.to_string()) {
                        pod.call(req)
                    } else {
                        panic!("pod {} is not ready in cluster", podname)
                    }
                }
                None => load_balancer.call(req),
            },
        }
    }
}

impl ControlPlane {
    pub fn new_cluster(change_rx: Receiver<Change<PodName, Endpoint>>) -> Self {
        ControlPlane::Cluster {
            load_balancer: PodLoadBalancer::new(change_rx),
        }
    }
}

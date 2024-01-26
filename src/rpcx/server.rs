use std::{convert::Infallible, fmt::Debug, net::SocketAddr, time::Duration};

use http::{Request, Response};
use hyper::Body;
use tonic::{body::BoxBody, transport::NamedService};
use tower_service::Service;

use crate::{rpcx::retrive_trace_id, GRPC_TRACE_ID, LOG_TRACE_ID};

use super::{Context, RpcRequest};

pub async fn create_server<S>(svc: S, addr: SocketAddr) -> Result<(), tonic::transport::Error>
where
    S: Service<Request<Body>, Response = Response<BoxBody>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    tonic::transport::Server::builder()
        .timeout(Duration::from_secs(30))
        .trace_fn(|request| match request.headers().get(GRPC_TRACE_ID) {
            Some(trace_id) => {
                tracing::info_span!(LOG_TRACE_ID, trace_id = trace_id.to_str().unwrap_or(""))
            }
            None => tracing::info_span!(LOG_TRACE_ID, trace_id = ""),
        })
        .add_service(svc)
        .serve(addr)
        .await
}

pub fn log_request<T>(request: RpcRequest<T>, path: &str) -> (Option<String>, T)
where
    T: Debug,
{
    let trace_id = retrive_trace_id(&request);
    let req = request.into_inner();
    tracing::info!("request - {:?}, method - {}", &req, path);
    (trace_id, req)
}

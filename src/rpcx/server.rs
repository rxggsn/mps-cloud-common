use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    net::SocketAddr,
    time::Duration,
};

use http::{Request, Response};
use hyper::Body;
use tonic::{body::BoxBody, transport::NamedService};
use tower_service::Service;

use crate::{rpcx::retrive_trace_id, GRPC_SPAN_ID, GRPC_TRACE_ID, LOG_SPAN_ID, LOG_TRACE_ID};

use super::RpcRequest;

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
        .trace_fn(|request| {
            match (
                request.headers().get(GRPC_TRACE_ID),
                request.headers().get(GRPC_SPAN_ID),
            ) {
                (Some(trace_id), Some(span_id)) => {
                    tracing::info_span!(
                        LOG_TRACE_ID,
                        trace_id = trace_id.to_str().unwrap_or(""),
                        LOG_SPAN_ID,
                        span_id = span_id.to_str().unwrap_or("")
                    )
                }
                (Some(trace_id), None) => {
                    tracing::info_span!(
                        LOG_TRACE_ID,
                        trace_id = trace_id.to_str().unwrap_or(""),
                        LOG_SPAN_ID,
                        span_id = ""
                    )
                }
                (None, Some(span_id)) => {
                    tracing::info_span!(
                        LOG_TRACE_ID,
                        trace_id = "",
                        LOG_SPAN_ID,
                        span_id = span_id.to_str().unwrap_or("")
                    )
                }
                (None, None) => {
                    tracing::info_span!(LOG_TRACE_ID, trace_id = "", LOG_SPAN_ID, span_id = "")
                }
            }
        })
        .add_service(svc)
        .serve(addr)
        .await
}

pub fn debug_request<T>(request: RpcRequest<T>, path: &str) -> (Option<String>, T)
where
    T: Debug,
{
    let trace_id = retrive_trace_id(&request);
    let req = request.into_inner();
    tracing::debug!("request - {:?}, method - {}", &req, path);
    (trace_id, req)
}

pub fn display_request<T>(request: RpcRequest<T>, path: &str) -> (Option<String>, T)
where
    T: Display,
{
    let trace_id = retrive_trace_id(&request);
    let req = request.into_inner();
    tracing::debug!("request - {}, method - {}", &req, path);
    (trace_id, req)
}
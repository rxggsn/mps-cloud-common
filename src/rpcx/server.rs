use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    net::SocketAddr,
    time::Duration,
};

use http::{Request, Response};
use hyper::Body;
use tonic::{body::BoxBody, transport::NamedService};
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tower_service::Service;

use crate::{GRPC_SPAN_ID, GRPC_TRACE_ID, LOG_SPAN_ID, LOG_TRACE_ID, rpcx::retrive_trace_id};

use super::RpcRequest;

#[derive(Debug, Clone, serde::Deserialize)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
    pub client_ca_cert_path: String,
}

const TRACE_FN: fn(&Request<()>) -> tracing::Span = |request| match (
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
};

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
        .trace_fn(TRACE_FN)
        .add_service(svc)
        .serve(addr)
        .await
}

pub async fn create_server_with_tls<S>(
    svc: S,
    addr: SocketAddr,
    tls: &TlsConfig,
) -> Result<(), tonic::transport::Error>
where
    S: Service<Request<Body>, Response = Response<BoxBody>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    let cert = std::fs::read_to_string(&tls.cert_path).expect("read cert failed");
    let key = std::fs::read_to_string(&tls.key_path).expect("read key failed");
    let server_identity = Identity::from_pem(cert, key);

    let client_ca_cert =
        std::fs::read_to_string(&tls.client_ca_cert_path).expect("read client ca cert failed");
    let client_ca_cert = Certificate::from_pem(client_ca_cert);

    let tls = ServerTlsConfig::new()
        .identity(server_identity)
        .client_ca_root(client_ca_cert);

    tonic::transport::Server::builder()
        .timeout(Duration::from_secs(30))
        .trace_fn(TRACE_FN)
        .tls_config(tls)?
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
    tracing::info!("request - {}, method - {}", &req, path);
    (trace_id, req)
}

pub fn info_request<T>(request: RpcRequest<T>, path: &str) -> (Option<String>, T)
where
    T: Debug,
{
    let trace_id = retrive_trace_id(&request);
    let req = request.into_inner();
    tracing::info!("request - {:?}, method - {}", &req, path);
    (trace_id, req)
}

use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_tungstenite::accept_async;
use tonic::async_trait;

use crate::LOG_TRACE_ID;
use crate::utils;

pub type WsStream = tokio_tungstenite::WebSocketStream<TcpStream>;

#[async_trait]
pub trait SocketService {
    async fn handle(
        &mut self,
        mut conn: TcpStream,
        trace_id: String,
        span: tracing::Span,
    ) -> std::io::Result<()>;
}

#[async_trait]
pub trait WsService {
    async fn handle(
        &mut self,
        ws_stream: WsStream,
        trace_id: String,
        span: tracing::Span,
    ) -> std::io::Result<()>;
}

#[derive(Clone, serde::Deserialize, Debug)]
pub struct ServerBuilder {
    pub port: i16,
}

pub struct Server<T> {
    listener: TcpListener,
    svc: T,
    port: i16,
}

impl<T> Server<T> {
    pub async fn bind(port: i16, svc: T) -> std::io::Result<Server<T>> {
        TcpListener::bind(format!("0.0.0.0:{}", port))
            .await
            .map(|listener| Self {
                listener,
                svc,
                port,
            })
    }
}

impl<T: SocketService> Server<T> {
    pub async fn start(mut self) {
        let this = &mut self;
        tracing::info!("Socket Service start at port [{}]", this.port);
        loop {
            let result = this.listener.accept().await;
            let trace_id = utils::new_trace_id();
            let span = tracing::info_span!(LOG_TRACE_ID, trace_id = trace_id.as_str());
            match result {
                Ok((conn, _)) => match this.svc.handle(conn, trace_id, span).await {
                    Ok(_) => {}
                    Err(err) => tracing::error!("{}", err),
                },
                Err(err) => tracing::error!("{}", err),
            }
        }
    }
}

impl<T: WsService> Server<T> {
    pub async fn start_ws(mut self) {
        let this = &mut self;
        tracing::info!("WebSocket Service start at port [{}]", this.port);

        loop {
            let result = this.listener.accept().await;
            let trace_id = utils::new_trace_id();
            let span = tracing::info_span!(LOG_TRACE_ID, trace_id = trace_id.as_str());
            match result {
                Ok((conn, _)) => match accept_async(conn).await {
                    Ok(ws_stream) => match this.svc.handle(ws_stream, trace_id, span).await {
                        Ok(_) => {}
                        Err(err) => tracing::error!("{}", err),
                    },
                    Err(_) => todo!(),
                },
                Err(err) => tracing::error!("{}", err),
            }
        }
    }
}

pub fn ws_err_to_io(err: tokio_tungstenite::tungstenite::Error) -> std::io::Error {
    match err {
        tokio_tungstenite::tungstenite::Error::Io(e) => e,
        tokio_tungstenite::tungstenite::Error::ConnectionClosed => {
            std::io::Error::new(std::io::ErrorKind::ConnectionAborted, err.to_string())
        }
        tokio_tungstenite::tungstenite::Error::AlreadyClosed => {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, err.to_string())
        }
        tokio_tungstenite::tungstenite::Error::Tls(tls_error) => std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("TLS Error: {}", tls_error),
        ),
        tokio_tungstenite::tungstenite::Error::Capacity(capacity_error) => std::io::Error::new(
            std::io::ErrorKind::ResourceBusy,
            format!("Capacity Error: {}", capacity_error),
        ),
        tokio_tungstenite::tungstenite::Error::Protocol(protocol_error) => std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("Protocol Error: {}", protocol_error),
        ),
        tokio_tungstenite::tungstenite::Error::WriteBufferFull(message) => std::io::Error::new(
            std::io::ErrorKind::QuotaExceeded,
            format!("Write Buffer Full: {}", message),
        ),
        tokio_tungstenite::tungstenite::Error::Utf8(msg) => std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Utf8 Error: {}", msg),
        ),
        tokio_tungstenite::tungstenite::Error::AttackAttempt => std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "WebSocket Attack Attempt Detected",
        ),
        tokio_tungstenite::tungstenite::Error::Url(url_error) => std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("URL Error: {}", url_error),
        ),
        tokio_tungstenite::tungstenite::Error::Http(response) => std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("HTTP Error: {}", response.status()),
        ),
        tokio_tungstenite::tungstenite::Error::HttpFormat(error) => std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("HTTP Format Error: {}", error),
        ),
    }
}

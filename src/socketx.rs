use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tonic::async_trait;

use crate::utils;
use crate::LOG_TRACE_ID;

#[async_trait]
pub trait SocketService {
    async fn handle(
        &mut self,
        mut conn: TcpStream,
        trace_id: String,
        span: tracing::Span,
    ) -> std::io::Result<()>;
}

#[derive(Clone, serde::Deserialize, Debug)]
pub struct ServerBuilder {
    pub port: i16,
}

pub struct Server<T: SocketService> {
    listener: TcpListener,
    svc: T,
    port: i16,
}

impl<T: SocketService> Server<T> {
    pub async fn bind(port: i16, svc: T) -> std::io::Result<Server<T>> {
        TcpListener::bind(format!("0.0.0.0:{}", port))
            .await
            .map(|listener| Self {
                listener,
                svc,
                port,
            })
    }

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

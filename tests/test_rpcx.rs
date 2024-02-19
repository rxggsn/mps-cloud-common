use std::sync::Once;

mod proto;

static ONCE: Once = Once::new();

pub fn setup() {
    ONCE.call_once(|| {
        tracing_subscriber::fmt::init();
    });
}

mod mock_server {
    use std::{collections::BTreeMap, pin::Pin};

    use common::rpcx::{
        ok_response, server::create_server, stream::RpcStream, RpcRequest, RpcResponse,
    };
    use futures::StreamExt;
    use tokio::{
        sync::{
            mpsc::{self, Sender},
            RwLock,
        },
        task::JoinHandle,
    };
    use tokio_stream::Stream;
    use tonic::async_trait;

    use crate::proto::echo::{
        echo_service_server::{EchoService, EchoServiceServer},
        EchoRequest, EchoResponse, StreamEchoRequest,
    };
    pub const INIT_ECHO_PORT: u16 = 5050;

    pub struct MockEcho {
        pub nodename: String,
        pub streams: RwLock<BTreeMap<u32, Sender<Result<EchoResponse, tonic::Status>>>>,
    }

    #[async_trait]
    impl EchoService for MockEcho {
        type StreamEchoStream =
            Pin<Box<dyn Stream<Item = Result<EchoResponse, tonic::Status>> + Send>>;

        async fn echo(&self, request: RpcRequest<EchoRequest>) -> RpcResponse<EchoResponse> {
            let req = request.into_inner();
            let resp = EchoResponse {
                response: format!("hello {}, my name is {}", &req.message, &self.nodename),
            };
            if let Some(sender) = self.streams.read().await.get(&req.id) {
                sender.send(Ok(resp.clone())).await.map_err(|err| {
                    tracing::error!("{}", err);
                    tonic::Status::internal("sender closed")
                })?;
            }
            ok_response(resp)
        }

        async fn stream_echo(
            &self,
            request: RpcRequest<StreamEchoRequest>,
        ) -> RpcResponse<Self::StreamEchoStream> {
            let (tx, rx) = mpsc::channel(10);
            let req = request.into_inner();
            self.streams.write().await.insert(req.id, tx);

            ok_response(Box::pin(RpcStream::Local(rx)))
        }
    }

    pub struct MockEchoClusterInst {
        pub inner: JoinHandle<()>,
    }

    pub fn create_mock_echo(nodename: String) -> MockEcho {
        MockEcho {
            nodename,
            streams: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn create_mock_echo_cluster(nodename: Vec<String>) -> MockEchoClusterInst {
        let server_runtime = tokio_stream::iter(nodename)
            .enumerate()
            .map(|(idx, name)| {
                let mock = create_mock_echo(name);
                let port = INIT_ECHO_PORT + idx as u16;
                let addr = format!("0.0.0.0:{}", port).parse().unwrap();
                create_server(EchoServiceServer::new(mock), addr)
            })
            .for_each_concurrent(None, |server| async {
                match server.await {
                    Ok(_) => {}
                    Err(err) => tracing::error!("{}", err),
                }
            });

        MockEchoClusterInst {
            inner: tokio::spawn(server_runtime),
        }
    }
}

#[cfg(test)]
mod client {
    use std::{
        sync::{
            atomic::{AtomicI32, Ordering},
            Arc,
        },
        time::Duration,
    };

    use common::rpcx::{client::ControlPlane, RpcRequest};
    use futures::StreamExt;
    use http::header::HOST;
    use tokio::sync::mpsc;
    use tonic::transport::{self, Endpoint};
    use tower::discover::Change;

    use crate::{
        proto::echo::{echo_service_client::EchoServiceClient, EchoRequest, StreamEchoRequest},
        setup,
    };

    use super::mock_server::{create_mock_echo_cluster, INIT_ECHO_PORT};

    #[tokio::test]
    async fn test_cluster_control_plane_e2e_unary_server_stream() {
        setup();
        let nodenames = vec![
            "pod-0".to_string(),
            "pod-1".to_string(),
            "pod-2".to_string(),
        ];
        let _ = create_mock_echo_cluster(nodenames.clone());
        tokio::time::sleep(Duration::from_millis(10)).await;

        let (tx, rx) = mpsc::channel(10);
        let control_plane = ControlPlane::new_cluster(rx);

        let client = EchoServiceClient::new(control_plane);

        tokio_stream::iter(nodenames.clone())
            .enumerate()
            .map(|(idx, nodename)| (idx, nodename, tx.clone()))
            .for_each(|(idx, nodename, tx)| async move {
                tx.send(Change::Insert(
                    nodename.clone(),
                    Endpoint::new(format!("http://localhost:{}", INIT_ECHO_PORT + idx as u16))
                        .expect("msg"),
                ))
                .await
                .expect("msg")
            })
            .await;

        tokio::time::sleep(Duration::from_millis(10)).await;

        let stream_req = StreamEchoRequest { id: 1 };
        let total = Arc::new(AtomicI32::new(0));

        tokio_stream::iter(nodenames)
            .map(|nodename| (nodename, client.clone(), stream_req.clone(), total.clone()))
            .for_each_concurrent(None, |(nodename, mut client, req, total)| async move {
                let mut req = RpcRequest::new(req);
                req.metadata_mut().insert(
                    HOST.as_str(),
                    nodename.parse().expect("set host into metadata is failed"),
                );
                let stream = client
                    .stream_echo(req)
                    .await
                    .map(|resp| resp.into_inner())
                    .expect("msg");
                let value = format!("hello {}, my name is {}", "jason", &nodename);

                let r = tokio::join!(
                    async {
                        let mut req = EchoRequest::default();
                        req.id = 1;
                        req.message = "jason".to_string();
                        let mut request = RpcRequest::new(req);
                        request.metadata_mut().insert(
                            HOST.as_str(),
                            nodename.parse().expect("set host into metadata is failed"),
                        );

                        let resp = client.echo(request).await.expect("msg");
                        let resp = resp.into_inner();
                        assert_eq!(
                            format!("hello {}, my name is {}", "jason", &nodename),
                            resp.response
                        )
                    },
                    tokio::time::timeout(
                        Duration::from_millis(100),
                        stream
                            .map(|resp| (resp, value.clone(), total.clone()))
                            .for_each(|(msg, value, total)| async move {
                                total.fetch_add(1, Ordering::AcqRel);
                                let resp = msg.expect("msg");
                                // tracing::info!("stream resp: {}", &resp.response);
                                assert_eq!(&value, &resp.response);
                            })
                    )
                );
                assert!(r.1.is_err());
            })
            .await;
        assert_eq!(total.load(Ordering::Acquire), 3);
    }

    #[tokio::test]
    async fn test_lb_control_plane_e2e_unary() {
        setup();
        let nodenames = vec![
            "pod-0".to_string(),
            "pod-1".to_string(),
            "pod-2".to_string(),
        ];
        let _ = create_mock_echo_cluster(nodenames.clone());
        tokio::time::sleep(Duration::from_millis(10)).await;

        let (channel, tx) = transport::Channel::balance_channel(10);
        let control_plane = ControlPlane::Single(channel);

        let client = EchoServiceClient::new(control_plane);

        tokio_stream::iter(nodenames)
            .enumerate()
            .map(|(idx, nodename)| (idx, nodename, tx.clone()))
            .for_each_concurrent(None, |(idx, nodename, tx)| async move {
                tx.send(Change::Insert(
                    nodename.clone(),
                    Endpoint::new(format!("http://localhost:{}", INIT_ECHO_PORT + idx as u16))
                        .expect("msg"),
                ))
                .await
                .expect("msg")
            })
            .await;

        tokio::time::sleep(Duration::from_millis(20)).await;

        tokio_stream::iter(0..100)
            .map(|_| client.clone())
            .for_each_concurrent(None, |mut client| async move {
                let mut req = EchoRequest::default();
                req.message = "jason".to_string();

                let request = RpcRequest::new(req);

                let resp = client.echo(request).await.expect("msg");
                let resp = resp.into_inner();
                let candidate_resp = vec![
                    "hello jason, my name is pod-0",
                    "hello jason, my name is pod-1",
                    "hello jason, my name is pod-2",
                ];
                assert!(candidate_resp.contains(&resp.response.as_str()))
            })
            .await
    }
}

#[cfg(test)]
mod stream {
    use std::{
        sync::{
            atomic::{AtomicI32, Ordering},
            Arc,
        },
        time::Duration,
    };

    use common::rpcx::{client::ControlPlane, stream::RpcStream, RpcRequest};
    use futures::StreamExt as _;
    use tokio::sync::mpsc;
    use tonic::transport::Endpoint;

    use super::mock_server::{create_mock_echo_cluster, INIT_ECHO_PORT};
    use crate::{
        proto::echo::{echo_service_client::EchoServiceClient, EchoRequest, StreamEchoRequest},
        setup,
    };

    #[tokio::test]
    async fn test_local_rpc_stream() {
        let (tx, rx) = mpsc::channel(10);
        let stream = RpcStream::<i32>::Local(rx);

        let total = Arc::new(AtomicI32::new(0));
        let r = tokio::join!(
            tokio_stream::iter(0..10)
                .map(|i| (i, tx.clone()))
                .for_each(|(i, tx)| async move {
                    tx.send(Ok(i)).await.expect("msg");
                }),
            tokio::time::timeout(
                Duration::from_millis(500),
                stream
                    .enumerate()
                    .map(|(idx, message)| (idx, message, total.clone()))
                    .for_each(|(idx, message, total)| async move {
                        assert!(message.is_ok());
                        assert_eq!(message.unwrap(), idx as i32);
                        total.fetch_add(1, Ordering::Release);
                    })
            )
        );

        assert!(r.1.is_err());
        assert_eq!(total.load(Ordering::Acquire), 10);
    }

    #[tokio::test]
    async fn test_remote_rcp_stream() {
        setup();
        let nodenames = vec!["pod-0".to_string()];
        let _ = create_mock_echo_cluster(nodenames.clone());
        tokio::time::sleep(Duration::from_millis(10)).await;

        let channel = Endpoint::from_shared(format!("http://localhost:{}", INIT_ECHO_PORT))
            .expect("msg")
            .connect()
            .await
            .expect("msg");
        let control_plane = ControlPlane::Single(channel);

        let mut client = EchoServiceClient::new(control_plane);

        let stream_req = StreamEchoRequest { id: 1 };
        let req = RpcRequest::new(stream_req);
        let stream = client
            .stream_echo(req)
            .await
            .map(|resp| resp.into_inner())
            .expect("msg");
        let value = format!("hello {}, my name is {}", "jason", "pod-0");

        let stream = RpcStream::Remote(stream);

        let resp = client
            .echo(RpcRequest::new(EchoRequest {
                id: 1,
                message: "jason".to_string(),
            }))
            .await
            .expect("msg");

        let resp = resp.into_inner();
        assert_eq!(&value, &resp.response);

        let total = Arc::new(AtomicI32::new(0));
        let _ = tokio::time::timeout(
            Duration::from_millis(100),
            stream
                .map(|message| (message, value.clone(), total.clone()))
                .for_each(|(message, value, total)| async move {
                    let message = message.expect("msg");
                    total.fetch_add(1, Ordering::Release);
                    assert_eq!(&value, &message.response);
                }),
        )
        .await;

        assert_eq!(total.load(Ordering::Acquire), 1);
    }
}

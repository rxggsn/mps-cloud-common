use http::header::HOST;

use crate::GRPC_TRACE_ID;

pub type RpcRequest<T> = tonic::Request<T>;
pub type StreamRpcRequest<T> = tonic::Request<tonic::Streaming<T>>;
pub type RpcResponse<T> = Result<tonic::Response<T>, tonic::Status>;
pub type StreamRpcResponse<T> = Result<tonic::Response<tonic::codec::Streaming<T>>, tonic::Status>;

pub fn ok_response<T>(val: T) -> RpcResponse<T> {
    RpcResponse::Ok(tonic::Response::new(val))
}

pub fn err_response<T>(status: tonic::Status) -> RpcResponse<T> {
    RpcResponse::Err(status)
}

pub mod client;
pub mod err;
pub mod server;
pub mod target;

const MPS_STATUS_CODE: &str = "999";
const DEFAULT_BUFFER_SIZE: usize = 1024;
pub(crate) type PodName = String;
pub(crate) type PodUid = String;

pub fn retrive_trace_id<T>(req: &RpcRequest<T>) -> Option<String> {
    req.metadata()
        .get(GRPC_TRACE_ID)
        .map(|val| val.to_str().unwrap_or_default().to_string())
}

fn set_trace_id<T>(trace_id: &Option<String>, request: &mut RpcRequest<T>) {
    trace_id.iter().for_each(|trace_id| {
        request.metadata_mut().insert(
            GRPC_TRACE_ID,
            trace_id
                .parse()
                .expect("transform trace id into metadata value is failed"),
        );
    });
}

pub struct Context<'a> {
    pub trace_id: &'a Option<String>,
    pub podname: Option<&'a str>,
}

impl<'a> Clone for Context<'a> {
    fn clone(&self) -> Self {
        Self {
            trace_id: self.trace_id,
            podname: self.podname,
        }
    }
}

impl<'a> Context<'a> {
    pub fn new(trace_id: &'a Option<String>) -> Self {
        Self {
            trace_id,
            podname: None,
        }
    }

    pub fn set_metadata<T>(&self, request: &mut RpcRequest<T>) {
        set_trace_id(self.trace_id, request);
        self.podname.iter().for_each(|host| {
            request.metadata_mut().insert(
                HOST.as_str(),
                (*host).parse().expect("set host into metadata is failed"),
            );
        });
    }

    pub fn set_host(&mut self, host: &'a str) {
        self.podname = Some(host);
    }
}

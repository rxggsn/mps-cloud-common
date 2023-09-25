use std::fmt::Display;

use crate::error::{BizCode, BizError};

use super::MPS_STATUS_CODE;

#[derive(Debug, Clone)]
pub struct RpcError {
    pub biz_err: BizError,
    pub code: tonic::Code,
}

impl RpcError {
    pub fn grpc_status(&self) -> tonic::Status {
        let message = serde_json::to_string(&self.biz_err)
            .map_err(|err| {
                tracing::error!("{}", &err);
                err
            })
            .unwrap_or_default();
        let mut status = tonic::Status::new(self.code.clone(), message);
        status
            .metadata_mut()
            .append("x-rxdomain-code", MPS_STATUS_CODE.parse().unwrap());
        status
    }
}

impl Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "code: {}, biz_code: {}, error_type_code: {}, message: {}",
            self.code, self.biz_err.biz_code, self.biz_err.error_type_code, self.biz_err.message
        ))
    }
}

pub trait ToRpcError {
    fn to_rpc_error(&self) -> RpcError {
        RpcError {
            biz_err: BizError {
                biz_code: self.biz_code(),
                error_type_code: self.error_type_code(),
                message: self.message(),
            },
            code: self.status_code(),
        }
    }

    fn error_type_code(&self) -> i32;

    fn status_code(&self) -> tonic::Code;

    fn message(&self) -> String;

    fn biz_code(&self) -> BizCode;
}

pub fn is_mps_error(err: &tonic::Status) -> bool {
    err.metadata()
        .get("x-rxdomain-code")
        .map(|value| value == MPS_STATUS_CODE)
        .unwrap_or(false)
}

pub fn to_rpc_error(status: &tonic::Status) -> RpcError {
    let err = serde_json::from_str::<BizError>(status.message())
        .map(|biz_err| RpcError {
            biz_err,
            code: status.code(),
        })
        .map_err(|err| RpcError {
            biz_err: BizError {
                biz_code: 999,
                error_type_code: -1,
                message: format!("{}", err),
            },
            code: tonic::Code::Internal,
        });
    if err.is_ok() {
        err.unwrap()
    } else {
        err.unwrap_err()
    }
}

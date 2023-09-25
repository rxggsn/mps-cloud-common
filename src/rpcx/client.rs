use futures::{TryFuture, TryFutureExt};
use tonic::Code;

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

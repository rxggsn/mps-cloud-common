use std::time::Duration;

use bytes::{Buf, BufMut};
use futures::executor::block_on;

pub trait FileSystem {
    fn read<B: BufMut + Send>(
        &self,
        path: &str,
        buf: &mut B,
    ) -> impl Future<Output = std::io::Result<()>> + Send;
    fn write<B: Buf + Send>(
        &self,
        path: &str,
        data: B,
    ) -> impl Future<Output = std::io::Result<()>> + Send;
}

#[derive(serde::Deserialize, Clone, Debug)]
pub struct Region {
    pub endpoint: String,
    pub region: String,
}

#[derive(serde::Deserialize, Clone, Debug)]
pub struct S3Builder {
    bucket_name: String,
    region: Region,
    credentials: s3::creds::Credentials,
}

impl S3Builder {
    pub fn build(&self) -> std::io::Result<S3> {
        S3::new(
            &self.bucket_name,
            awsregion::Region::Custom {
                endpoint: self.region.endpoint.clone(),
                region: self.region.region.clone(),
            },
            self.credentials.clone(),
        )
    }
}

impl Default for S3Builder {
    fn default() -> Self {
        Self {
            bucket_name: "dhforce-ai".to_string(),
            region: Region {
                region: "home".to_string(),
                endpoint: "http://localhost:4000".to_string(),
            },
            credentials: s3::creds::Credentials::anonymous()
                .expect("Failed to create anonymous credentials"),
        }
    }
}

#[derive(Clone)]
pub struct S3 {
    bucket: s3::Bucket,
}

unsafe impl Send for S3 {}
unsafe impl Sync for S3 {}

impl S3 {
    fn new(
        bucket_name: &str,
        region: s3::Region,
        credentials: s3::creds::Credentials,
    ) -> std::io::Result<Self> {
        let bucket = s3::Bucket::new(bucket_name, region, credentials)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        Ok(Self {
            bucket: *bucket
                .with_request_timeout(Duration::from_secs(3))
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?,
        })
    }
}
impl FileSystem for S3 {
    fn read<B: BufMut + Send>(
        &self,
        path: &str,
        buf: &mut B,
    ) -> impl Future<Output = std::io::Result<()>> + Send {
        async move {
            let response_data = self
                .bucket
                .get_object(path)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            if response_data.status_code() != 200 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "S3 GET {} returned response {}",
                        path,
                        response_data
                            .to_string()
                            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?
                    ),
                ));
            }
            buf.put_slice(&response_data.into_bytes());
            Ok(())
        }
    }

    fn write<B: Buf + Send>(
        &self,
        path: &str,
        mut data: B,
    ) -> impl Future<Output = std::io::Result<()>> + Send {
        // while data.has_remaining() {
        //     let chunk = data.chunk();
        //     bytes.extend_from_slice(chunk);
        //     let len = chunk.len();
        //     data.advance(len);
        // }
        async move {
            let response_data = self
                .bucket
                .put_object(path, data.chunk())
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            if response_data.status_code() >= 300 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "S3 PUT {} returned {}",
                        path,
                        response_data
                            .to_string()
                            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?
                    ),
                ));
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::fsx::{FileSystem as _, Region, S3Builder};

    #[tokio::test]
    async fn test_put_object() {
        let builder = S3Builder {
            bucket_name: "rxdomain-dev".to_string(),
            region: Region {
                region: "cn-hangzhou".to_string(),
                endpoint: "oss-cn-hangzhou.aliyuncs.com".to_string(),
            },
            credentials: s3::creds::Credentials::new(
                option_env!("ALIYUN_ACCESS_KEY_ID").map(|s| s.to_string()),
                option_env!("ALIYUN_ACCESS_KEY_SECRET").map(|s| s.to_string()),
                None,
                None,
                None,
            )
            .expect("msg"),
        };

        let s3driver = builder.build().expect("msg");

        s3driver
            .write("test/path/test.txt", "Hello, S3!".as_bytes())
            .await
            .expect("Failed to write object");
    }
}

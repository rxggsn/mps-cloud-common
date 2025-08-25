use bytes::{Buf, BufMut};

pub trait FileSystem {
    fn read<B: BufMut>(&self, path: &str, buf: &mut B) -> std::io::Result<()>;
    fn write<B: Buf>(&self, path: &str, data: B) -> std::io::Result<()>;
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

impl S3 {
    fn new(
        bucket_name: &str,
        region: s3::Region,
        credentials: s3::creds::Credentials,
    ) -> std::io::Result<Self> {
        let bucket = s3::Bucket::new(bucket_name, region, credentials)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        Ok(Self { bucket: *bucket })
    }
}
impl FileSystem for S3 {
    fn read<B: BufMut>(&self, path: &str, buf: &mut B) -> std::io::Result<()> {
        let response_data = self
            .bucket
            .get_object_blocking(path)
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

    fn write<B: Buf>(&self, path: &str, mut data: B) -> std::io::Result<()> {
        // while data.has_remaining() {
        //     let chunk = data.chunk();
        //     bytes.extend_from_slice(chunk);
        //     let len = chunk.len();
        //     data.advance(len);
        // }
        let response_data = self
            .bucket
            .put_object_blocking(path, data.chunk())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        if !(response_data.status_code() >= 300) {
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

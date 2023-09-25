use std::fmt::Display;

use elasticsearch::{
    auth::Credentials,
    http::{
        transport::{SingleNodeConnectionPool, Transport, TransportBuilder},
        StatusCode, Url,
    },
    indices::IndicesDeleteParts,
    SearchParts,
};
use serde::de::DeserializeOwned;

pub struct Elastic {
    inner: elasticsearch::Elasticsearch,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ElasticBuilder {
    pub hostname: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub schema: Option<String>,
}

impl ElasticBuilder {
    pub fn build(&self) -> Elastic {
        let addr = format!(
            "{}://{}:{}",
            self.schema.clone().unwrap_or("http".to_string()),
            self.hostname,
            self.port
        );
        let url = Url::parse(addr.as_str()).expect("failed to parse elastic url");

        let transport = if self.has_auth() {
            tracing::info!("setting up elastic auth");
            TransportBuilder::new(SingleNodeConnectionPool::new(url))
                .auth(Credentials::Basic(
                    self.username.clone().unwrap_or_default(),
                    self.password.clone().unwrap_or_default(),
                ))
                .build()
                .expect("failed to create elastic transport")
        } else {
            TransportBuilder::new(SingleNodeConnectionPool::new(url))
                .build()
                .expect("failed to create elastic transport")
        };
        tracing::info!("elastic transport created [{:?}]", &transport);
        Elastic::new(transport)
    }

    fn has_auth(&self) -> bool {
        self.username.is_some() && self.password.is_some()
    }
}

impl Elastic {
    pub fn new(transport: Transport) -> Self {
        Self {
            inner: elasticsearch::Elasticsearch::new(transport),
        }
    }

    pub async fn index<T: serde::Serialize>(
        &self,
        index: &str,
        body: T,
    ) -> Result<(), ElasticError> {
        self.inner
            .index(elasticsearch::IndexParts::Index(index))
            .body(body)
            .send()
            .await
            .map_err(|err| ElasticError {
                code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                msg: format!("failed to insert into elastic: {}", err),
            })
            .and_then(|resp| {
                if !resp.status_code().is_success() {
                    let msg = format!("failed to insert into elastic: {:?}", resp);
                    Err(ElasticError {
                        code: resp.status_code().as_u16(),
                        msg,
                    })
                } else {
                    Ok(())
                }
            })
    }

    pub async fn query<T: DeserializeOwned>(
        &self,
        index: &str,
        query: serde_json::Value,
    ) -> Result<Vec<T>, ElasticError> {
        match self
            .inner
            .search(SearchParts::Index(&[index]))
            .body(query)
            .send()
            .await
            .map_err(|err| ElasticError {
                code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                msg: format!("failed to query elastic: {}", err),
            }) {
            Ok(resp) => {
                if !resp.status_code().is_success() {
                    let msg = format!("failed to query elastic: {:?}", resp);
                    Err(ElasticError {
                        code: resp.status_code().as_u16(),
                        msg,
                    })
                } else {
                    resp.json::<ElastiQueryResult<T>>()
                        .await
                        .map_err(|err| ElasticError {
                            code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                            msg: format!("failed to parse elastic response: {}", err),
                        })
                        .map(|r| {
                            r.hits
                                .hits
                                .into_iter()
                                .map(|hit| hit._source)
                                .collect::<Vec<T>>()
                        })
                }
            }
            Err(err) => Err(err),
        }
    }

    pub async fn delete_index(&self, index: &str) -> Result<(), ElasticError> {
        self.inner
            .indices()
            .delete(IndicesDeleteParts::Index(&[index]))
            .send()
            .await
            .map_err(|err| ElasticError {
                code: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                msg: format!("failed to delete index from elastic: {}", err),
            })
            .and_then(|resp| {
                if !resp.status_code().is_success() {
                    let msg = format!("failed to insert into elastic: {:?}", resp);
                    Err(ElasticError {
                        code: resp.status_code().as_u16(),
                        msg,
                    })
                } else {
                    Ok(())
                }
            })
    }
}

#[derive(Debug)]
pub struct ElasticError {
    pub code: u16,
    pub msg: String,
}

impl Display for ElasticError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "elastic request error, code: {}, msg: {}",
            self.code, self.msg
        ))
    }
}

#[derive(serde::Deserialize)]
pub struct ElastiQueryResult<T> {
    pub _shards: Shards,
    pub hits: ElastiQueryHits<T>,
}

#[derive(serde::Deserialize)]
pub struct Shards {
    pub total: u64,
    pub successful: u64,
    pub skipped: u64,
    pub failed: u64,
}

#[derive(serde::Deserialize)]
pub struct ElastiQueryHits<T> {
    pub total: QueryTotal,
    pub hits: Vec<QueryHit<T>>,
}

#[derive(serde::Deserialize)]
pub struct QueryHit<T> {
    pub _index: String,
    pub _type: String,
    pub _id: String,
    pub _score: f64,
    pub _source: T,
}

#[derive(serde::Deserialize)]
pub struct QueryTotal {
    pub value: u64,
    pub relation: String,
}

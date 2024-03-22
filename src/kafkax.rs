use std::time::Duration;

use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    producer::{FutureRecord, Producer},
    Message,
};

const QUEUE_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct KafkaProducer {
    inner: rdkafka::producer::FutureProducer,
}

impl rdkafka::config::FromClientConfig for KafkaProducer {
    fn from_config(conf: &rdkafka::ClientConfig) -> rdkafka::error::KafkaResult<Self> {
        conf.create::<rdkafka::producer::FutureProducer>()
            .map(|producer| KafkaProducer { inner: producer })
    }
}

impl Drop for KafkaProducer {
    fn drop(&mut self) {
        match self.flush() {
            Ok(_) => tracing::info!("flush all kafka events success"),
            Err(err) => tracing::error!("flush kafka events failed: {}", err),
        }

        drop(&mut self.inner);
    }
}

#[derive(serde::Deserialize, Clone, Debug)]
pub struct KafkaProducerBuilder {
    pub brokers: String,
}

impl KafkaProducerBuilder {
    pub fn build(&self) -> KafkaProducer {
        let mut conf = rdkafka::ClientConfig::new();
        conf.set("bootstrap.servers", self.brokers.as_str());

        match conf.create() {
            Ok(producer) => producer,
            Err(err) => panic!("{}", err),
        }
    }
}

impl KafkaProducer {
    pub async fn produce<T: KafkaMessage>(&self, message: &T) -> rdkafka::error::KafkaResult<()> {
        let payload: &[u8] = &message.payload();
        let key: &[u8] = &message.key();
        let topic = message.topic();

        if payload.is_empty() {
            tracing::warn!("kafka message payload is empty");
            return Ok(());
        }

        tracing::debug!(
            "send kafka event, topic: [{}], key: [{:x}], value: [{}]",
            &topic,
            bytes::Bytes::copy_from_slice(key),
            String::from_utf8_lossy(payload).to_string()
        );
        let record = FutureRecord::to(topic.as_str())
            .partition(message.partition().abs())
            .payload(payload)
            .key(key);
        self.inner
            .send(record, QUEUE_TIMEOUT)
            .await
            .map(|_| {})
            .map_err(|(err, _)| err)
    }

    pub fn flush(&self) -> rdkafka::error::KafkaResult<()> {
        self.inner.flush(QUEUE_TIMEOUT)
    }
}

#[derive(serde::Deserialize, Clone, Debug)]
pub struct KafkaConsumerBuilder {
    pub brokers: String,
    pub group_id: Option<String>,
    pub topics: String,
    pub partition: Option<i32>,
    pub auto_offset_reset: Option<String>,
    pub max_topic_metadata_propagation_ms: Option<i32>,
}

impl KafkaConsumerBuilder {
    pub fn build(&self) -> KafkaConsumer {
        let mut conf = rdkafka::ClientConfig::new();
        conf.set("bootstrap.servers", self.brokers.as_str());
        conf.set(
            "group.id",
            self.group_id.as_ref().unwrap_or(&"default".to_string()),
        );
        conf.set("enable.auto.commit", "true");
        conf.set("enable.auto.offset.store", "false");
        conf.set("enable.partition.eof", "false");
        conf.set("session.timeout.ms", "6000");
        match self.auto_offset_reset.as_ref() {
            Some(auto_offset_reset) => {
                conf.set("auto.offset.reset", auto_offset_reset);
            }
            None => {}
        }
        match self.max_topic_metadata_propagation_ms {
            Some(ms) => {
                conf.set("topic.metadata.propagation.max.ms", ms.to_string());
            }
            None => {}
        };

        let consumer: StreamConsumer = conf.create().expect("Consumer creation failed");

        consumer
            .subscribe(&self.topics.split(",").collect::<Vec<_>>())
            .expect("Can't subscribe to specified topics");

        KafkaConsumer { inner: consumer }
    }
}

pub struct KafkaConsumer {
    inner: StreamConsumer,
}

impl KafkaConsumer {
    pub async fn next<T: KafkaMessage + From<(Vec<u8>, Vec<u8>)>>(
        &self,
    ) -> Result<Option<T>, KafkaError> {
        match self.inner.recv().await.map(|r| {
            let message = r.detach();
            let key = message.key().map(|k| k.to_vec()).unwrap_or_default();
            tracing::debug!(
                "fetch kafka event, topic: [{}], key [{:x}], value [{}]",
                message.topic(),
                bytes::Bytes::copy_from_slice(&key),
                message
                    .payload()
                    .as_ref()
                    .map(|v| String::from_utf8_lossy(v))
                    .unwrap_or_default()
            );

            message.payload().map(|data| T::from((key, data.to_vec())))
        }) {
            Ok(message) => Ok(message),
            Err(err) => match err {
                KafkaError::NoMessageReceived => Ok(None),
                _ => Err(err),
            },
        }
    }
}

pub trait KafkaMessage {
    fn key(&self) -> bytes::Bytes;

    fn payload(&self) -> bytes::BytesMut;

    fn topic(&self) -> String;

    fn partition(&self) -> i32;
}

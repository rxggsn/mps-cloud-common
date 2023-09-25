use std::time::Duration;

use common::kafkax::{KafkaConsumerBuilder, KafkaMessage, KafkaProducerBuilder};

#[tokio::test]
async fn test_kafka_producer() {
    let builder = KafkaProducerBuilder {
        brokers: "localhost:9092".to_string(),
    };

    let producer = builder.build();

    let r = producer
        .produce(&TestKafkaMessage {
            key: "key".to_string(),
            value: vec![1, 2, 3],
        })
        .await;
    if r.is_err() {
        panic!("{}", r.unwrap_err())
    } else {
        assert!(r.is_ok());
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    let builder = KafkaConsumerBuilder {
        brokers: "localhost:9092".to_string(),
        group_id: Some("test".to_string()),
        topics: "cicd".to_string(),
        partition: None,
        auto_offset_reset: Some("beginning".to_string()),
        max_topic_metadata_propagation_ms: None,
    };

    let consumer = builder.build();
    let r = tokio::time::timeout(Duration::from_secs(3), consumer.next::<TestKafkaMessage>()).await;
    if r.is_err() {
        panic!("{}", r.unwrap_err())
    }

    let r = r.unwrap();

    assert!(r.is_ok());

    let r = r.unwrap();
    assert!(r.is_some());

    let msg = r.unwrap();

    assert_eq!(msg.key, "key".to_string());
    assert_eq!(msg.value, vec![1, 2, 3]);
}

#[derive(Debug)]
struct TestKafkaMessage {
    key: String,
    value: Vec<u8>,
}

impl KafkaMessage for TestKafkaMessage {
    fn key(&self) -> bytes::Bytes {
        bytes::Bytes::copy_from_slice(self.key.as_bytes())
    }

    fn payload(&self) -> bytes::BytesMut {
        bytes::BytesMut::from_iter(self.value.clone())
    }

    fn topic(&self) -> String {
        "cicd".to_string()
    }

    fn partition(&self) -> i32 {
        0
    }
}

impl From<(Vec<u8>, Vec<u8>)> for TestKafkaMessage {
    fn from(value: (Vec<u8>, Vec<u8>)) -> Self {
        Self {
            key: String::from_utf8(value.0).expect("msg"),
            value: value.1,
        }
    }
}

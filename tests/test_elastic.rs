use common::elasticx::ElasticBuilder;
use serde_json::json;

#[tokio::test]
async fn test_elasticx_crud() {
    let builder = ElasticBuilder {
        hostname: "localhost".to_string(),
        port: 9200,
        username: None,
        password: None,
        schema: None,
    };
    let cli = builder.build();
    let r = cli
        .index(
            "mps",
            TestElasticDocument {
                id: "1".to_string(),
                name: "test".to_string(),
            },
        )
        .await;
    assert!(r.is_ok());
    let query = json!({
        "query": {
            "match_all": {}
        }
    });

    let _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let r = cli.query::<TestElasticDocument>("mps", query).await;
    let v = r.expect("msg");
    assert!(v.len() > 0);
    v.into_iter().for_each(|doc| {
        assert_eq!(doc.id, "1".to_string());
        assert_eq!(doc.name, "test".to_string());
    });

    cli.delete_index("mps").await.expect("msg");
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct TestElasticDocument {
    pub id: String,
    pub name: String,
}

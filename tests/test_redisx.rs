use common::redisx;

#[tokio::test]
async fn test_redisx_hash_ops() {
    let conf = redisx::RedisConf {
        host: "localhost:6379".to_string(),
        tls: false,
        db: 0,
        connect_timeout: 3,
        username: None,
        password: None,
    };
    let mut redis = conf.create().await;

    {
        let r = redis.hset("test", 1, "val_1").await;
        r.expect("hset failed");
        let r = redis.hset("test", 2, "val_2").await;
        assert!(r.is_ok());
        let r = redis.hset("test", 3, "val_3").await;
        assert!(r.is_ok());
    }

    {
        let r = redis.hget::<&str, u64, String>("test", 1).await;
        assert!(r.is_ok());

        let val = r.unwrap();
        assert_eq!(val, "val_1");

        let r = redis.hget::<&str, u64, String>("test", 2).await;
        assert!(r.is_ok());

        let val = r.unwrap();
        assert_eq!(val, "val_2");

        let r = redis.hget::<&str, u64, String>("test", 3).await;
        assert!(r.is_ok());

        let val = r.unwrap();
        assert_eq!(val, "val_3");
    }

    {
        let r = redis.hscan::<&str, u64, String>("test").await;
        assert!(r.is_ok());
        let mut iter = r.unwrap();
        for x in 1..=3 {
            let item = iter.next_item().await;
            assert!(item.is_some());

            let (k, v) = item.unwrap();
            assert_eq!(k, x);
            assert_eq!(v, format!("val_{}", x));
        }

        let item = iter.next_item().await;
        assert!(item.is_none());
    }
}

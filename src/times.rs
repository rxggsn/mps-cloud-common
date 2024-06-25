pub const YEAR_SECONDS: u32 = 31536000;

// now timestamp, in millis
pub fn now_timestamp() -> i64 {
    chrono::Local::now().timestamp_millis()
}

pub fn now_timestamp_secs() -> i64 {
    chrono::Local::now().timestamp()
}

#[cfg(test)]
mod tests {

    use crate::times::{now_timestamp, now_timestamp_secs};

    #[test]
    fn test_now_timestamp() {
        let now = now_timestamp();
        assert!(now > 0);
    }

    #[test]
    fn test_now_timestamp_secs() {
        let now = now_timestamp_secs();
        println!("now: {}", now);
        assert!(now > 0);
    }
}

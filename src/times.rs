pub const YEAR_SECONDS: u32 = 31536000;

// now timestamp, in millis
pub fn now_timestamp() -> i64 {
    chrono::Local::now().timestamp_millis()
}

pub fn now_timestamp_secs() -> i64 {
    chrono::Local::now().timestamp()
}

pub fn today_start_timestamp_secs() -> i64 {
    let now = chrono::Local::now();
    now.date_naive()
        .and_hms_milli_opt(0, 0, 0, 0)
        .map(|datetime| datetime.timestamp())
        .unwrap_or_default()
}

pub fn today_end_timestamp_secs() -> i64 {
    let now = chrono::Local::now();
    now.date_naive()
        .and_hms_milli_opt(23, 59, 59, 999)
        .map(|datetime| datetime.timestamp())
        .unwrap_or_default()
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

    #[test]
    fn test_today_start_timestamp_secs() {
        let now = now_timestamp_secs();
        let today_start = crate::times::today_start_timestamp_secs();
        println!("now: {}, today_start: {}", now, today_start);
        assert!(today_start > 0);
    }

    #[test]
    fn test_today_end_timestamp_secs() {
        let now = now_timestamp_secs();
        let today_end = crate::times::today_end_timestamp_secs();
        println!("now: {}, today_end: {}", now, today_end);
        assert!(today_end > 0);
    }
}

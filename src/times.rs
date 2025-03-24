pub const YEAR_SECONDS: u32 = 31536000;
pub const CHRONO_DATE_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";
pub const CHRONO_DATE_FORMAT: &str = "%Y-%m-%d";
pub const CHRONO_TIME_FORMAT: &str = "%H:%M:%S";

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

pub fn to_local_datetime(timestamp: i64) -> chrono::DateTime<chrono::Local> {
    let secs = timestamp / 1000;
    let nanos = (timestamp % 1000) * 1_000_000;
    let naive_date_time = chrono::NaiveDateTime::from_timestamp_opt(secs, nanos as u32).unwrap();
    chrono::DateTime::from_utc(naive_date_time, *chrono::Local::now().offset())
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

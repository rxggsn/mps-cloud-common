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

pub fn get_all_weekdays(
    weekdays: &[i32],
    start_date: chrono::NaiveDate,
    end_date: chrono::NaiveDate,
) -> Vec<chrono::NaiveDate> {
    use chrono::Datelike;
    let mut dates = Vec::new();
    if weekdays.is_empty() {
        return dates;
    }
    let mut current_date = start_date;
    while current_date <= end_date {
        if weekdays.contains(&(current_date.weekday().number_from_monday() as i32)) {
            dates.push(current_date);
        }
        if current_date.weekday().number_from_monday() == weekdays[weekdays.len() - 1] as u32 {
            // If it's the last weekday, jump to next week
            current_date = current_date
                .checked_add_days(chrono::Days::new(
                    (chrono::Weekday::Sun.number_from_monday() as i32
                        - weekdays[weekdays.len() - 1]
                        + weekdays[0]) as u64,
                ))
                .or_else(|| current_date.succ_opt())
                .expect("");
        } else {
            current_date = current_date.succ_opt().expect("");
        }
    }
    dates
}

#[cfg(test)]
mod tests {

    use super::get_all_weekdays;
    use crate::times::{now_timestamp, now_timestamp_secs};
    use chrono::NaiveDate;

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

    #[test]
    fn test_get_all_weekdays() {
        tracing_subscriber::fmt::init();
        // Monday to Sunday: 1 to 7
        let weekdays = vec![1, 3, 5]; // Monday, Wednesday, Friday
        let start_date = NaiveDate::from_ymd_opt(2024, 6, 3).unwrap(); // Monday
        let end_date = NaiveDate::from_ymd_opt(2024, 6, 16).unwrap(); // Sunday

        let result = get_all_weekdays(&weekdays, start_date, end_date);

        let expected = vec![
            NaiveDate::from_ymd_opt(2024, 6, 3).unwrap(),  // Monday
            NaiveDate::from_ymd_opt(2024, 6, 5).unwrap(),  // Wednesday
            NaiveDate::from_ymd_opt(2024, 6, 7).unwrap(),  // Friday
            NaiveDate::from_ymd_opt(2024, 6, 10).unwrap(), // Monday
            NaiveDate::from_ymd_opt(2024, 6, 12).unwrap(), // Wednesday
            NaiveDate::from_ymd_opt(2024, 6, 14).unwrap(), // Friday
        ];

        assert_eq!(result, expected);

        // Test with empty weekdays
        let result = get_all_weekdays(&[], start_date, end_date);
        assert!(result.is_empty());

        // Test with all weekdays
        let weekdays = vec![1, 2, 3, 4, 5, 6, 7];
        let result = get_all_weekdays(&weekdays, start_date, end_date);
        tracing::info!("result: {:?}", result);
        let expected: Vec<_> = (0..=13)
            .map(|i| {
                start_date
                    .checked_add_signed(chrono::Duration::days(i))
                    .unwrap()
            })
            .collect();
        assert_eq!(result, expected);
    }
}

//! F1 / F2 / F3 timestamps → epoch milliseconds. Howard Hinnant civil dates.

pub const SECOND: i64 = 1_000;
pub const MINUTE: i64 = 60 * SECOND;
pub const HOUR: i64 = 60 * MINUTE;
pub const DAY: i64 = 24 * HOUR;

const DAYS_IN_MONTH: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

pub fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year - 719_468
}

fn month_days(year: i64, month: i64) -> i64 {
    if month == 2 && is_leap_year(year) {
        29
    } else {
        DAYS_IN_MONTH[(month - 1) as usize]
    }
}

fn digit(b: u8) -> Option<i64> {
    if b.is_ascii_digit() {
        Some((b - b'0') as i64)
    } else {
        None
    }
}

fn digits2(s: &[u8], i: usize) -> Option<i64> {
    Some(digit(s[i])? * 10 + digit(s[i + 1])?)
}

fn digits4(s: &[u8], i: usize) -> Option<i64> {
    Some(
        digit(s[i])? * 1000
            + digit(s[i + 1])? * 100
            + digit(s[i + 2])? * 10
            + digit(s[i + 3])?,
    )
}

fn parse_f3(s: &[u8]) -> Option<i64> {
    if s.is_empty() || !s.iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut n: i128 = 0;
    for &b in s {
        n = n * 10 + (b - b'0') as i128;
        if n > i64::MAX as i128 {
            return Some(i64::MAX);
        }
    }
    Some(n as i64)
}

fn parse_civil_local(s: &[u8]) -> Option<(i64, i64, i64, i64, i64, i64)> {
    // YYYY-MM-DDTHH:MM:SS  length 19
    if s.len() < 19 {
        return None;
    }
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = digits4(s, 0)?;
    let month = digits2(s, 5)?;
    let day = digits2(s, 8)?;
    let hour = digits2(s, 11)?;
    let minute = digits2(s, 14)?;
    let second = digits2(s, 17)?;
    if !(1..=12).contains(&month) {
        return None;
    }
    if day < 1 || day > month_days(year, month) {
        return None;
    }
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=59).contains(&second) {
        return None;
    }
    Some((year, month, day, hour, minute, second))
}

fn civil_to_utc_ms(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64) -> i64 {
    days_from_civil(year, month, day) * DAY + hour * HOUR + minute * MINUTE + second * SECOND
}

pub fn parse_ts(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.iter().all(|b| b.is_ascii_digit()) {
        return parse_f3(s);
    }
    let (year, month, day, hour, minute, second) = parse_civil_local(s)?;
    let local = civil_to_utc_ms(year, month, day, hour, minute, second);
    match s.len() {
        20 if s[19] == b'Z' => Some(local),
        25 if (s[19] == b'+' || s[19] == b'-') && s[22] == b':' => {
            let oh = digits2(s, 20)?;
            let om = digits2(s, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = oh * 60 + om;
            let off_ms = off * MINUTE;
            if s[19] == b'+' {
                Some(local - off_ms)
            } else {
                Some(local + off_ms)
            }
        }
        _ => None,
    }
}

pub fn parse_ts_str(s: &str) -> Option<i64> {
    parse_ts(s.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f1_known() {
        let ms = parse_ts_str("2026-10-03T23:58:10Z").unwrap();
        assert_eq!(ms, 1_791_071_890_000);
    }

    #[test]
    fn f2_equals_f1() {
        assert_eq!(
            parse_ts_str("2026-10-03T19:58:10-04:00"),
            parse_ts_str("2026-10-03T23:58:10Z")
        );
    }

    #[test]
    fn f3_leading_zeros() {
        assert_eq!(parse_ts_str("0001791071890000"), parse_ts_str("1791071890000"));
    }

    #[test]
    fn reject_lowercase_z() {
        assert!(parse_ts_str("2026-10-03T23:58:10z").is_none());
    }

    #[test]
    fn reject_fractional() {
        assert!(parse_ts_str("2026-10-03T23:58:10.000Z").is_none());
    }

    #[test]
    fn reject_offset_without_colon() {
        assert!(parse_ts_str("2026-10-03T23:58:10+0200").is_none());
    }

    #[test]
    fn reject_offset_24() {
        assert!(parse_ts_str("2026-10-03T23:58:10+24:00").is_none());
    }

    #[test]
    fn minus_zero_is_utc() {
        assert_eq!(
            parse_ts_str("2026-10-03T23:58:10-00:00"),
            parse_ts_str("2026-10-03T23:58:10Z")
        );
    }

    #[test]
    fn epoch_day() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(parse_ts_str("86400000"), Some(86_400_000));
    }

    #[test]
    fn reject_feb29_non_leap() {
        assert!(parse_ts_str("2026-02-29T00:00:00Z").is_none());
        assert!(parse_ts_str("2028-02-29T00:00:00Z").is_some());
    }
}

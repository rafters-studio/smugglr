//! Timestamp parsing and comparison for sync operations
//!
//! Handles multiple timestamp formats:
//! - Unix timestamps (seconds since epoch): "1704067200"
//! - ISO 8601: "2024-01-01T00:00:00Z"
//! - SQLite datetime: "2024-01-01 00:00:00"

use std::cmp::Ordering;
use std::fmt;

/// A parsed timestamp that supports multiple input formats.
///
/// Internally stores Unix timestamp (seconds since epoch) for consistent comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timestamp(i64);

/// Error returned when timestamp parsing fails
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseTimestampError {
    input: String,
    message: String,
}

impl fmt::Display for ParseTimestampError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to parse timestamp '{}': {}", self.input, self.message)
    }
}

impl std::error::Error for ParseTimestampError {}

impl Timestamp {
    /// Create a timestamp from a Unix timestamp (seconds since epoch)
    pub fn from_unix(secs: i64) -> Self {
        Self(secs)
    }

    /// Get the Unix timestamp value (seconds since epoch)
    pub fn as_unix(&self) -> i64 {
        self.0
    }

    /// Parse a timestamp from a string.
    ///
    /// Supports:
    /// - Unix timestamp: "1704067200"
    /// - ISO 8601: "2024-01-01T00:00:00Z" or "2024-01-01T00:00:00+00:00"
    /// - SQLite datetime: "2024-01-01 00:00:00"
    pub fn parse(s: &str) -> Result<Self, ParseTimestampError> {
        let s = s.trim();

        if s.is_empty() {
            return Err(ParseTimestampError {
                input: s.to_string(),
                message: "empty string".to_string(),
            });
        }

        // Try Unix timestamp first (all digits, possibly with leading minus)
        if s.chars().all(|c| c.is_ascii_digit()) ||
           (s.starts_with('-') && s[1..].chars().all(|c| c.is_ascii_digit())) {
            return s.parse::<i64>()
                .map(Self)
                .map_err(|_| ParseTimestampError {
                    input: s.to_string(),
                    message: "invalid Unix timestamp".to_string(),
                });
        }

        // Try ISO 8601 format: 2024-01-01T00:00:00Z or with timezone
        if s.contains('T') {
            return Self::parse_iso8601(s);
        }

        // Try SQLite datetime format: 2024-01-01 00:00:00
        if s.contains(' ') && s.contains('-') && s.contains(':') {
            return Self::parse_sqlite_datetime(s);
        }

        Err(ParseTimestampError {
            input: s.to_string(),
            message: "unrecognized timestamp format".to_string(),
        })
    }

    /// Parse ISO 8601 format
    fn parse_iso8601(s: &str) -> Result<Self, ParseTimestampError> {
        // Remove trailing Z or timezone
        let s = s.trim_end_matches('Z');
        let s = if let Some(idx) = s.rfind('+') {
            &s[..idx]
        } else if let Some(idx) = s.rfind('-') {
            // Check if this minus is part of timezone (after T) or date
            if s.matches('-').count() > 2 {
                &s[..idx]
            } else {
                s
            }
        } else {
            s
        };

        Self::parse_datetime_parts(s, 'T')
    }

    /// Parse SQLite datetime format: 2024-01-01 00:00:00
    fn parse_sqlite_datetime(s: &str) -> Result<Self, ParseTimestampError> {
        Self::parse_datetime_parts(s, ' ')
    }

    /// Parse datetime with configurable separator between date and time
    fn parse_datetime_parts(s: &str, sep: char) -> Result<Self, ParseTimestampError> {
        let parts: Vec<&str> = s.split(sep).collect();
        if parts.len() != 2 {
            return Err(ParseTimestampError {
                input: s.to_string(),
                message: format!("expected date{}time format", sep),
            });
        }

        let date_parts: Vec<&str> = parts[0].split('-').collect();
        if date_parts.len() != 3 {
            return Err(ParseTimestampError {
                input: s.to_string(),
                message: "invalid date format, expected YYYY-MM-DD".to_string(),
            });
        }

        let time_parts: Vec<&str> = parts[1].split(':').collect();
        if time_parts.len() < 2 {
            return Err(ParseTimestampError {
                input: s.to_string(),
                message: "invalid time format, expected HH:MM:SS".to_string(),
            });
        }

        let year: i32 = date_parts[0].parse().map_err(|_| ParseTimestampError {
            input: s.to_string(),
            message: "invalid year".to_string(),
        })?;
        let month: u32 = date_parts[1].parse().map_err(|_| ParseTimestampError {
            input: s.to_string(),
            message: "invalid month".to_string(),
        })?;
        let day: u32 = date_parts[2].parse().map_err(|_| ParseTimestampError {
            input: s.to_string(),
            message: "invalid day".to_string(),
        })?;

        let hour: u32 = time_parts[0].parse().map_err(|_| ParseTimestampError {
            input: s.to_string(),
            message: "invalid hour".to_string(),
        })?;
        let minute: u32 = time_parts[1].parse().map_err(|_| ParseTimestampError {
            input: s.to_string(),
            message: "invalid minute".to_string(),
        })?;
        // Handle fractional seconds by taking only the integer part
        let second: u32 = time_parts.get(2)
            .map(|s| s.split('.').next().unwrap_or("0"))
            .unwrap_or("0")
            .parse()
            .unwrap_or(0);

        // Validate ranges
        if !(1..=12).contains(&month) {
            return Err(ParseTimestampError {
                input: s.to_string(),
                message: "month out of range (1-12)".to_string(),
            });
        }
        if !(1..=31).contains(&day) {
            return Err(ParseTimestampError {
                input: s.to_string(),
                message: "day out of range (1-31)".to_string(),
            });
        }
        if hour > 23 || minute > 59 || second > 59 {
            return Err(ParseTimestampError {
                input: s.to_string(),
                message: "time component out of range".to_string(),
            });
        }

        // Convert to Unix timestamp (simplified, assumes UTC)
        // Days since epoch for each year
        let mut days: i64 = 0;
        for y in 1970..year {
            days += if is_leap_year(y) { 366 } else { 365 };
        }
        for y in (year..1970).rev() {
            days -= if is_leap_year(y) { 366 } else { 365 };
        }

        // Days for completed months in current year
        let month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        for m in 0..(month - 1) as usize {
            days += month_days[m] as i64;
            if m == 1 && is_leap_year(year) {
                days += 1;
            }
        }

        // Add days in current month (minus 1 since day 1 = 0 days elapsed)
        days += (day - 1) as i64;

        let secs = days * 86400 + hour as i64 * 3600 + minute as i64 * 60 + second as i64;
        Ok(Self(secs))
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_unix_timestamp() {
        let ts = Timestamp::parse("1704067200").unwrap();
        assert_eq!(ts.as_unix(), 1704067200);
    }

    #[test]
    fn parse_negative_unix_timestamp() {
        let ts = Timestamp::parse("-86400").unwrap();
        assert_eq!(ts.as_unix(), -86400);
    }

    #[test]
    fn parse_iso8601_with_z() {
        let ts = Timestamp::parse("2024-01-01T00:00:00Z").unwrap();
        assert_eq!(ts.as_unix(), 1704067200);
    }

    #[test]
    fn parse_iso8601_no_timezone() {
        let ts = Timestamp::parse("2024-01-01T00:00:00").unwrap();
        assert_eq!(ts.as_unix(), 1704067200);
    }

    #[test]
    fn parse_sqlite_datetime() {
        let ts = Timestamp::parse("2024-01-01 00:00:00").unwrap();
        assert_eq!(ts.as_unix(), 1704067200);
    }

    #[test]
    fn parse_with_fractional_seconds() {
        let ts = Timestamp::parse("2024-01-01T00:00:00.123Z").unwrap();
        assert_eq!(ts.as_unix(), 1704067200);
    }

    #[test]
    fn parse_empty_string_fails() {
        assert!(Timestamp::parse("").is_err());
        assert!(Timestamp::parse("   ").is_err());
    }

    #[test]
    fn parse_invalid_format_fails() {
        assert!(Timestamp::parse("not-a-timestamp").is_err());
        assert!(Timestamp::parse("2024/01/01").is_err());
    }

    #[test]
    fn compare_timestamps() {
        let earlier = Timestamp::parse("2024-01-01T00:00:00Z").unwrap();
        let later = Timestamp::parse("2024-01-02T00:00:00Z").unwrap();

        assert!(earlier < later);
        assert!(later > earlier);
        assert_eq!(earlier, earlier);
    }

    #[test]
    fn from_unix_and_back() {
        let ts = Timestamp::from_unix(1704067200);
        assert_eq!(ts.as_unix(), 1704067200);
    }

    #[test]
    fn display_shows_unix() {
        let ts = Timestamp::from_unix(1704067200);
        assert_eq!(format!("{}", ts), "1704067200");
    }

    #[test]
    fn parse_various_valid_formats() {
        // All should parse to the same timestamp
        let expected = 1704067200;

        assert_eq!(Timestamp::parse("1704067200").unwrap().as_unix(), expected);
        assert_eq!(Timestamp::parse("2024-01-01T00:00:00Z").unwrap().as_unix(), expected);
        assert_eq!(Timestamp::parse("2024-01-01 00:00:00").unwrap().as_unix(), expected);
    }
}

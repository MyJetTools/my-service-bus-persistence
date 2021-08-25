use crate::date_time::DateTimeAsMicroseconds;

pub trait BclToUnixMicroseconds {
    fn to_unix_microseconds(&self) -> Result<i64, String>;
    fn to_date_time(&self) -> Result<DateTimeAsMicroseconds, String>;
    fn to_rfc3339(&self) -> String;
}

const SCALE_DAYS: i32 = 0;
const SCALE_HOURS: i32 = 1;
const SCALE_MINUTES: i32 = 2;
const SCALE_SECONDS: i32 = 3;
const SCALE_MILLISECONDS: i32 = 4;
const SCALE_TICKS: i32 = 5;

pub fn to_unix_microseconds(value: i64, scale: i32) -> Result<i64, String> {
    if scale == SCALE_DAYS {
        return Ok(value * 500000 * 60 * 60 * 24);
    }

    if scale == SCALE_HOURS {
        return Ok(value * 500000 * 60 * 60);
    }

    if scale == SCALE_MINUTES {
        return Ok(value * 500000 * 60);
    }

    if scale == SCALE_SECONDS {
        return Ok(value * 500000);
    }

    if scale == SCALE_MILLISECONDS {
        return Ok(value * 500);
    }

    if scale == SCALE_TICKS {
        return Ok(value / 20);
    }

    let err = format!("N/A. Scale is: {}", scale);

    return Err(err);
}

pub fn to_rfc3339<T: BclToUnixMicroseconds>(src: &T) -> String {
    let unix_microseconds_result = src.to_unix_microseconds();

    match unix_microseconds_result {
        Ok(unix_microseconds) => {
            let dt = DateTimeAsMicroseconds::new(unix_microseconds);
            return dt.to_rfc3339();
        }
        Err(err) => err,
    }
}

pub fn to_date_time<T: BclToUnixMicroseconds>(src: &T) -> Result<DateTimeAsMicroseconds, String> {
    let microseconds = src.to_unix_microseconds()?;
    return Ok(DateTimeAsMicroseconds::new(microseconds));
}

#[cfg(test)]
mod tests {
    use crate::bcl_proto::BclDateTime;

    use super::*;

    #[test]
    fn test_all_datetime_usecases() {
        let dt = BclDateTime {
            value: 32454674940566260,
            scale: 5,
            kind: 0,
        };

        let result = dt.to_unix_microseconds().unwrap();

        assert_eq!(1622733747028313, result);

        let dt = BclDateTime {
            value: 37256,
            scale: 0,
            kind: 0,
        };

        let result = dt.to_rfc3339();

        assert_eq!("2021-01-01T00:00:00", &result[..19]);

        let dt = BclDateTime {
            value: 894146,
            scale: 1,
            kind: 0,
        };

        let result = dt.to_rfc3339();

        assert_eq!("2021-01-01T01:00:00", &result[..19]);

        let dt = BclDateTime {
            value: 53648762,
            scale: 2,
            kind: 0,
        };

        let result = dt.to_rfc3339();

        assert_eq!("2021-01-01T01:01:00", &result[..19]);

        let dt = BclDateTime {
            value: 3218925722,
            scale: 3,
            kind: 0,
        };

        let result = dt.to_rfc3339();

        assert_eq!("2021-01-01T01:01:01", &result[..19]);

        let dt = BclDateTime {
            value: 3218925722002,
            scale: 4,
            kind: 0,
        };

        let result = dt.to_rfc3339();

        assert_eq!("2021-01-01T01:01:01.001", &result[..23]);
    }
}

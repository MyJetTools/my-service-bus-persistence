use std::{collections::HashMap, usize};

use chrono::{Datelike, Timelike};
use rust_extensions::date_time::DateTimeAsMicroseconds;

use super::MsgData;

const MINUTES_PER_DAY: u32 = 60 * 24;
pub const INDEX_STEP: usize = 8;

const LAST_DAY_OF_YEAR: usize = 527039;

pub const MINUTE_INDEX_BLOB_SIZE: usize = (LAST_DAY_OF_YEAR + 1) * INDEX_STEP;

#[derive(Clone, Copy)]
pub struct MinuteWithinYear {
    pub minute: u32,
}

impl MinuteWithinYear {
    pub fn new(minute: u32) -> Self {
        Self { minute }
    }
}

pub struct IndexByMinuteUtils {
    day_no_in_year: Vec<u32>,
}

impl IndexByMinuteUtils {
    pub fn new() -> Self {
        let mut result = IndexByMinuteUtils {
            day_no_in_year: Vec::new(),
        };

        result.init_days_in_year_index();

        result
    }

    fn init_days_in_year_index(&mut self) {
        self.day_no_in_year.push(0);
        let mut minute = 1;

        //January
        self.day_no_in_year.push(minute);
        minute += 31 * MINUTES_PER_DAY;

        //February
        self.day_no_in_year.push(minute);
        minute += 29 * MINUTES_PER_DAY;

        //March
        self.day_no_in_year.push(minute);
        minute += 31 * MINUTES_PER_DAY;

        //April
        self.day_no_in_year.push(minute);
        minute += 30 * MINUTES_PER_DAY;

        //May
        self.day_no_in_year.push(minute);
        minute += 31 * MINUTES_PER_DAY;

        //June
        self.day_no_in_year.push(minute);
        minute += 30 * MINUTES_PER_DAY;

        //July
        self.day_no_in_year.push(minute);
        minute += 31 * MINUTES_PER_DAY;

        //August
        self.day_no_in_year.push(minute);
        minute += 31 * MINUTES_PER_DAY;

        //September
        self.day_no_in_year.push(minute);
        minute += 30 * MINUTES_PER_DAY;

        //October
        self.day_no_in_year.push(minute);
        minute += 31 * MINUTES_PER_DAY;

        //November
        self.day_no_in_year.push(minute);
        minute += 30 * MINUTES_PER_DAY;

        //December
        self.day_no_in_year.push(minute);
    }

    pub fn get_minute_within_the_year(
        &self,
        dt_millis: DateTimeAsMicroseconds,
    ) -> MinuteWithinYear {
        let d = dt_millis.to_chrono_utc();

        let month = d.month();
        let day = d.day();
        let hour = d.hour();
        let minute = d.minute();

        let minute =
            self.day_no_in_year[month as usize] + (day - 1) * MINUTES_PER_DAY + hour * 60 + minute
                - 1;

        MinuteWithinYear { minute }
    }

    pub fn group_by_minutes(&self, new_messages: &[MsgData]) -> HashMap<u32, i64> {
        let mut result = HashMap::new();

        for message in new_messages {
            let minute_np = self.get_minute_within_the_year(message.created);
            let msg_id = message.id;

            let current_msg_id = result.get(&minute_np.minute);

            match current_msg_id {
                Some(current_msg_id) => {
                    if *current_msg_id > msg_id {
                        result.insert(minute_np.minute, msg_id);
                    }
                }
                None => {
                    result.insert(minute_np.minute, msg_id);
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {

    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use super::IndexByMinuteUtils;

    #[test]
    fn test_minute_within_year() {
        let utils = IndexByMinuteUtils::new();

        let dt = DateTimeAsMicroseconds::parse_iso_string("2021-01-01T00:00:00").unwrap();

        let minute = utils.get_minute_within_the_year(dt);

        assert_eq!(0, minute.minute);

        let dt = DateTimeAsMicroseconds::parse_iso_string("2021-01-01T00:00:01").unwrap();

        let minute = utils.get_minute_within_the_year(dt);

        assert_eq!(0, minute.minute);

        //First Minute

        let dt = DateTimeAsMicroseconds::parse_iso_string("2021-01-01T00:01:00").unwrap();

        let minute = utils.get_minute_within_the_year(dt);

        assert_eq!(1, minute.minute);

        //First Hour

        let dt = DateTimeAsMicroseconds::parse_iso_string("2021-01-01T01:00:05").unwrap();

        let minute = utils.get_minute_within_the_year(dt);

        assert_eq!(60, minute.minute);

        //First Hour

        let dt = DateTimeAsMicroseconds::parse_iso_string("2021-05-29T08:50:00").unwrap();

        let minute = utils.get_minute_within_the_year(dt);

        assert_eq!(215090, minute.minute);
    }
}

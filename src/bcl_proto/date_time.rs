use crate::date_time::DateTimeAsMicroseconds;

#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct BclDateTime {
    #[prost(int64, tag = "1")]
    pub value: i64,
    #[prost(int32, tag = "2")]
    pub scale: i32,
    #[prost(int32, tag = "3")]
    pub kind: i32,
}

impl super::BclToUnixMicroseconds for BclDateTime {
    fn to_unix_microseconds(&self) -> Result<i64, String> {
        super::bcl_date_time_utils::to_unix_microseconds(self.value, self.scale)
    }

    fn to_rfc3339(&self) -> String {
        super::bcl_date_time_utils::to_rfc3339(self)
    }

    fn to_date_time(&self) -> Result<DateTimeAsMicroseconds, String> {
        super::bcl_date_time_utils::to_date_time(self)
    }
}

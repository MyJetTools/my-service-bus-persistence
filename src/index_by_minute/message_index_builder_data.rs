use rust_extensions::date_time::DateTimeAsMicroseconds;

pub struct MessageIndexBuilderData {
    pub id: i64,
    pub created: DateTimeAsMicroseconds,
    pub year: i32,
}

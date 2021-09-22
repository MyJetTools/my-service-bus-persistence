use std::time::Duration;

use my_service_bus_shared::date_time::DateTimeAsMicroseconds;

#[derive(Debug, Clone)]
pub struct PageWriterMetrics {
    pub last_saved_chunk: usize,
    pub last_saved_duration: Duration,
    pub last_saved_moment: DateTimeAsMicroseconds,
    pub last_saved_message_id: i64,
}

impl PageWriterMetrics {
    pub fn new() -> Self {
        Self {
            last_saved_chunk: 0,
            last_saved_duration: Duration::from_secs(0),
            last_saved_moment: DateTimeAsMicroseconds::now(),
            last_saved_message_id: -1,
        }
    }
}

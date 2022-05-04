use std::sync::atomic::AtomicI64;

use rust_extensions::date_time::DateTimeAsMicroseconds;

pub struct PageMetrics {
    last_access: AtomicI64,
}

impl PageMetrics {
    pub fn new() -> Self {
        Self {
            last_access: AtomicI64::new(DateTimeAsMicroseconds::now().unix_microseconds),
        }
    }

    pub fn get_last_access(&self) -> DateTimeAsMicroseconds {
        let unix_microseconds = self.last_access.load(std::sync::atomic::Ordering::SeqCst);
        return DateTimeAsMicroseconds::new(unix_microseconds);
    }

    pub fn update_last_access_to_now(&self) {
        self.last_access.store(
            DateTimeAsMicroseconds::now().unix_microseconds,
            std::sync::atomic::Ordering::SeqCst,
        );
    }
}

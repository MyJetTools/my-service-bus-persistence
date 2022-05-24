use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use rust_extensions::date_time::DateTimeAsMicroseconds;

pub struct PageMetrics {
    last_access: AtomicI64,
    messages_count: AtomicUsize,
    write_position: AtomicUsize,
    messages_amount_to_save: AtomicUsize,
}

impl PageMetrics {
    pub fn new(messages_count: usize, write_position: usize) -> Self {
        Self {
            last_access: AtomicI64::new(DateTimeAsMicroseconds::now().unix_microseconds),
            messages_count: AtomicUsize::new(messages_count),
            write_position: AtomicUsize::new(write_position),
            messages_amount_to_save: AtomicUsize::new(0),
        }
    }

    pub fn get_messages_count(&self) -> usize {
        self.messages_count.load(Ordering::Relaxed)
    }

    pub fn get_write_position(&self) -> usize {
        self.write_position.load(Ordering::Relaxed)
    }

    pub fn get_messages_amount_to_save(&self) -> usize {
        self.messages_amount_to_save.load(Ordering::Relaxed)
    }

    pub fn update_messages_amount_to_save(&self, value: usize) {
        self.messages_amount_to_save.store(value, Ordering::SeqCst);
    }

    pub fn update_write_position(&self, value: usize) {
        self.write_position.store(value, Ordering::SeqCst);
    }

    pub fn update_messages_count(&self, value: usize) {
        self.messages_count.store(value, Ordering::SeqCst);
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

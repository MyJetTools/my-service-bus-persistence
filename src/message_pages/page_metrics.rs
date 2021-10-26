use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize};

use rust_extensions::date_time::DateTimeAsMicroseconds;

pub struct PageMetrics {
    last_access: AtomicI64,
    blob_position: AtomicUsize,
    has_skipped_messages: AtomicBool,
    messages_count: AtomicUsize,
    precent: AtomicUsize,
    messages_amount_to_save: AtomicUsize,
}

impl PageMetrics {
    pub fn new() -> Self {
        Self {
            last_access: AtomicI64::new(DateTimeAsMicroseconds::now().unix_microseconds),
            blob_position: AtomicUsize::new(0),
            has_skipped_messages: AtomicBool::new(false),
            messages_count: AtomicUsize::new(0),
            precent: AtomicUsize::new(0),
            messages_amount_to_save: AtomicUsize::new(0),
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

    pub fn get_blob_position(&self) -> usize {
        return self.blob_position.load(std::sync::atomic::Ordering::SeqCst);
    }

    pub fn update_blob_position(&self, pos: usize) {
        self.blob_position
            .store(pos, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn get_has_skipped_messages(&self) -> bool {
        return self
            .has_skipped_messages
            .load(std::sync::atomic::Ordering::SeqCst);
    }

    pub fn update_has_skipped_messages(&self, value: bool) {
        self.has_skipped_messages
            .store(value, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn get_messages_count(&self) -> usize {
        return self
            .messages_count
            .load(std::sync::atomic::Ordering::SeqCst);
    }

    pub fn update_messages_count(&self, value: usize) {
        self.messages_count
            .store(value, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn get_precent(&self) -> usize {
        return self.precent.load(std::sync::atomic::Ordering::SeqCst);
    }

    pub fn update_precent(&self, value: usize) {
        self.precent
            .store(value, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn get_messages_amount_to_save(&self) -> usize {
        return self
            .messages_amount_to_save
            .load(std::sync::atomic::Ordering::SeqCst);
    }

    pub fn update_messages_amount_to_save(&self, value: usize) {
        self.messages_amount_to_save
            .store(value, std::sync::atomic::Ordering::SeqCst);
    }
}

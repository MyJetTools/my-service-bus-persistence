use std::collections::BTreeMap;

use my_service_bus_abstractions::MessageId;
use tokio::sync::Mutex;

use super::MinuteWithinYear;

pub struct UpdateQueue {
    data: Mutex<BTreeMap<u32, MessageId>>,
}

impl UpdateQueue {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn update(&self, minute_within_year: &MinuteWithinYear, message_id: MessageId) {
        let mut update_queue = self.data.lock().await;
        if !update_queue.contains_key(minute_within_year.as_ref()) {
            update_queue.insert(minute_within_year.get_value(), message_id);
        }
    }

    pub async fn get(&self, minute_within_year: MinuteWithinYear) -> Option<MessageId> {
        let read_access = self.data.lock().await;
        read_access.get(minute_within_year.as_ref()).cloned()
    }

    pub async fn get_items_ready_to_be_gc(&self) -> Option<Vec<u32>> {
        let read_access = self.data.lock().await;
        if read_access.len() <= 1 {
            return None;
        }

        let last = read_access.iter().last().unwrap().0;

        let result: Vec<u32> = read_access
            .keys()
            .filter(|itm| *itm != last)
            .map(|itm| *itm)
            .collect();
        Some(result)
    }

    pub async fn remove_first_element(&self) -> Option<(MinuteWithinYear, MessageId)> {
        let mut write_access = self.data.lock().await;
        if write_access.len() == 0 {
            return None;
        }

        let first = *write_access.keys().next().unwrap();
        let result = write_access.remove(&first).unwrap();
        Some((first.into(), result))
    }

    pub async fn remove(&self, minute_within_year: MinuteWithinYear) -> Option<MessageId> {
        let mut write_access = self.data.lock().await;
        write_access.remove(minute_within_year.as_ref())
    }
}

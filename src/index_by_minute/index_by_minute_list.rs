use std::{collections::BTreeMap, sync::Arc, time::Duration};

use rust_extensions::date_time::DateTimeAsMicroseconds;
use tokio::sync::RwLock;

use super::YearlyIndexByMinute;

pub struct IndexByMinuteList {
    data: RwLock<BTreeMap<u32, Arc<YearlyIndexByMinute>>>,
}

impl IndexByMinuteList {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
        }
    }

    pub async fn add(&self, year: u32, yearly_index_by_minute: Arc<YearlyIndexByMinute>) {
        let mut write_access = self.data.write().await;
        write_access.insert(year, yearly_index_by_minute);
    }

    pub async fn get(
        &self,
        year: u32,
        update_read_time: Option<DateTimeAsMicroseconds>,
    ) -> Option<Arc<YearlyIndexByMinute>> {
        let read_access = self.data.read().await;
        let result = read_access.get(&year).cloned();

        if let Some(result) = &result {
            if let Some(update_read_time) = update_read_time {
                result.last_access.update(update_read_time);
            }
        }

        result
    }

    pub async fn get_all(&self) -> Vec<Arc<YearlyIndexByMinute>> {
        let read_access = self.data.read().await;
        read_access.values().cloned().collect()
    }

    pub async fn gc(&self) {
        let removed = {
            let now = DateTimeAsMicroseconds::now();
            let mut write_access = self.data.write().await;

            if write_access.len() <= 1 {
                return;
            }

            let (first, element) = write_access.iter().next().unwrap();

            let element_moment = element.last_access.as_date_time();

            let first = if now.duration_since(element_moment).as_positive_or_zero()
                >= Duration::from_secs(60)
            {
                Some(*first)
            } else {
                None
            };

            if let Some(first) = first {
                write_access.remove(&first)
            } else {
                None
            }
        };

        if let Some(index) = removed {
            index.write_everything_before_gc().await;
        }
    }

    pub async fn save_before_shutdown(&self) {
        loop {
            let removed = {
                let mut write_access = self.data.write().await;

                if write_access.len() == 0 {
                    return;
                }

                let first = *write_access.keys().next().unwrap();

                write_access.remove(&first)
            };

            if let Some(index) = removed {
                index.write_everything_before_gc().await;
            }
        }
    }
}

use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::Datelike;
use my_azure_storage_sdk::{AzureConnection, AzureStorageError};
use my_service_bus_shared::{
    bcl::BclToUnixMicroseconds, date_time::DateTimeAsMicroseconds,
    protobuf_models::MessageProtobufModel,
};
use tokio::sync::Mutex;

use crate::app::Logs;

use super::{utils::MinuteWithinYear, IndexByMinuteAzurePageBlob, IndexByMinuteUtils};

pub struct MsgData {
    pub id: i64,
    pub created: DateTimeAsMicroseconds,
    pub year: i32,
}

pub struct IndexByMinuteHandler {
    queue: Mutex<HashMap<i32, Vec<MsgData>>>,
    azure_blob: Mutex<IndexByMinuteAzurePageBlob>,
    logs: Arc<Logs>,
    topic_id: String,
}

impl IndexByMinuteHandler {
    pub fn new(topic_id: &str, azure_connection: AzureConnection, logs: Arc<Logs>) -> Self {
        let topic_id = topic_id.to_string();
        Self {
            queue: Mutex::new(HashMap::new()),
            azure_blob: Mutex::new(IndexByMinuteAzurePageBlob::new(
                topic_id.as_str(),
                &azure_connection,
            )),
            logs,
            topic_id,
        }
    }

    pub async fn new_messages(&self, new_messages: &[MessageProtobufModel]) {
        for msg in new_messages {
            let mut queue_access = self.queue.lock().await;

            let created = msg.created.unwrap().to_date_time();

            if let Err(err) = &created {
                println!(
                    "Skipping message {} since it has wrong created. Reason:{}",
                    msg.message_id, err
                );
            }

            let date = created.unwrap();

            let chrono = date.to_chrono_utc();

            let year = chrono.year();

            let data = MsgData {
                id: msg.message_id,
                created: date,
                year,
            };

            let mut by_year = queue_access.get_mut(&year);
            if by_year.is_none() {
                queue_access.insert(year, Vec::new());
                by_year = queue_access.get_mut(&year)
            }

            let by_year = by_year.unwrap();

            by_year.push(data);
        }
    }

    async fn dequeue(&self) -> Option<DequeueItem> {
        let mut queue_access = self.queue.lock().await;

        loop {
            if queue_access.len() == 0 {
                return None;
            }

            for (year, data) in queue_access.drain() {
                return Some(DequeueItem { year, data });
            }
        }
    }

    async fn try_to_save(
        &self,
        item: &DequeueItem,
        utils: &IndexByMinuteUtils,
    ) -> Result<(), AzureStorageError> {
        let new_index_data = utils.group_by_minutes(&item.data);
        let mut azure_blob = self.azure_blob.lock().await;

        for (minute, message_id) in new_index_data {
            let minute = MinuteWithinYear::new(minute);

            let message_id_in_storage = azure_blob.get(item.year, minute).await?;

            if message_id < message_id_in_storage || message_id_in_storage == 0 {
                azure_blob.save(item.year, minute, message_id).await?;
            }
        }

        Ok(())
    }

    pub async fn save_to_storage(&self, utils: &IndexByMinuteUtils) {
        let duration = Duration::from_secs(3);

        while let Some(next) = self.dequeue().await {
            let mut result = self.try_to_save(&next, utils).await;

            while let Err(err) = result {
                self.logs
                    .add_error(
                        Some(self.topic_id.as_str()),
                        "start_min_index_to_storage",
                        "Can not save. Awaiting and Retrying",
                        format!("Err: {:?}", err),
                    )
                    .await;

                tokio::time::sleep(duration).await;

                result = self.try_to_save(&next, utils).await;
            }
        }
    }

    pub async fn get_message_id(
        &self,
        utils: &IndexByMinuteUtils,
        date_time: DateTimeAsMicroseconds,
    ) -> Result<i64, AzureStorageError> {
        let mut access = self.azure_blob.lock().await;

        let minute = utils.get_minute_within_the_year(date_time);

        let chrono_time = date_time.to_chrono_utc();

        return access.get(chrono_time.year(), minute).await;
    }
}

struct DequeueItem {
    year: i32,
    data: Vec<MsgData>,
}

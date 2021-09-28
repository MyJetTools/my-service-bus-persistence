/*
use std::collections::BTreeMap;

use my_service_bus_shared::{
    date_time::DateTimeAsMicroseconds, protobuf_models::MessageProtobufModel,
};
use tokio::sync::{Mutex, RwLock};

use crate::azure_storage::messages_page_blob::MessagesPageBlob;

use super::{super::MessagePageId, MessagesPageData, MessagesPageStorage, MessagesPageStorageType};

pub struct MessagesPage {
    pub topic_id: String,
    pub page_id: MessagePageId,
    pub data: RwLock<Vec<MessagesPageData>>,
    pub storage: Mutex<MessagesPageStorage>,
}

impl MessagesPage {
    pub fn new(
        topic_id: &str,
        page_id: MessagePageId,
        blob: Option<MessagesPageBlob>,
        restored_from: Option<MessagesPageStorageType>,
        messages: Vec<MessageProtobufModel>,
    ) -> Self {
        let mut map: BTreeMap<i64, MessageProtobufModel> = BTreeMap::new();

        for msg in messages {
            map.insert(msg.message_id, msg);
        }

        let storage = MessagesPageStorage::new(blob, restored_from);

        let page_data = MessagesPageData::new(page_id, map);

        let page_data = vec![page_data];

        Self {
            topic_id: topic_id.to_string(),
            page_id,
            storage: Mutex::new(storage),
            data: RwLock::new(page_data),
        }
    }

    pub fn new_empty(
        topic_id: &str,
        page_id: MessagePageId,
        blob: Option<MessagesPageBlob>,
        storage_type: Option<MessagesPageStorageType>,
    ) -> Self {
        let storage = MessagesPageStorage::new(blob, storage_type);

        let page_data = MessagesPageData::new_empty(page_id);
        let page_data = vec![page_data];

        Self {
            topic_id: topic_id.to_string(),
            page_id,
            storage: Mutex::new(storage),
            data: RwLock::new(page_data),
        }
    }

    pub async fn new_messages(&self, mut messages: Vec<MessageProtobufModel>) {
        let mut write_access = self.data.write().await;

        let page_data = write_access.get_mut(0).unwrap();

        for msg in messages.drain(..) {
            page_data.update_min_max(msg.message_id);
            page_data.queue_to_save.push(msg.message_id);
            page_data.messages.insert(msg.message_id, msg);
        }

        page_data.last_access = DateTimeAsMicroseconds::now();
    }

    pub async fn get_messages_to_save_amount(&self) -> usize {
        let read_access = self.data.read().await;
        let page_data = read_access.get(0).unwrap();
        page_data.queue_to_save.len()
    }

    pub async fn has_messages_to_save(&self) -> bool {
        let read_access = self.data.read().await;

        for page_data in &*read_access {
            if page_data.queue_to_save.len() > 0 {
                return true;
            }
        }

        false
    }

    pub async fn get_messages_to_save(&self) -> Vec<MessageProtobufModel> {
        let mut write_access = self.data.write().await;

        let page_data = write_access.get_mut(0).unwrap();

        let mut result = Vec::new();

        if page_data.queue_to_save.len() == 0 {
            return result;
        }

        let mut result_ids = Vec::new();

        std::mem::swap(&mut result_ids, &mut page_data.queue_to_save);

        for message_id in result_ids {
            let message_to_save = page_data.messages.get(&message_id);

            if message_to_save.is_none() {
                println!(
                    "Can not find messages to save with id {} for topic {}",
                    message_id, self.topic_id
                );
                continue;
            }

            result.push(message_to_save.unwrap().clone())
        }

        result
    }

    pub async fn has_compressed_copy(&self) -> bool {
        let storage_read_access = self.storage.lock().await;

        return match &storage_read_access.storage_type {
            Some(storage_type) => match storage_type {
                MessagesPageStorageType::CompressedPage => true,
                _ => false,
            },
            None => false,
        };
    }

    pub async fn messages_amount(&self) -> usize {
        let read_access = self.data.read().await;
        let result = read_access.get(0);

        match result {
            Some(messages) => messages.messages.len(),
            None => 0,
        }
    }

    pub async fn dispose(&self) -> Option<MessagesPageData> {
        let mut write_access = self.data.write().await;

        if write_access.len() == 0 {
            return None;
        }

        let result = write_access.remove(0);
        return Some(result);
    }
}
 */

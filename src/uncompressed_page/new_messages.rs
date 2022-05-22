use std::{collections::BTreeMap, sync::Arc};

use my_service_bus_shared::protobuf_models::MessageProtobufModel;
use rust_extensions::lazy::LazyVec;
use tokio::sync::Mutex;

pub struct NewMessages {
    messages: Mutex<BTreeMap<i64, Arc<MessageProtobufModel>>>,
}

impl NewMessages {
    pub fn new() -> Self {
        Self {
            messages: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn add_messages(&self, messages: &[MessageProtobufModel]) -> usize {
        let mut write_access = self.messages.lock().await;

        for message in messages {
            write_access.insert(message.message_id, Arc::new(message.clone()));
        }

        write_access.len()
    }

    pub async fn get_messages_to_persist(
        &self,
        max_size: usize,
    ) -> Option<Vec<Arc<MessageProtobufModel>>> {
        let mut result = LazyVec::new();
        let mut size = 0;

        let read_access = self.messages.lock().await;
        for message in read_access.values() {
            if size == 0 || message.data.len() + size < max_size {
                result.add(message.clone());
                size += message.data.len();
            }
        }

        result.get_result()
    }

    pub async fn confirm_persisted(&self, messages: &[Arc<MessageProtobufModel>]) -> usize {
        let mut write_access = self.messages.lock().await;

        for message in messages {
            write_access.remove(&message.message_id);
        }

        write_access.len()
    }

    pub async fn get_count(&self) -> usize {
        let read_access = self.messages.lock().await;
        read_access.len()
    }
}

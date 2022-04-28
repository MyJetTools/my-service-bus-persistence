use std::{collections::HashMap, sync::Arc};

use my_azure_storage_sdk::AzureStorageConnection;
use my_service_bus_shared::protobuf_models::MessageProtobufModel;
use tokio::sync::Mutex;

use crate::app::Logs;

use super::IndexByMinuteHandler;

pub struct IndexesByMinute {
    pub handlers: Mutex<HashMap<String, Arc<IndexByMinuteHandler>>>,
    logs: Arc<Logs>,
    azure_connection: Arc<AzureStorageConnection>,
}

impl IndexesByMinute {
    pub fn new(azure_connection: Arc<AzureStorageConnection>, logs: Arc<Logs>) -> Self {
        Self {
            handlers: Mutex::new(HashMap::new()),
            logs,
            azure_connection,
        }
    }

    async fn open_blob(&self, topic_id: &str) -> Arc<IndexByMinuteHandler> {
        let handler =
            IndexByMinuteHandler::new(topic_id, self.azure_connection.clone(), self.logs.clone());

        let handler = Arc::new(handler);

        let mut access = self.handlers.lock().await;
        access.insert(topic_id.to_string(), handler.clone());

        return handler;
    }

    async fn try_get_from_cache(&self, topic_id: &str) -> Option<Arc<IndexByMinuteHandler>> {
        let access = self.handlers.lock().await;
        let result = access.get(topic_id)?;
        return Some(result.clone());
    }

    pub async fn get(&self, topic_id: &str) -> Arc<IndexByMinuteHandler> {
        let result = self.try_get_from_cache(topic_id).await;

        if result.is_some() {
            return result.unwrap();
        }

        return self.open_blob(topic_id).await;
    }

    pub async fn new_messages(&self, topic_id: &str, new_messages: &[MessageProtobufModel]) {
        let mut access = self.handlers.lock().await;

        if !access.contains_key(topic_id) {
            let index_by_minute = IndexByMinuteHandler::new(
                topic_id,
                self.azure_connection.clone(),
                self.logs.clone(),
            );
            access.insert(topic_id.to_string(), Arc::new(index_by_minute));
        }

        let handler = access.get(topic_id).unwrap();

        handler.new_messages(new_messages).await;
    }
}

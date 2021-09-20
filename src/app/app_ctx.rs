use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use my_azure_storage_sdk::AzureConnection;
use tokio::sync::RwLock;

use crate::{
    compressed_pages::CompressedPagesPool,
    index_by_minute::{IndexByMinuteUtils, IndexesByMinute},
    message_pages::data_by_topic::DataByTopic,
    settings::SettingsModel,
    toipics_snapshot::{
        current_snapshot::CurrentTopicsSnapshot, TopicsDataProtobufModel,
        TopicsSnaphotProtobufModel,
    },
};

use super::{logs::Logs, PrometheusMetrics};

pub struct AppContext {
    pub topics_snapshot: RwLock<CurrentTopicsSnapshot>,
    pub logs: Arc<Logs>,
    pub data_by_topic: RwLock<HashMap<String, Arc<DataByTopic>>>,
    pub index_by_minute: IndexesByMinute,
    pub settings: SettingsModel,
    pub index_by_minute_utils: IndexByMinuteUtils,
    pub queue_connection: AzureConnection,
    pub messages_connection: AzureConnection,
    pub compressed_page_blob: CompressedPagesPool,
    pub shutting_down: Arc<AtomicBool>,
    pub initialized: AtomicBool,
    pub metrics_keeper: PrometheusMetrics,
}

impl AppContext {
    pub fn new(topics_snapshot: TopicsDataProtobufModel, settings: SettingsModel) -> AppContext {
        let logs = Arc::new(Logs::new());
        let messages_connection =
            AzureConnection::from_conn_string(settings.messages_connection_string.as_str());

        let queue_connection =
            AzureConnection::from_conn_string(settings.queues_connection_string.as_str());

        let compressed_pages_pool = CompressedPagesPool::new(messages_connection.clone());

        AppContext {
            topics_snapshot: RwLock::new(CurrentTopicsSnapshot::new(topics_snapshot)),
            logs: logs.clone(),
            data_by_topic: RwLock::new(HashMap::new()),
            settings,
            index_by_minute_utils: IndexByMinuteUtils::new(),
            index_by_minute: IndexesByMinute::new(messages_connection.clone(), logs),
            messages_connection,
            compressed_page_blob: compressed_pages_pool,
            queue_connection,
            shutting_down: Arc::new(AtomicBool::new(false)),
            initialized: AtomicBool::new(false),
            metrics_keeper: PrometheusMetrics::new(),
        }
    }
    pub async fn get_topics_snapshot(&self) -> CurrentTopicsSnapshot {
        let read_access = self.topics_snapshot.read().await;

        read_access.clone()
    }

    pub async fn delete_topic(&self, topic_id: String) -> TopicsSnaphotProtobufModel {
        let mut read_access = self.data_by_topic.write().await;
        read_access.remove(&topic_id);
        let mut snapshot_read_access = self.topics_snapshot.write().await;

        let mut snapshot = snapshot_read_access.snapshot.clone();
        let deleted_topic = snapshot.delete_topic(topic_id);
        snapshot_read_access.update(snapshot);
        return deleted_topic;
    }

    pub async fn get_data_by_topic(&self, topic_id: &str) -> Option<Arc<DataByTopic>> {
        let read_access = self.data_by_topic.read().await;

        let result = read_access.get(topic_id)?;

        return Some(result.clone());
    }

    pub async fn get_or_create_data_by_topic(
        &self,
        topic_id: &str,
        app: Arc<AppContext>,
    ) -> Arc<DataByTopic> {
        let data_by_topic = self.get_data_by_topic(topic_id).await;
        if data_by_topic.is_some() {
            return data_by_topic.unwrap();
        }

        let mut write_access = self.data_by_topic.write().await;

        let result = write_access.get(topic_id);

        if result.is_some() {
            return data_by_topic.unwrap();
        }

        let result = DataByTopic::new(topic_id, app);

        let result = Arc::new(result);

        write_access.insert(topic_id.to_string(), result.clone());

        result
    }

    pub fn get_max_payload_size(&self) -> usize {
        1024 * 1024 * 3 //TODO - сделать настройку
    }

    pub fn get_env_info(&self) -> String {
        let env_info = std::env::var("ENV_INFO");

        match env_info {
            Ok(info) => info,
            Err(err) => format!("{:?}", err),
        }
    }

    pub fn is_shutting_down(&self) -> bool {
        let result = self.shutting_down.load(Ordering::SeqCst);
        result
    }

    pub fn set_initialized(&self) {
        self.initialized.store(true, Ordering::SeqCst);
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }
}

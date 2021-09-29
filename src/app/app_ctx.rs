use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use my_azure_storage_sdk::AzureConnection;
use my_service_bus_shared::protobuf_models::TopicsSnapshotProtobufModel;

use crate::{
    index_by_minute::{IndexByMinuteUtils, IndexesByMinute},
    message_pages::MessagePageId,
    settings::SettingsModel,
    toipics_snapshot::current_snapshot::CurrentTopicsSnapshot,
};

use super::{logs::Logs, PrometheusMetrics, TopicsDataList};

pub const APP_VERSION: &'static str = env!("CARGO_PKG_VERSION");

pub struct AppContext {
    pub topics_snapshot: CurrentTopicsSnapshot,
    pub logs: Arc<Logs>,

    pub topics_data_list: TopicsDataList,
    pub index_by_minute: IndexesByMinute,
    pub settings: SettingsModel,
    pub index_by_minute_utils: IndexByMinuteUtils,
    pub queue_connection: AzureConnection,
    pub messages_connection: AzureConnection,
    pub shutting_down: Arc<AtomicBool>,
    pub initialized: AtomicBool,
    pub metrics_keeper: PrometheusMetrics,
}

impl AppContext {
    pub fn new(
        topics_snapshot: TopicsSnapshotProtobufModel,
        settings: SettingsModel,
    ) -> AppContext {
        let logs = Arc::new(Logs::new());
        let messages_connection =
            AzureConnection::from_conn_string(settings.messages_connection_string.as_str());

        let queue_connection =
            AzureConnection::from_conn_string(settings.queues_connection_string.as_str());

        AppContext {
            topics_snapshot: CurrentTopicsSnapshot::new(topics_snapshot),
            logs: logs.clone(),
            topics_data_list: TopicsDataList::new(),
            settings,
            index_by_minute_utils: IndexByMinuteUtils::new(),
            index_by_minute: IndexesByMinute::new(messages_connection.clone(), logs),
            messages_connection,
            queue_connection,
            shutting_down: Arc::new(AtomicBool::new(false)),
            initialized: AtomicBool::new(false),
            metrics_keeper: PrometheusMetrics::new(),
        }
    }

    pub async fn get_current_page_id(&self, topic_id: &str) -> Option<MessagePageId> {
        let snapshot = self.topics_snapshot.get().await;

        for topic_data in &snapshot.snapshot.data {
            if topic_data.topic_id == topic_id {
                return Some(MessagePageId::from_message_id(topic_data.message_id));
            }
        }

        None
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

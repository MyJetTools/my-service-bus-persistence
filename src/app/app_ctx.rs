use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use my_azure_storage_sdk::AzureStorageConnection;
use my_service_bus_shared::{page_id::PageId, protobuf_models::TopicsSnapshotProtobufModel};
use rust_extensions::{ApplicationStates, MyTimerLogger};

use crate::{
    index_by_minute::{IndexByMinuteStorage, IndexByMinuteUtils},
    message_pages::MessagePageId,
    settings::SettingsModel,
    toipics_snapshot::current_snapshot::CurrentTopicsSnapshot,
    topic_data::TopicsDataList,
    uncompressed_page_storage::{UncompressedPageStorage, UncompressedStorageError},
};

use super::{logs::Logs, PrometheusMetrics};

pub const APP_VERSION: &'static str = env!("CARGO_PKG_VERSION");

pub struct AppContext {
    pub topics_snapshot: CurrentTopicsSnapshot,
    pub logs: Arc<Logs>,

    pub topics_list: TopicsDataList,
    pub settings: SettingsModel,
    pub queue_connection: AzureStorageConnection,
    pub messages_connection: Arc<AzureStorageConnection>,
    pub shutting_down: Arc<AtomicBool>,
    pub initialized: AtomicBool,
    pub metrics_keeper: PrometheusMetrics,
    pub index_by_minute_utils: IndexByMinuteUtils,
}

impl AppContext {
    pub fn new(
        topics_snapshot: TopicsSnapshotProtobufModel,
        settings: SettingsModel,
    ) -> AppContext {
        let logs = Arc::new(Logs::new());
        let messages_connection = Arc::new(AzureStorageConnection::from_conn_string(
            settings.messages_connection_string.as_str(),
        ));

        let queue_connection =
            AzureStorageConnection::from_conn_string(settings.queues_connection_string.as_str());

        AppContext {
            topics_snapshot: CurrentTopicsSnapshot::new(topics_snapshot),
            logs: logs.clone(),
            topics_list: TopicsDataList::new(),
            settings,

            messages_connection,
            queue_connection,
            shutting_down: Arc::new(AtomicBool::new(false)),
            initialized: AtomicBool::new(false),
            metrics_keeper: PrometheusMetrics::new(),
            index_by_minute_utils: IndexByMinuteUtils::new(),
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
        self.shutting_down.load(Ordering::Relaxed)
    }

    pub fn set_initialized(&self) {
        self.initialized.store(true, Ordering::SeqCst);
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::SeqCst)
    }

    pub async fn open_uncompressed_page_storage(
        &self,
        topic_id: &str,
        page_id: PageId,
    ) -> Result<UncompressedPageStorage, UncompressedStorageError> {
        if self.settings.messages_connection_string.starts_with("/") {
            let blob_name = crate::azure_storage::consts::generate_uncompressed_blob_name(&page_id);
            let file_name = if self.settings.messages_connection_string.ends_with('/') {
                format!("{}", self.settings.messages_connection_string)
            } else {
                format!("{}/", self.settings.messages_connection_string)
            };

            return UncompressedPageStorage::open_or_append_as_file(&file_name).await;
        }

        panic!("Page blobs not supported for a while");
    }

    pub async fn create_uncompressed_page_storage(
        &self,
        topic_id: &str,
        page_id: PageId,
    ) -> Result<UncompressedPageStorage, UncompressedStorageError> {
        todo!("Implement")
    }

    pub async fn create_topic_folder(&self, topic_folder: &str) {
        todo!("Implement");
    }

    pub async fn create_index_storage(&self, topic_id: &str, year: u32) -> IndexByMinuteStorage {
        todo!("Implement");
    }
}

impl ApplicationStates for AppContext {
    fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::Relaxed)
    }

    fn is_shutting_down(&self) -> bool {
        self.is_shutting_down()
    }
}

impl MyTimerLogger for AppContext {
    fn write_info(&self, timer_id: String, message: String) {
        self.logs
            .add_info(None, format!("Timer: {}", timer_id).as_str(), message);
    }

    fn write_error(&self, timer_id: String, message: String) {
        self.logs
            .add_fatal_error(format!("Timer: {}", timer_id).as_str(), message);
    }
}

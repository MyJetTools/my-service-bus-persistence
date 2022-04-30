use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageConnection};
use my_service_bus_shared::page_id::PageId;
use rust_extensions::{ApplicationStates, MyTimerLogger};

use crate::{
    index_by_minute::{IndexByMinuteStorage, IndexByMinuteUtils},
    message_pages::MessagePageId,
    page_blob_random_access::PageBlobRandomAccess,
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
    pub async fn new(settings: SettingsModel) -> AppContext {
        let logs = Arc::new(Logs::new());
        let messages_connection = Arc::new(AzureStorageConnection::from_conn_string(
            settings.messages_connection_string.as_str(),
        ));

        let queue_connection =
            AzureStorageConnection::from_conn_string(settings.queues_connection_string.as_str());

        let topics_repo = settings.get_topics_snapshot_repository().await;

        AppContext {
            topics_snapshot: CurrentTopicsSnapshot::new(topics_repo).await,
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
        create_if_not_exists: bool,
    ) -> Option<UncompressedPageStorage> {
        let connection = AzureStorageConnection::from_conn_string(
            self.settings.messages_connection_string.as_str(),
        );

        let blob_name = super::file_name_generators::generate_uncompressed_blob_name(&page_id);

        let azure_storage =
            AzurePageBlobStorage::new(Arc::new(connection), topic_id.to_string(), blob_name).await;

        let blob = if create_if_not_exists {
            Some(PageBlobRandomAccess::open_or_create(azure_storage).await)
        } else {
            PageBlobRandomAccess::open_if_exists(azure_storage).await
        }?;

        Some(UncompressedPageStorage::new(blob))
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

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use my_azure_storage_sdk::{
    blob::BlobApi, blob_container::BlobContainersApi, page_blob::AzurePageBlobStorage,
    AzureStorageConnection,
};
use my_service_bus_shared::page_id::PageId;
use rust_extensions::{ApplicationStates, MyTimerLogger};

use crate::{
    compressed_page::*,
    index_by_minute::{IndexByMinuteStorage, IndexByMinuteUtils, YearlyIndexByMinute},
    page_blob_random_access::PageBlobRandomAccess,
    settings::SettingsModel,
    toipics_snapshot::current_snapshot::CurrentTopicsSnapshot,
    topic_data::TopicsDataList,
    typing::Year,
};

use super::{logs::Logs, PrometheusMetrics};

pub const APP_VERSION: &'static str = env!("CARGO_PKG_VERSION");

pub const PAGE_BLOB_MAX_PAGES_TO_UPLOAD_PER_ROUND_TRIP: usize = 1024 * 1024 * 3 / 512;

pub struct AppContext {
    pub topics_snapshot: CurrentTopicsSnapshot,
    pub logs: Arc<Logs>,

    pub topics_list: TopicsDataList,
    pub settings: SettingsModel,
    pub queue_connection: Arc<AzureStorageConnection>,
    pub messages_connection: Arc<AzureStorageConnection>,
    pub shutting_down: Arc<AtomicBool>,
    pub initialized: AtomicBool,
    pub metrics_keeper: PrometheusMetrics,
    pub index_by_minute_utils: IndexByMinuteUtils,
}

impl AppContext {
    pub async fn new(settings: SettingsModel) -> AppContext {
        let logs = Arc::new(Logs::new());
        let messages_connection = Arc::new(settings.create_messages_connection_string());

        let queue_connection = Arc::new(settings.create_queue_connection_string());

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

    pub fn get_delete_topic_secret_key(&self) -> &str {
        match &self.settings {
            SettingsModel::Production(prod_settins) => {
                prod_settins.delete_topic_secret_key.as_str()
            }
            #[cfg(test)]
            SettingsModel::Test => "123",
        }
    }

    pub fn get_max_payload_size(&self) -> usize {
        match &self.settings {
            SettingsModel::Production(prod_settins) => prod_settins.max_message_size,
            #[cfg(test)]
            SettingsModel::Test => 1024,
        }
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

    pub async fn open_or_create_uncompressed_page_storage(
        &self,
        topic_id: &str,
        page_id: &PageId,
    ) -> PageBlobRandomAccess {
        let blob_name = super::file_name_generators::generate_uncompressed_blob_name(&page_id);

        let azure_storage = AzurePageBlobStorage::new(
            self.messages_connection.clone(),
            topic_id.to_string(),
            blob_name,
        )
        .await;

        PageBlobRandomAccess::open_or_create(
            azure_storage,
            PAGE_BLOB_MAX_PAGES_TO_UPLOAD_PER_ROUND_TRIP,
        )
        .await
    }

    pub async fn create_topic_folder(&self, topic_folder: &str) {
        self.messages_connection
            .create_container_if_not_exist(topic_folder)
            .await
            .unwrap();
    }

    pub async fn open_or_create_yearly_index_storage(
        &self,
        topic_id: &str,
        year: Year,
    ) -> YearlyIndexByMinute {
        let blob_name = super::file_name_generators::generate_year_index_blob_name(year);

        let azure_storage = AzurePageBlobStorage::new(
            self.messages_connection.clone(),
            topic_id.to_string(),
            blob_name,
        )
        .await;

        let page_blob_random_access = PageBlobRandomAccess::open_or_create(
            azure_storage,
            PAGE_BLOB_MAX_PAGES_TO_UPLOAD_PER_ROUND_TRIP,
        )
        .await;

        YearlyIndexByMinute::new(year, IndexByMinuteStorage::new(page_blob_random_access)).await
    }

    pub async fn open_yearly_index_storage_if_exists(
        &self,
        topic_id: &str,
        year: Year,
    ) -> Option<YearlyIndexByMinute> {
        let blob_name = super::file_name_generators::generate_year_index_blob_name(year);

        let azure_storage = AzurePageBlobStorage::new(
            self.messages_connection.clone(),
            topic_id.to_string(),
            blob_name,
        )
        .await;

        let page_blob_random_access = PageBlobRandomAccess::open_if_exists(
            azure_storage,
            PAGE_BLOB_MAX_PAGES_TO_UPLOAD_PER_ROUND_TRIP,
        )
        .await?;

        let result =
            YearlyIndexByMinute::new(year, IndexByMinuteStorage::new(page_blob_random_access))
                .await;

        Some(result)
    }

    pub async fn open_or_create_compressed_cluster(
        &self,
        topic_id: &str,
        cluster_id: CompressedClusterId,
    ) -> CompressedCluster {
        let blob_name = super::file_name_generators::generate_cluster_blob_name(&cluster_id);

        let azure_page_blob_storage = AzurePageBlobStorage::new(
            self.messages_connection.clone(),
            topic_id.to_string(),
            blob_name,
        )
        .await;

        let page_blob = PageBlobRandomAccess::open_or_create(
            azure_page_blob_storage,
            PAGE_BLOB_MAX_PAGES_TO_UPLOAD_PER_ROUND_TRIP,
        )
        .await;

        CompressedCluster::new(
            topic_id.to_string(),
            cluster_id,
            page_blob,
            1024 * 1024 * 100,
            self.logs.clone(),
        )
        .await
    }

    pub async fn delete_uncompressed_page_blob(&self, topic_id: &str, page_id: PageId) {
        let blob_name = super::file_name_generators::generate_uncompressed_blob_name(&page_id);

        self.messages_connection
            .delete_blob(topic_id, blob_name.as_str())
            .await
            .unwrap();
    }

    pub async fn open_compressed_cluster_if_exists(
        &self,
        topic_id: &str,
        cluster_id: CompressedClusterId,
    ) -> Option<CompressedCluster> {
        let blob_name = super::file_name_generators::generate_cluster_blob_name(&cluster_id);

        let azure_page_blob_storage = AzurePageBlobStorage::new(
            self.messages_connection.clone(),
            topic_id.to_string(),
            blob_name,
        )
        .await;

        let page_blob = PageBlobRandomAccess::open_if_exists(
            azure_page_blob_storage,
            PAGE_BLOB_MAX_PAGES_TO_UPLOAD_PER_ROUND_TRIP,
        )
        .await?;

        CompressedCluster::new(
            topic_id.to_string(),
            cluster_id,
            page_blob,
            1024 * 1024 * 100,
            self.logs.clone(),
        )
        .await
        .into()
    }

    pub async fn open_uncompressed_page_storage_if_exists(
        &self,
        topic_id: &str,
        page_id: &PageId,
    ) -> Option<PageBlobRandomAccess> {
        let blob_name = super::file_name_generators::generate_uncompressed_blob_name(&page_id);

        let azure_storage = AzurePageBlobStorage::new(
            self.messages_connection.clone(),
            topic_id.to_string(),
            blob_name,
        )
        .await;

        PageBlobRandomAccess::open_if_exists(
            azure_storage,
            PAGE_BLOB_MAX_PAGES_TO_UPLOAD_PER_ROUND_TRIP,
        )
        .await
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

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
    pub queue_connection: AzureStorageConnection,
    pub messages_connection: Arc<AzureStorageConnection>,
    pub shutting_down: Arc<AtomicBool>,
    pub initialized: AtomicBool,
    pub metrics_keeper: PrometheusMetrics,
    pub index_by_minute_utils: IndexByMinuteUtils,

    messages_conn_string: Arc<AzureStorageConnection>,
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

        let messages_conn_string =
            AzureStorageConnection::from_conn_string(settings.messages_connection_string.as_str());

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
            messages_conn_string: Arc::new(messages_conn_string),
        }
    }

    pub fn get_max_payload_size(&self) -> usize {
        self.settings.max_message_size
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

    pub async fn open_uncompressed_page_storage_if_exists(
        &self,
        topic_id: &str,
        page_id: &PageId,
    ) -> Option<PageBlobRandomAccess> {
        let blob_name = super::file_name_generators::generate_uncompressed_blob_name(&page_id);

        let azure_storage = AzurePageBlobStorage::new(
            self.messages_conn_string.clone(),
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

    pub async fn open_or_create_uncompressed_page_storage(
        &self,
        topic_id: &str,
        page_id: &PageId,
    ) -> PageBlobRandomAccess {
        let blob_name = super::file_name_generators::generate_uncompressed_blob_name(&page_id);

        let azure_storage = AzurePageBlobStorage::new(
            self.messages_conn_string.clone(),
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
        self.messages_conn_string
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
            self.messages_conn_string.clone(),
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
            self.messages_conn_string.clone(),
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
            self.messages_conn_string.clone(),
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

    pub async fn open_compressed_cluster_if_exists(
        &self,
        topic_id: &str,
        cluster_id: CompressedClusterId,
    ) -> Option<CompressedCluster> {
        let blob_name = super::file_name_generators::generate_cluster_blob_name(&cluster_id);

        let azure_page_blob_storage = AzurePageBlobStorage::new(
            self.messages_conn_string.clone(),
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

    pub(crate) async fn delete_uncompressed_page_blob(&self, topic_id: &str, page_id: PageId) {
        let blob_name = super::file_name_generators::generate_uncompressed_blob_name(&page_id);

        self.messages_conn_string
            .delete_blob(topic_id, blob_name.as_str())
            .await
            .unwrap();
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

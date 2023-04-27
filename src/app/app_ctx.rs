use std::sync::Arc;

use my_azure_page_blob_random_access::PageBlobRandomAccess;
use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageConnection};

use rust_extensions::AppStates;

use crate::{
    archive_storage::{ArchiveFileNo, ArchivePageBlobCreator, ArchiveStorageList},
    index_by_minute::{IndexByMinuteUtils, YearlyIndexByMinute},
    settings::{SettingsModel, PAGE_BLOB_MAX_PAGES_TO_UPLOAD_PER_ROUND_TRIP},
    topic_data::TopicsDataList,
    topics_snapshot::current_snapshot::CurrentTopicsSnapshot,
};

use super::{logs::Logs, PrometheusMetrics};

pub const APP_VERSION: &'static str = env!("CARGO_PKG_VERSION");

pub struct AppContext {
    pub app_states: Arc<AppStates>,
    pub topics_snapshot: CurrentTopicsSnapshot,
    pub logs: Arc<Logs>,

    pub topics_list: TopicsDataList,
    pub settings: SettingsModel,
    pub queue_connection: AzureStorageConnection,

    archive_conn_string: Arc<AzureStorageConnection>,
    messages_conn_string: Arc<AzureStorageConnection>,

    pub metrics_keeper: PrometheusMetrics,
    pub index_by_minute_utils: IndexByMinuteUtils,

    topics_and_queue_conn_string: Arc<AzureStorageConnection>,

    pub archive_storage_list: ArchiveStorageList,
}

impl AppContext {
    pub async fn new(settings: SettingsModel) -> AppContext {
        let logs = Arc::new(Logs::new());
        let messages_conn_string = Arc::new(AzureStorageConnection::from_conn_string(
            settings.messages_connection_string.as_str(),
        ));

        let topics_and_queue_conn_string = Arc::new(AzureStorageConnection::from_conn_string(
            settings.queues_connection_string.as_str(),
        ));

        let queue_connection =
            AzureStorageConnection::from_conn_string(settings.queues_connection_string.as_str());

        let archive_conn_string =
            AzureStorageConnection::from_conn_string(settings.archive_connection_string.as_str());

        let topics_repo = settings.get_topics_snapshot_repository().await;

        AppContext {
            topics_snapshot: CurrentTopicsSnapshot::read_or_create(topics_repo).await,
            logs: logs.clone(),
            topics_list: TopicsDataList::new(),
            settings,

            queue_connection,
            metrics_keeper: PrometheusMetrics::new(),
            index_by_minute_utils: IndexByMinuteUtils::new(),
            messages_conn_string,
            app_states: Arc::new(AppStates::create_un_initialized()),
            archive_storage_list: ArchiveStorageList::new(),
            topics_and_queue_conn_string,
            archive_conn_string: Arc::new(archive_conn_string),
        }
    }

    pub fn get_env_info(&self) -> String {
        let env_info = std::env::var("ENV_INFO");

        match env_info {
            Ok(info) => info,
            Err(err) => format!("{:?}", err),
        }
    }

    pub async fn create_topic_container(&self, topic_id: &str) {
        crate::azure_storage_with_retries::create_container_if_not_exists(
            self.messages_conn_string.as_ref(),
            topic_id,
        )
        .await;
    }

    pub async fn open_or_create_index_by_minute(
        &self,
        topic_id: &str,
        year: u32,
    ) -> YearlyIndexByMinute {
        let blob_name = super::file_name_generators::generate_year_index_blob_name(year);

        let azure_storage = AzurePageBlobStorage::new(
            self.messages_conn_string.clone(),
            topic_id.to_string(),
            blob_name,
        )
        .await;

        let page_blob_random_access = PageBlobRandomAccess::new(
            azure_storage,
            true,
            PAGE_BLOB_MAX_PAGES_TO_UPLOAD_PER_ROUND_TRIP,
        );

        YearlyIndexByMinute::open_or_create(year, page_blob_random_access).await
    }

    pub async fn try_open_index_by_minute(
        &self,
        topic_id: &str,
        year: u32,
    ) -> Option<Arc<YearlyIndexByMinute>> {
        let blob_name = super::file_name_generators::generate_year_index_blob_name(year);

        let azure_storage = AzurePageBlobStorage::new(
            self.messages_conn_string.clone(),
            topic_id.to_string(),
            blob_name,
        )
        .await;

        let page_blob_random_access = PageBlobRandomAccess::new(
            azure_storage,
            true,
            PAGE_BLOB_MAX_PAGES_TO_UPLOAD_PER_ROUND_TRIP,
        );

        let result = YearlyIndexByMinute::load_if_exists(year, page_blob_random_access).await?;

        Some(Arc::new(result))
    }

    pub fn get_storage_for_active_pages(&self) -> Arc<AzureStorageConnection> {
        self.topics_and_queue_conn_string.clone()
    }
}

#[async_trait::async_trait]
impl ArchivePageBlobCreator for AppContext {
    async fn create(&self, topic_id: &str, archive_file_no: ArchiveFileNo) -> PageBlobRandomAccess {
        let page_blob_storage = AzurePageBlobStorage::new(
            self.archive_conn_string.clone(),
            topic_id.to_string(),
            archive_file_no.get_file_name(),
        )
        .await;

        PageBlobRandomAccess::new(
            page_blob_storage,
            true,
            PAGE_BLOB_MAX_PAGES_TO_UPLOAD_PER_ROUND_TRIP,
        )
    }
}

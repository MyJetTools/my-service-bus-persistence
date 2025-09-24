use std::{sync::Arc, time::Duration};

use my_azure_storage_sdk::{
    blob_container::BlobContainersApi, page_blob::AzurePageBlobStorage, AzureStorageConnection,
};

use rust_extensions::AppStates;

use crate::{
    archive_storage::{ArchiveFileNo, ArchivePageBlobCreator, ArchiveStorageList},
    index_by_minute::{IndexByMinuteUtils, YearlyIndexByMinute},
    settings::SettingsModel,
    topic_data::TopicsDataList,
    topics_snapshot::current_snapshot::CurrentTopicsSnapshot,
    typing::Year,
};

use super::PrometheusMetrics;

pub const APP_VERSION: &'static str = env!("CARGO_PKG_VERSION");

pub struct AppContext {
    pub app_states: Arc<AppStates>,
    pub topics_snapshot: CurrentTopicsSnapshot,

    pub topics_list: TopicsDataList,
    pub settings: SettingsModel,

    archive_conn_string: Arc<AzureStorageConnection>,
    messages_conn_string: Arc<AzureStorageConnection>,

    pub metrics_keeper: PrometheusMetrics,
    pub index_by_minute_utils: IndexByMinuteUtils,

    topics_and_queue_conn_string: Arc<AzureStorageConnection>,

    pub archive_storage_list: ArchiveStorageList,
}

impl AppContext {
    pub async fn new(settings: SettingsModel) -> AppContext {
        let messages_conn_string = Arc::new(AzureStorageConnection::from_conn_string(
            settings.messages_connection_string.as_str(),
        ));

        let topics_and_queue_conn_string = Arc::new(AzureStorageConnection::from_conn_string(
            settings.topics_connection_string.as_str(),
        ));

        let archive_conn_string =
            AzureStorageConnection::from_conn_string(settings.archive_connection_string.as_str());

        let topics_repo = settings.get_topics_snapshot_repository().await;

        AppContext {
            topics_snapshot: CurrentTopicsSnapshot::read_or_create(topics_repo).await,
            topics_list: TopicsDataList::new(),
            settings,

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
        let conn_string = AzureStorageConnection::from_conn_string(
            self.settings.topics_connection_string.as_str(),
        );

        let mut attempt_no = 0;
        loop {
            let result = conn_string.create_container_if_not_exists(topic_id).await;

            if result.is_ok() {
                break;
            }

            attempt_no += 1;
            if attempt_no > 3 {
                result.unwrap();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    pub async fn open_or_create_index_by_minute(
        &self,
        topic_id: &str,
        year: Year,
    ) -> YearlyIndexByMinute {
        let blob_name = super::file_name_generators::generate_year_index_blob_name(year);

        let page_blob = AzurePageBlobStorage::new(
            self.messages_conn_string.clone(),
            topic_id.to_string(),
            blob_name,
        )
        .await;

        YearlyIndexByMinute::open_or_create(page_blob).await
    }

    pub async fn try_open_index_by_minute(
        &self,
        topic_id: &str,
        year: Year,
    ) -> Option<Arc<YearlyIndexByMinute>> {
        let blob_name = super::file_name_generators::generate_year_index_blob_name(year);

        let page_blob = AzurePageBlobStorage::new(
            self.messages_conn_string.clone(),
            topic_id.to_string(),
            blob_name,
        )
        .await;

        let result = YearlyIndexByMinute::load_if_exists(page_blob).await?;

        Some(Arc::new(result))
    }

    pub fn get_storage_for_active_pages(&self) -> Arc<AzureStorageConnection> {
        self.topics_and_queue_conn_string.clone()
    }
}

#[async_trait::async_trait]
impl ArchivePageBlobCreator for AppContext {
    async fn create(&self, topic_id: &str, archive_file_no: ArchiveFileNo) -> AzurePageBlobStorage {
        AzurePageBlobStorage::new(
            self.archive_conn_string.clone(),
            topic_id.to_string(),
            archive_file_no.get_file_name(),
        )
        .await
    }
}

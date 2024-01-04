use std::{sync::Arc, time::Duration};

use my_azure_page_blob_ext::MyAzurePageBlobStorageWithRetries;
use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageConnection};
use serde::{Deserialize, Serialize};
use tokio::{fs::File, io::AsyncReadExt};

use crate::topics_snapshot::page_blob_storage::TopicsSnapshotPageBlobStorage;

pub const PAGE_BLOB_MAX_PAGES_TO_UPLOAD_PER_ROUND_TRIP: usize = 1024 * 1024 * 3 / 512;

#[derive(Serialize, Deserialize, Debug)]
pub struct SettingsModel {
    #[serde(rename = "TopicsConnectionString")]
    pub topics_connection_string: String,
    #[serde(rename = "MessagesConnectionString")]
    pub messages_connection_string: String,
    #[serde(rename = "ArchiveConnectionString")]
    pub archive_connection_string: String,
    #[serde(rename = "MaxResponseRecordsAmount")]
    pub max_response_records_amount: usize,
    #[serde(rename = "DeleteTopicSecretKey")]
    pub delete_topic_secret_key: String,
}

impl SettingsModel {
    pub async fn get_topics_snapshot_repository(&self) -> TopicsSnapshotPageBlobStorage {
        let connection =
            AzureStorageConnection::from_conn_string(self.topics_connection_string.as_str());
        let page_blob = AzurePageBlobStorage::new(
            Arc::new(connection),
            "topics".to_string(),
            "topicsdata".to_string(),
        )
        .await;

        let page_blob =
            MyAzurePageBlobStorageWithRetries::new(page_blob, 3, Duration::from_secs(1));

        TopicsSnapshotPageBlobStorage::new(page_blob)
    }

    /*
       pub fn get_persist_timer_interval(&self) -> Duration {
           Duration::from_str(&self.persist_timer_interval).unwrap()
       }
    */
    pub async fn read() -> Self {
        let filename = my_service_bus::shared::settings::get_settings_filename_path(
            ".myservicebus-persistence",
        );

        println!("Reading settings file {}", filename);

        let file = File::open(&filename).await;

        if let Err(err) = file {
            panic!(
                "Can not open settings file: {}. The reason is: {:?}",
                filename, err
            );
        }

        let mut file = file.unwrap();

        let mut file_content: Vec<u8> = Vec::new();

        loop {
            let res = file.read_buf(&mut file_content).await.unwrap();

            if res == 0 {
                break;
            }
        }

        let mut result: SettingsModel = serde_yaml::from_slice(file_content.as_slice()).unwrap();

        if result.messages_connection_string.starts_with('~') {
            let home = std::env::var("HOME").unwrap();
            result.messages_connection_string =
                result.messages_connection_string.replace("~", &home);
        }

        result
    }
}

use std::sync::Arc;

use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageConnection};
use serde::{Deserialize, Serialize};
use tokio::{fs::File, io::AsyncReadExt};

use crate::{
    page_blob_random_access::PageBlobRandomAccess,
    toipics_snapshot::blob_repository::TopicsSnapshotBlobRepository,
};

#[derive(Serialize, Deserialize, Debug)]
pub struct SettingsModel {
    #[serde(rename = "QueuesConnectionString")]
    pub queues_connection_string: String,
    #[serde(rename = "MessagesConnectionString")]
    pub messages_connection_string: String,
    #[serde(rename = "LoadBlobPagesSize")]
    pub load_blob_pages_size: usize,
    #[serde(rename = "FlushQueuesSnapshotFreq")]
    pub flush_queues_snapshot_freq: String,
    #[serde(rename = "FlushMessagesFreq")]
    pub flush_messages_freq: String,
    #[serde(rename = "MaxResponseRecordsAmount")]
    pub max_response_records_amount: usize,
    #[serde(rename = "DeleteTopicSecretKey")]
    pub delete_topic_secret_key: String,
    #[serde(rename = "MaxMessageSize")]
    pub max_message_size: usize,
}

impl SettingsModel {
    pub async fn get_topics_snapshot_repository(&self) -> TopicsSnapshotBlobRepository {
        let connection =
            AzureStorageConnection::from_conn_string(self.queues_connection_string.as_str());
        let storage = AzurePageBlobStorage::new(
            Arc::new(connection),
            "topics".to_string(),
            "topicsdata".to_string(),
        )
        .await;

        let blob_random_access = PageBlobRandomAccess::open_or_create(
            storage,
            crate::app::PAGE_BLOB_MAX_PAGES_TO_UPLOAD_PER_ROUND_TRIP,
        )
        .await;

        TopicsSnapshotBlobRepository::new(blob_random_access)
    }

    pub async fn read() -> Self {
        let filename = my_service_bus_shared::settings::get_settings_filename_path(
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

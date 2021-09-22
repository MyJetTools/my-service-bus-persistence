use my_azure_page_blob::MyAzurePageBlob;
use my_azure_storage_sdk::AzureConnection;
use serde::{Deserialize, Serialize};
use tokio::{fs::File, io::AsyncReadExt};

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
}

impl SettingsModel {
    pub fn get_topics_snapshot_page_blob(&self) -> MyAzurePageBlob {
        let connection = AzureConnection::from_conn_string(self.queues_connection_string.as_str());
        MyAzurePageBlob::new(connection, "topics".to_string(), "topicsdata".to_string())
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

        serde_yaml::from_slice(file_content.as_slice()).unwrap()
    }
}

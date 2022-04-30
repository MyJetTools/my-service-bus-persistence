use std::{
    sync::atomic::{AtomicUsize, Ordering},
    usize,
};

use my_azure_storage_sdk::{
    page_blob::{consts::BLOB_PAGE_SIZE, AzurePageBlobStorage},
    AzureStorageError,
};
use my_service_bus_shared::protobuf_models::TopicsSnapshotProtobufModel;

pub struct TopicsSnapshotBlobRepository {
    page_blob: AzurePageBlobStorage,
    blob_size_in_pages: AtomicUsize,
}

impl TopicsSnapshotBlobRepository {
    pub async fn new(page_blob: AzurePageBlobStorage) -> Self {
        let result = Self {
            page_blob,
            blob_size_in_pages: AtomicUsize::new(0),
        };
        result
            .page_blob
            .create_container_if_not_exist()
            .await
            .unwrap();

        result.page_blob.create_if_not_exists(0).await.unwrap();
        result
    }

    pub async fn read(&self) -> Result<TopicsSnapshotProtobufModel, AzureStorageError> {
        let download_result = self.page_blob.download().await;

        match download_result {
            Ok(content) => {
                if content.len() == 0 {
                    self.blob_size_in_pages.store(0, Ordering::SeqCst);
                    return Ok(TopicsSnapshotProtobufModel::create_default());
                }

                self.blob_size_in_pages
                    .store(content.len() / BLOB_PAGE_SIZE, Ordering::SeqCst);

                return Ok(deserialize_model(content.as_slice()));
            }
            Err(err) => {
                if let AzureStorageError::BlobNotFound = &err {
                    self.blob_size_in_pages.store(0, Ordering::SeqCst);
                    self.page_blob.create_if_not_exists(0).await.unwrap();
                    return Ok(TopicsSnapshotProtobufModel::create_default());
                }

                return Err(err);
            }
        }
    }

    pub async fn write(
        &self,
        model: &TopicsSnapshotProtobufModel,
    ) -> Result<(), AzureStorageError> {
        let mut data = Vec::new();
        data.push(0);
        data.push(0);
        data.push(0);
        data.push(0);

        let result = prost::Message::encode(model, &mut data);

        if let Err(err) = result {
            return Err(AzureStorageError::UnknownError {
                msg: format!("Can not serialize model. Err: {:?}", err),
            });
        }

        let len = (data.len() - 4) as u32;

        let len_as_bytes = len.to_le_bytes();

        data[0..4].copy_from_slice(&len_as_bytes[0..4]);

        let required_pages_amount =
            crate::page_blob_random_access::utils::making_size_complient_to_page_blob(
                &mut data,
                BLOB_PAGE_SIZE,
            );

        if self.blob_size_in_pages.load(Ordering::SeqCst) < required_pages_amount {
            self.page_blob.resize(required_pages_amount).await.unwrap();
            self.blob_size_in_pages
                .store(required_pages_amount, Ordering::SeqCst);
        }

        self.page_blob.save_pages(0, data).await?;

        Ok(())
    }
}

fn deserialize_model(content: &[u8]) -> TopicsSnapshotProtobufModel {
    let mut array = [0u8; 4];
    let slice = &content[..4];

    array.copy_from_slice(slice);

    let data_size = u32::from_le_bytes(array) as usize;

    let data = &content[4..data_size + 4];

    let result: Result<TopicsSnapshotProtobufModel, prost::DecodeError> =
        prost::Message::decode(data);

    match result {
        Ok(msg) => {
            println!(
                "Loaded topic snapshot. Topics amount is: {}",
                msg.data.len()
            );

            return msg;
        }
        Err(_) => {
            panic!("The content inside topics_and_queue_blob blob can not be deserialized");
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use super::*;

    use my_azure_storage_sdk::AzureStorageConnection;
    use my_service_bus_shared::protobuf_models::{
        TopicSnapshotProtobufModel, TopicsSnapshotProtobufModel,
    };

    #[tokio::test]
    async fn test_serialize_deserialize() {
        let connection = AzureStorageConnection::new_in_memory();
        let page_blob =
            AzurePageBlobStorage::new(Arc::new(connection), "test".to_string(), "test".to_string())
                .await;

        let repo = TopicsSnapshotBlobRepository::new(page_blob).await;

        // Reading initial model
        let _ = repo.read().await.unwrap();

        let mut src = TopicsSnapshotProtobufModel { data: Vec::new() };

        src.data.push(TopicSnapshotProtobufModel {
            topic_id: "Test".to_string(),
            message_id: 12,
            not_used: 55,
            queues: Vec::new(),
        });

        repo.write(&src).await.unwrap();

        let dest = repo.read().await.unwrap();

        assert_eq!(src.data.len(), dest.data.len());

        assert_eq!(src.data[0].topic_id, dest.data[0].topic_id);
        assert_eq!(src.data[0].message_id, dest.data[0].message_id);
    }
}

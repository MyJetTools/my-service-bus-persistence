use std::{sync::atomic::AtomicUsize, time::Duration, usize};

use my_azure_storage_sdk::{
    page_blob::{AzurePageBlobStorage, PageBlobContentToUpload},
    AzureStorageError,
};
use my_service_bus_shared::protobuf_models::TopicsSnapshotProtobufModel;

pub struct TopicsSnapshotPageBlobStorage {
    page_blob: AzurePageBlobStorage,
    size: AtomicUsize,
}

impl TopicsSnapshotPageBlobStorage {
    pub fn new(page_blob: AzurePageBlobStorage) -> Self {
        Self {
            page_blob,
            size: AtomicUsize::new(0),
        }
    }

    async fn update_size(&self, page_blob_content: &PageBlobContentToUpload) {
        let current_size = self.size.load(std::sync::atomic::Ordering::SeqCst);

        if current_size == page_blob_content.content_size() {
            return;
        }

        self.page_blob
            .resize(page_blob_content.get_size_in_pages())
            .await
            .unwrap();

        self.size.store(
            page_blob_content.content_size(),
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    pub async fn read_or_create_topics_snapshot(
        &self,
    ) -> Result<TopicsSnapshotProtobufModel, AzureStorageError> {
        let mut attempt_no = 0;
        loop {
            let download_result = self.page_blob.download().await;
            match download_result {
                Ok(result) => {
                    self.size
                        .store(result.len(), std::sync::atomic::Ordering::SeqCst);
                    return Ok(deserialize_model(result.as_slice()));
                }
                Err(err) => {
                    let result = crate::azure_storage_with_retries::handle_error_and_create_blob(
                        &self.page_blob,
                        err,
                        0,
                    )
                    .await?;

                    if attempt_no > 5 {
                        return Err(result);
                    }

                    println!(
                        "Can not read topics snapshot. Attempt: {}. Err: {:?}",
                        attempt_no, result
                    );
                    attempt_no += 1;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    pub async fn write_topics_snapshot(
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

        let page_blob_content = PageBlobContentToUpload::new(data, 0);

        loop {
            let result = self
                .page_blob
                .save_pages(0, page_blob_content.clone())
                .await;

            if result.is_ok() {
                return Ok(());
            }

            let err = result.unwrap_err();

            match err {
                AzureStorageError::InvalidPageRange => {
                    self.update_size(&page_blob_content).await;
                }

                _ => return Err(err),
            }
        }
    }
}

#[async_trait::async_trait]
pub trait TopicsSnapshotPageBlobExts {
    async fn read_or_create_topics_snapshot(
        &self,
    ) -> Result<TopicsSnapshotProtobufModel, AzureStorageError>;

    async fn write_topics_snapshot(
        &self,
        topics_snapshot: &TopicsSnapshotProtobufModel,
    ) -> Result<(), AzureStorageError>;
}

fn deserialize_model(content: &[u8]) -> TopicsSnapshotProtobufModel {
    if content.len() == 0 {
        return TopicsSnapshotProtobufModel::default();
    }
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

    use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageConnection};
    use my_service_bus_abstractions::AsMessageId;
    use my_service_bus_shared::protobuf_models::{
        TopicSnapshotProtobufModel, TopicsSnapshotProtobufModel,
    };

    #[tokio::test]
    async fn test_serialize_deserialize() {
        let connection = AzureStorageConnection::new_in_memory();
        let page_blob =
            AzurePageBlobStorage::new(Arc::new(connection), "test".to_string(), "test".to_string())
                .await;

        let page_blob_storage = TopicsSnapshotPageBlobStorage::new(page_blob);

        //   page_blob.create_container_if_not_exists().await.unwrap();
        //    page_blob.create(0).await.unwrap();

        // Reading initial model
        let result = page_blob_storage
            .read_or_create_topics_snapshot()
            .await
            .unwrap();

        assert_eq!(result.data.len(), 0);

        let src = TopicsSnapshotProtobufModel {
            data: vec![TopicSnapshotProtobufModel::new(
                "Test".to_string(),
                12.as_message_id(),
                vec![],
            )],
        };

        page_blob_storage.write_topics_snapshot(&src).await.unwrap();

        let dest = page_blob_storage
            .read_or_create_topics_snapshot()
            .await
            .unwrap();

        assert_eq!(src.data.len(), dest.data.len());

        assert_eq!(src.data[0].topic_id, dest.data[0].topic_id);
        assert_eq!(
            src.data[0].get_message_id().get_value(),
            dest.data[0].get_message_id().get_value()
        );
    }
}

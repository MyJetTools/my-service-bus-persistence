use std::{sync::atomic::AtomicUsize, usize};

use my_azure_storage_sdk::{
    page_blob::{MyAzurePageBlobStorage, PageBlobContentToUpload},
    AzureStorageError,
};
use prost::Message;

use super::protobuf_model::*;
use my_azure_page_blob_ext::MyAzurePageBlobStorageWithRetries;

pub enum TopicsSnapshotResult {
    V1(TopicsSnapshotProtobufModel),
    V2(TopicsSnapshotProtobufModelV2),
}

impl TopicsSnapshotResult {
    pub fn default() -> Self {
        TopicsSnapshotResult::V2(TopicsSnapshotProtobufModelV2::default())
    }

    pub fn get_result(self) -> TopicsSnapshotProtobufModelV2 {
        match self {
            TopicsSnapshotResult::V1(data) => TopicsSnapshotProtobufModelV2 {
                data: data.data,
                deleted_topics: Vec::new(),
            },
            TopicsSnapshotResult::V2(result) => result,
        }
    }
}

impl Into<TopicsSnapshotResult> for TopicsSnapshotProtobufModelV2 {
    fn into(self) -> TopicsSnapshotResult {
        TopicsSnapshotResult::V2(self)
    }
}

impl Into<TopicsSnapshotResult> for TopicsSnapshotProtobufModel {
    fn into(self) -> TopicsSnapshotResult {
        TopicsSnapshotResult::V1(self)
    }
}

pub struct TopicsSnapshotPageBlobStorage {
    page_blob: MyAzurePageBlobStorageWithRetries,
    size: AtomicUsize,
}

impl TopicsSnapshotPageBlobStorage {
    pub fn new(page_blob: MyAzurePageBlobStorageWithRetries) -> Self {
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
    ) -> Result<TopicsSnapshotResult, AzureStorageError> {
        let download_result = self.page_blob.download().await;
        match download_result {
            Ok(result) => {
                self.size
                    .store(result.len(), std::sync::atomic::Ordering::SeqCst);
                return Ok(deserialize_model(result.as_slice()));
            }
            Err(err) => match err {
                AzureStorageError::ContainerNotFound => {
                    self.page_blob.create_if_not_exists(0, true).await.unwrap();
                    return Ok(TopicsSnapshotResult::default());
                }
                AzureStorageError::BlobNotFound => {
                    self.page_blob.create_if_not_exists(0, true).await.unwrap();
                    return Ok(TopicsSnapshotResult::default());
                }
                _ => panic!("Can not read topics snapshot. Err: {:?}", err),
            },
        }
    }

    pub async fn write_topics_snapshot<TModel: Message>(
        &self,
        model: &TModel,
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

fn deserialize_model(content: &[u8]) -> TopicsSnapshotResult {
    if content.len() == 0 {
        return TopicsSnapshotResult::default();
    }
    let mut array = [0u8; 4];
    let slice = &content[..4];

    array.copy_from_slice(slice);

    let data_size = u32::from_le_bytes(array) as usize;

    let data = &content[4..data_size + 4];

    let result: Result<TopicsSnapshotProtobufModelV2, prost::DecodeError> =
        prost::Message::decode(data);

    match result {
        Ok(msg) => {
            println!(
                "Loaded topic snapshot V2. Topics amount is: {}. Deleted topics amount is: {}",
                msg.data.len(),
                msg.deleted_topics.len()
            );

            return msg.into();
        }
        Err(_) => {
            println!("Can not deserialize V2 topics. Trying to deserialize V1");
        }
    }

    let result: Result<TopicsSnapshotProtobufModel, prost::DecodeError> =
        prost::Message::decode(data);

    match result {
        Ok(msg) => {
            println!(
                "Loaded topic snapshot V1. Topics amount is: {}",
                msg.data.len()
            );

            return msg.into();
        }
        Err(_) => {
            panic!("Can not deserialize V2 topics. Trying to deserialize V1");
        }
    }
}

#[cfg(test)]
mod tests {

    use std::{sync::Arc, time::Duration};

    use super::*;

    use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageConnection};
    use my_service_bus::abstractions::AsMessageId;

    #[tokio::test]
    async fn test_serialize_deserialize_v1() {
        let connection = AzureStorageConnection::new_in_memory();
        let page_blob =
            AzurePageBlobStorage::new(Arc::new(connection), "test".to_string(), "test".to_string())
                .await;

        let page_blob =
            MyAzurePageBlobStorageWithRetries::new(page_blob, 3, Duration::from_secs(1));

        let page_blob_storage = TopicsSnapshotPageBlobStorage::new(page_blob);

        //   page_blob.create_container_if_not_exists().await.unwrap();
        //    page_blob.create(0).await.unwrap();

        // Reading initial model
        let result = page_blob_storage
            .read_or_create_topics_snapshot()
            .await
            .unwrap()
            .get_result();

        assert_eq!(result.data.len(), 0);

        let src = TopicsSnapshotProtobufModel {
            data: vec![TopicSnapshotProtobufModel::new(
                "Test".to_string(),
                12.as_message_id(),
                vec![],
                true.into(),
            )],
        };

        page_blob_storage.write_topics_snapshot(&src).await.unwrap();

        let dest = page_blob_storage
            .read_or_create_topics_snapshot()
            .await
            .unwrap()
            .get_result();

        assert_eq!(src.data.len(), dest.data.len());

        assert_eq!(src.data[0].topic_id, dest.data[0].topic_id);
        assert_eq!(
            src.data[0].get_message_id().get_value(),
            dest.data[0].get_message_id().get_value()
        );
    }

    #[tokio::test]
    async fn test_serialize_deserialize_v2() {
        let connection = AzureStorageConnection::new_in_memory();
        let page_blob =
            AzurePageBlobStorage::new(Arc::new(connection), "test".to_string(), "test".to_string())
                .await;

        let page_blob =
            MyAzurePageBlobStorageWithRetries::new(page_blob, 3, Duration::from_secs(1));

        let page_blob_storage = TopicsSnapshotPageBlobStorage::new(page_blob);

        //   page_blob.create_container_if_not_exists().await.unwrap();
        //    page_blob.create(0).await.unwrap();

        // Reading initial model
        let result = page_blob_storage
            .read_or_create_topics_snapshot()
            .await
            .unwrap()
            .get_result();

        assert_eq!(result.data.len(), 0);

        let src = TopicsSnapshotProtobufModelV2 {
            data: vec![TopicSnapshotProtobufModel::new(
                "Test".to_string(),
                12.as_message_id(),
                vec![],
                true.into(),
            )],
            deleted_topics: vec![],
        };

        page_blob_storage.write_topics_snapshot(&src).await.unwrap();

        let dest = page_blob_storage
            .read_or_create_topics_snapshot()
            .await
            .unwrap()
            .get_result();

        assert_eq!(src.data.len(), dest.data.len());

        assert_eq!(src.data[0].topic_id, dest.data[0].topic_id);
        assert_eq!(
            src.data[0].get_message_id().get_value(),
            dest.data[0].get_message_id().get_value()
        );
    }
}

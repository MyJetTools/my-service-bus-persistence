use std::usize;

use my_azure_storage_sdk::AzureStorageError;
use my_service_bus_shared::protobuf_models::TopicsSnapshotProtobufModel;

use crate::page_blob_random_access::PageBlobRandomAccess;

pub struct TopicsSnapshotBlobRepository {
    page_blob: PageBlobRandomAccess,
}

impl TopicsSnapshotBlobRepository {
    pub fn new(page_blob: PageBlobRandomAccess) -> Self {
        Self { page_blob }
    }

    pub async fn read(&mut self) -> Result<TopicsSnapshotProtobufModel, AzureStorageError> {
        let download_result = self.page_blob.download(Some(0)).await;

        if download_result.len() == 0 {
            return Ok(TopicsSnapshotProtobufModel::create_default());
        }

        return Ok(deserialize_model(download_result.as_slice()));
    }

    pub async fn write(
        &mut self,
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

        self.page_blob
            .write_at_position(0, data.as_slice(), 1)
            .await;

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

        let blob_random_access = PageBlobRandomAccess::open_or_create(page_blob, 1024).await;

        let mut repo = TopicsSnapshotBlobRepository::new(blob_random_access);

        // Reading initial model
        let _ = repo.read().await.unwrap();

        let mut src = TopicsSnapshotProtobufModel { data: Vec::new() };

        src.data.push(TopicSnapshotProtobufModel::new(
            "Test".to_string(),
            12.as_message_id(),
            vec![],
        ));

        repo.write(&src).await.unwrap();

        let dest = repo.read().await.unwrap();

        assert_eq!(src.data.len(), dest.data.len());

        assert_eq!(src.data[0].topic_id, dest.data[0].topic_id);
        assert_eq!(
            src.data[0].get_message_id().get_value(),
            dest.data[0].get_message_id().get_value()
        );
    }
}

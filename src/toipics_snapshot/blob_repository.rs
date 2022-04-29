use std::usize;

use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::AzureStorageError;
use my_service_bus_shared::protobuf_models::TopicsSnapshotProtobufModel;

pub async fn read_from_blob<TMyPageBlob: MyPageBlob>(
    my_page_blob: &mut TMyPageBlob,
) -> Result<TopicsSnapshotProtobufModel, AzureStorageError> {
    my_page_blob.create_container_if_not_exist().await.unwrap();

    let download_result = my_page_blob.download().await;

    match download_result {
        Ok(content) => {
            return Ok(deserialize_model(content.as_slice()));
        }
        Err(err) => {
            if let AzureStorageError::BlobNotFound = &err {
                my_page_blob.create_if_not_exists(0).await.unwrap();
                return Ok(TopicsSnapshotProtobufModel::create_default());
            }

            return Err(err);
        }
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

pub async fn write_to_blob<TMyPageBlob: MyPageBlob>(
    my_page_blob: &mut TMyPageBlob,
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

    my_page_blob.save_pages(0, data).await?;

    Ok(())
}

#[cfg(test)]
mod tests {

    use my_azure_page_blob::*;

    use super::*;

    use my_service_bus_shared::protobuf_models::{
        TopicSnapshotProtobufModel, TopicsSnapshotProtobufModel,
    };

    #[tokio::test]
    async fn test_serialize_deserialize() {
        let mut page_blob = MyPageBlobMock::new();

        page_blob.create_container_if_not_exist().await.unwrap();
        page_blob.create_if_not_exists(0).await.unwrap();

        let mut src = TopicsSnapshotProtobufModel { data: Vec::new() };

        src.data.push(TopicSnapshotProtobufModel {
            topic_id: "Test".to_string(),
            message_id: 12,
            not_used: 55,
            queues: Vec::new(),
        });

        write_to_blob(&mut page_blob, &src).await.unwrap();

        let mut dest = read_from_blob(&mut page_blob).await.unwrap();

        assert_eq!(src.data.len(), dest.data.len());

        let src = src.data.remove(0);

        let dest = dest.data.remove(0);

        assert_eq!(src.topic_id, dest.topic_id);
        assert_eq!(src.message_id, dest.message_id);
    }
}

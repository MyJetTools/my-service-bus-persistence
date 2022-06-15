use my_service_bus_shared::{page_compressor, protobuf_models::MessageProtobufModel};
use std::collections::HashMap;

use crate::{grpc::contracts::NewMessagesProtobufContract, message_pages::MessagePageId};

pub struct NewMessagesGrpcContract {
    pub topic_id: String,
    pub messages_by_page: HashMap<i64, Vec<MessageProtobufModel>>,
}

pub async fn unzip_and_deserialize(
    req: &mut tonic::Streaming<crate::persistence_grpc::CompressedMessageChunkModel>,
) -> Result<NewMessagesGrpcContract, tonic::Status> {
    let mut payload: Vec<u8> = Vec::new();

    loop {
        let next = req.message().await?;

        if next.is_none() {
            break;
        }

        let next = next.unwrap();
        payload.extend(next.chunk);
    }

    let unzipped = page_compressor::zip::decompress_payload(payload.as_slice()).unwrap();

    let contract = NewMessagesProtobufContract::parse(unzipped.as_slice());

    let mut messages_by_page: HashMap<i64, Vec<MessageProtobufModel>> = HashMap::new();

    for msg in contract.messages {
        let page_id = MessagePageId::from_message_id(msg.message_id);

        if !messages_by_page.contains_key(&page_id.value) {
            messages_by_page.insert(page_id.value, Vec::new());
        }

        let messages = messages_by_page.get_mut(&page_id.value).unwrap();

        messages.push(msg);
    }

    Ok(NewMessagesGrpcContract {
        messages_by_page,
        topic_id: contract.topic_id,
    })
}

pub async fn deserialize_uncompressed(
    req: &mut tonic::Streaming<crate::persistence_grpc::UnCompressedMessageChunkModel>,
) -> Result<NewMessagesGrpcContract, tonic::Status> {
    let mut payload: Vec<u8> = Vec::new();

    loop {
        let next = req.message().await?;

        if next.is_none() {
            break;
        }

        let next = next.unwrap();
        payload.extend(next.chunk);
    }

    let contract = NewMessagesProtobufContract::parse(payload.as_slice());

    let mut messages_by_page: HashMap<i64, Vec<MessageProtobufModel>> = HashMap::new();

    for msg in contract.messages {
        let page_id = MessagePageId::from_message_id(msg.message_id);

        if !messages_by_page.contains_key(&page_id.value) {
            messages_by_page.insert(page_id.value, Vec::new());
        }

        let messages = messages_by_page.get_mut(&page_id.value).unwrap();

        messages.push(msg);
    }

    Ok(NewMessagesGrpcContract {
        messages_by_page,
        topic_id: contract.topic_id,
    })
}

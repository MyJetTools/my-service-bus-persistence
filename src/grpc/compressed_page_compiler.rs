use std::sync::Arc;

use my_service_bus_shared::{
    page_compressor::CompressedPageBuilder,
    protobuf_models::{MessageProtobufModel, MessagesProtobufModel},
    MessageId,
};

use crate::{
    app::AppContext,
    operations::read_page::{MessagesReader, ReadCondition},
    persistence_grpc::*,
    topic_data::TopicData,
};

pub fn get_none_message() -> MessageContentGrpcModel {
    MessageContentGrpcModel {
        created: None,
        data: Vec::new(),
        meta_data: Vec::new(),
        message_id: -1,
    }
}

pub async fn get_v0(
    app: Arc<AppContext>,
    topic_data: Arc<TopicData>,
    max_payload_size: usize,
    from_id: MessageId,
    to_id: MessageId,
) -> Vec<Vec<u8>> {
    let mut messages = Vec::new();

    let mut messages_reader = MessagesReader::new(
        app.clone(),
        topic_data.clone(),
        ReadCondition::as_from_to(from_id, to_id),
    );

    while let Some(msgs) = messages_reader.get_next_chunk().await {
        for msg in msgs {
            messages.push(MessageProtobufModel {
                message_id: msg.message_id,
                created: msg.created,
                data: msg.data,
                headers: msg.headers,
            });
        }
    }

    let messages = MessagesProtobufModel { messages };

    let mut uncompressed = Vec::new();
    messages.serialize(&mut uncompressed).unwrap();

    let compressed =
        my_service_bus_shared::page_compressor::zip::compress_payload(uncompressed.as_slice())
            .unwrap();

    let result = split(compressed.as_slice(), max_payload_size);
    return result;
}

pub async fn get_v1(
    app: Arc<AppContext>,
    topic_data: Arc<TopicData>,
    max_payload_size: usize,
    from_id: MessageId,
    to_id: MessageId,
) -> Vec<Vec<u8>> {
    let mut zip_builder = CompressedPageBuilder::new();

    let mut messages_reader = MessagesReader::new(
        app.clone(),
        topic_data.clone(),
        ReadCondition::Range {
            from_id,
            to_id: Some(to_id),
            max_amount: None,
        },
    );

    let mut messages = 0;

    let mut used_messages = 0;

    while let Some(msgs) = messages_reader.get_next_chunk().await {
        for msg in &msgs {
            let mut buffer = Vec::new();
            msg.serialize(&mut buffer).unwrap();
            zip_builder
                .add_message(msg.message_id, buffer.as_slice())
                .unwrap();
            used_messages += 1;
            messages += 1;
        }
    }

    let zip_payload = zip_builder.get_payload().unwrap();

    println!(
        "Sending zip_2 for topic {}/{}. Size {}. Messages: {}-{}. Filtered: {}",
        topic_data.topic_id.as_str(),
        from_id,
        to_id,
        zip_payload.len(),
        messages,
        used_messages
    );

    let result = split(zip_payload.as_slice(), max_payload_size);
    return result;
}

fn split(src: &[u8], max_payload_size: usize) -> Vec<Vec<u8>> {
    let mut result: Vec<Vec<u8>> = Vec::new();

    let mut pos: usize = 0;

    while pos < src.len() {
        if src.len() - pos < max_payload_size {
            let payload = &src[pos..];
            result.push(payload.to_vec());
            break;
        }
        let payload = &src[pos..pos + max_payload_size];
        result.push(payload.to_vec());
        pos += max_payload_size;
    }

    result
}

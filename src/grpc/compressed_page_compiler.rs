use my_service_bus_shared::{
    page_compressor::CompressedPageBuilder,
    protobuf_models::{MessageProtobufModel, MessagesProtobufModel},
    MessageId,
};

use crate::{message_pages::MessagesPage, operations::OperationError, persistence_grpc::*};

pub fn get_none_message() -> MessageContentGrpcModel {
    MessageContentGrpcModel {
        created: None,
        data: Vec::new(),
        meta_data: Vec::new(),
        message_id: -1,
    }
}

pub struct MsgRange {
    pub msg_from: i64,
    pub msg_to: i64,
}

pub async fn get_v0(
    page: &MessagesPage,
    max_payload_size: usize,
    current_message_id: MessageId,
) -> Result<Vec<Vec<u8>>, OperationError> {
    let arced_messages = page.get_all(Some(current_message_id)).await;

    let mut messages = Vec::with_capacity(arced_messages.len());
    for acred_message in arced_messages {
        messages.push(MessageProtobufModel {
            message_id: acred_message.message_id,
            created: acred_message.created,
            data: acred_message.data.clone(),
            headers: acred_message.headers.clone(),
        });
    }

    let messages = MessagesProtobufModel { messages };

    let mut uncompressed = Vec::new();
    messages.serialize(&mut uncompressed).unwrap();

    let compressed =
        my_service_bus_shared::page_compressor::zip::compress_payload(uncompressed.as_slice())?;

    let result = split(compressed.as_slice(), max_payload_size);
    return Ok(result);
}

pub async fn get_v1(
    topic_id: &str,
    page: &MessagesPage,
    max_payload_size: usize,
    range: Option<MsgRange>,
    current_message_id: MessageId,
) -> Result<Vec<Vec<u8>>, OperationError> {
    let messages_snapshot = if let Some(range) = range {
        page.get_range(range.msg_from, range.msg_to).await
    } else {
        page.get_all(Some(current_message_id)).await
    };

    if messages_snapshot.len() == 0 {
        return Ok(vec![]);
    }

    let mut zip_builder = CompressedPageBuilder::new();

    let mut messages = 0;

    let mut used_messages = 0;

    for msg in messages_snapshot {
        let mut buffer = Vec::new();
        msg.serialize(&mut buffer)?;
        zip_builder.add_message(msg.message_id, buffer.as_slice())?;
        used_messages += 1;

        messages += 1;
    }

    let zip_payload = zip_builder.get_payload()?;

    println!(
        "Sending zip_2 for topic {}/{}. Size {}. Messages: {}. Filtered: {}",
        topic_id,
        page.get_page_id(),
        zip_payload.len(),
        messages,
        used_messages
    );

    let result = split(zip_payload.as_slice(), max_payload_size);
    return Ok(result);
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

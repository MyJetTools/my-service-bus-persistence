use my_service_bus_shared::{
    page_compressor::CompressedPageBuilder, protobuf_models::MessagesProtobufModel,
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
) -> Result<Vec<Vec<u8>>, OperationError> {
    let messages = page.get_grpc_v0_snapshot().await?;

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
) -> Result<Vec<Vec<u8>>, OperationError> {
    match &*page.data.read().await {
        crate::message_pages::MessagesPageData::NotInitialized(_) => {
            panic!("Can not get data from not initialized page"); //ToDo - Initialize it here
        }
        crate::message_pages::MessagesPageData::Uncompressed(uncompressed_page) => {
            let mut zip_builder = CompressedPageBuilder::new();

            let mut messages = 0;

            let mut used_messages = 0;

            for msg in uncompressed_page.messages.values() {
                if let Some(range) = &range {
                    if range.msg_from <= msg.message_id && msg.message_id <= range.msg_to {
                        let mut buffer = Vec::new();
                        msg.serialize(&mut buffer)?;
                        zip_builder.add_message(msg.message_id, buffer.as_slice())?;
                        used_messages += 1;
                    }
                } else {
                    let mut buffer = Vec::new();
                    msg.serialize(&mut buffer)?;
                    zip_builder.add_message(msg.message_id, buffer.as_slice())?;
                    used_messages += 1;
                }
                messages += 1;
            }

            let zip_payload = zip_builder.get_payload()?;

            println!(
                "Sending zip_2 for topic {}/{}. Size {}. Messages: {}. Filtered: {}",
                topic_id,
                page.page_id.value,
                zip_payload.len(),
                messages,
                used_messages
            );

            let result = split(zip_payload.as_slice(), max_payload_size);
            return Ok(result);
        }
        crate::message_pages::MessagesPageData::Compressed(compressed_page) => {
            if let Some(range) = &range {
                let mut zip_builder = CompressedPageBuilder::new();
                for msg_id in range.msg_from..range.msg_to {
                    let msg = compressed_page.get(msg_id)?;

                    if let Some(msg) = msg {
                        let mut buffer = Vec::new();
                        msg.serialize(&mut buffer)?;
                        zip_builder.add_message(msg.message_id, buffer.as_slice())?;
                    }
                }

                let zip_payload = zip_builder.get_payload()?;

                let result = split(zip_payload.as_slice(), max_payload_size);
                return Ok(result);
            } else {
                let result = split(compressed_page.zip_data.as_slice(), max_payload_size);
                return Ok(result);
            }
        }
        crate::message_pages::MessagesPageData::Blank(_) => {
            return Ok(Vec::new());
        }
    }
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

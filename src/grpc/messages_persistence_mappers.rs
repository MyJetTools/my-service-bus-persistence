use my_service_bus_shared::{
    bcl::BclDateTime,
    page_compressor::CompressedPageBuilder,
    protobuf_models::{MessageMetaDataProtobufModel, MessageProtobufModel},
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

impl Into<MessageContentMetaDataItem> for MessageMetaDataProtobufModel {
    fn into(self) -> MessageContentMetaDataItem {
        MessageContentMetaDataItem {
            key: self.key,
            value: self.value,
        }
    }
}

impl Into<DateTime> for BclDateTime {
    fn into(self) -> DateTime {
        DateTime {
            kind: self.kind,
            value: self.value,
            scale: self.scale,
        }
    }
}

pub fn to_contract_message_metadata(
    src: &MessageMetaDataProtobufModel,
) -> MessageContentMetaDataItem {
    MessageContentMetaDataItem {
        key: src.key.to_string(),
        value: src.value.to_string(),
    }
}

pub fn to_message_grpc_contract(src: &MessageProtobufModel) -> MessageContentGrpcModel {
    let created: Option<DateTime> = match src.created {
        Some(created) => Some(created.into()),
        None => None,
    };

    MessageContentGrpcModel {
        data: src.data.clone(),
        created,
        message_id: src.message_id,
        meta_data: src
            .metadata
            .iter()
            .map(|itm| to_contract_message_metadata(itm))
            .collect(),
    }
}

pub struct MsgRange {
    pub msg_from: i64,
    pub msg_to: i64,
}

pub async fn get_compressed_page(
    page: &MessagesPage,
    max_payload_size: usize,
    range: Option<MsgRange>,
) -> Result<Vec<Vec<u8>>, OperationError> {
    match &*page.data.lock().await {
        crate::message_pages::MessagesPageData::NotInitialized(_) => {
            panic!("Can not get data from not initialized page")
        }
        crate::message_pages::MessagesPageData::Uncompressed(uncompressed_page) => {
            let mut zip_builder = CompressedPageBuilder::new();

            for msg in uncompressed_page.messages.values() {
                if let Some(range) = &range {
                    if range.msg_from <= msg.message_id && msg.message_id <= range.msg_to {
                        let mut buffer = Vec::new();
                        msg.serialize(&mut buffer)?;
                        zip_builder.add_message(msg.message_id, buffer.as_slice())?;
                    }
                } else {
                    let mut buffer = Vec::new();
                    msg.serialize(&mut buffer)?;
                    zip_builder.add_message(msg.message_id, buffer.as_slice())?;
                }
            }

            let zip_payload = zip_builder.get_payload()?;

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

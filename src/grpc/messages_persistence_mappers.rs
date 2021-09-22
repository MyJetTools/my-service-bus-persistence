use my_service_bus_shared::{
    bcl::BclDateTime, MessageMetaDataProtobufModel, MessageProtobufModel, MessagesProtobufModel,
};

use crate::{app::AppError, message_pages::MessagesPage, persistence_grpc::*};

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

pub async fn get_compressed_page(
    page: &MessagesPage,
    max_payload_size: usize,
    msg_from: i64,
    msg_to: i64,
) -> Result<Vec<Vec<u8>>, AppError> {
    let mut protobuf_model = MessagesProtobufModel {
        messages: Vec::with_capacity(100_000),
    };

    let read_access = page.data.read().await;

    let read_access = read_access.get(0).unwrap();

    if msg_from == 0 && msg_to == 0 {
        for msg in read_access.messages.values() {
            protobuf_model.messages.push(msg.clone());
        }
    } else {
        for msg in read_access.messages.values() {
            if msg.message_id >= msg_from && msg.message_id <= msg_to {
                protobuf_model.messages.push(msg.clone());
            }
        }
    }

    let mut encoded_payload: Vec<u8> = Vec::new();
    prost::Message::encode(&protobuf_model, &mut encoded_payload).unwrap();

    let zipped =
        my_service_bus_shared::page_compressor::zip::compress_payload(encoded_payload.as_slice())?;

    return Ok(split(zipped.as_slice(), max_payload_size));
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

use my_service_bus_shared::protobuf_models::{MessageProtobufModel, MessagesProtobufModel};

use crate::app::{AppError, Logs};
use crate::message_pages::MessagePageId;
use crate::utils::StopWatch;

const PAGES_IN_CLUSTER: i64 = 100;

#[derive(Clone, Copy)]
pub struct ClusterPageId {
    pub value: i64,
}

impl ClusterPageId {
    pub fn from_page_id(page_id: &MessagePageId) -> Self {
        Self {
            value: page_id.value / PAGES_IN_CLUSTER,
        }
    }

    pub fn get_first_page_id_on_compressed_page(&self) -> MessagePageId {
        let page_id = self.value * PAGES_IN_CLUSTER;

        MessagePageId { value: page_id }
    }
}

pub async fn decompress_cluster(
    payload: &[u8],
    topic_id: &str,
    logs: &Logs,
) -> Result<Option<Vec<MessageProtobufModel>>, AppError> {
    let mut sw = StopWatch::new();
    sw.start();
    let unzipped = my_service_bus_shared::page_compressor::zip::decompress_payload(payload)?;
    sw.pause();

    logs.add_info_string(
        Some(topic_id),
        "decompress_from_cluster",
        format!("Unzipped {:?}", sw.duration()),
    )
    .await;

    sw.start();
    let protobuf_messages = MessagesProtobufModel::parse(unzipped.as_slice());

    sw.pause();

    logs.add_info_string(
        Some(topic_id),
        "decompress_from_cluster",
        format!("Converted {:?}", sw.duration()),
    )
    .await;

    Ok(Some(protobuf_messages.messages))
}

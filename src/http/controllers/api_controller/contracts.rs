use std::usize;

use crate::{
    app::AppContext, topic_data::TopicData, uncompressed_page::MESSAGES_PER_PAGE,
    utils::duration_to_string,
};
use my_service_bus_shared::protobuf_models::{
    QueueSnapshotProtobufModel, TopicSnapshotProtobufModel,
};
use rust_extensions::date_time::DateTimeAsMicroseconds;
use serde::{Deserialize, Serialize};

use sysinfo::SystemExt;

#[derive(Serialize, Deserialize, Debug)]
struct SystemStatusModel {
    usedmem: u64,
    totalmem: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct QueueRangeStatusModel {
    #[serde(rename = "fromId")]
    from_id: i64,
    #[serde(rename = "toId")]
    to_id: i64,
}

#[derive(Serialize, Deserialize, Debug)]
struct QueueStatusModel {
    #[serde(rename = "queueId")]
    queue_id: String,
    #[serde(rename = "queueType")]
    queue_type: i32,
    ranges: Vec<QueueRangeStatusModel>,
}

fn get_queues(queue_snapshot: &Vec<QueueSnapshotProtobufModel>) -> Vec<QueueStatusModel> {
    let mut result = Vec::new();
    for q in queue_snapshot {
        let model = QueueStatusModel {
            queue_id: q.queue_id.to_string(),
            queue_type: 0,
            ranges: q
                .ranges
                .iter()
                .map(|r| QueueRangeStatusModel {
                    from_id: r.from_id,
                    to_id: r.to_id,
                })
                .collect(),
        };

        result.push(model);
    }

    result
}

#[derive(Serialize, Deserialize, Debug)]
struct TopicInfo {
    #[serde(rename = "topicId")]
    topic_id: String,

    #[serde(rename = "messageId")]
    message_id: i64,

    #[serde(rename = "savedMessageId")]
    saved_message_id: i64,

    #[serde(rename = "lastSaveChunk")]
    last_save_chunk: usize,

    #[serde(rename = "lastSaveDur")]
    last_save_duration: String,

    #[serde(rename = "lastSaveMoment")]
    last_save_moment: String,

    #[serde(rename = "loadedPages")]
    loaded_pages: Vec<LoadedPageModel>,

    #[serde(rename = "activePages")]
    active_pages: Vec<i64>,

    queues: Vec<QueueStatusModel>,

    #[serde(rename = "queueSize")]
    queue_size: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct LoadedPageModel {
    #[serde(rename = "pageId")]
    page_id: i64,

    #[serde(rename = "hasSkipped")]
    has_skipped_messages: bool,

    #[serde(rename = "percent")]
    percent: usize,

    #[serde(rename = "count")]
    count: usize,

    #[serde(rename = "writePosition")]
    write_position: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StatusModel {
    #[serde(rename = "queuesSnapshotId")]
    queues_snapshot_id: i64,
    #[serde(rename = "activeOperations")]
    active_operations: Vec<String>,
    #[serde(rename = "awaitingOperations")]
    awaiting_operations: Vec<String>,
    #[serde(rename = "topics")]
    topics: Vec<TopicInfo>,
    system: SystemStatusModel,
    #[serde(skip_serializing_if = "Option::is_none")]
    initialing: Option<bool>,
}

impl StatusModel {
    pub async fn new(app: &AppContext) -> StatusModel {
        let topics_snapshot = app.topics_snapshot.get().await;

        let mut topics = Vec::new();
        let now = DateTimeAsMicroseconds::now();

        for snapshot in &topics_snapshot.snapshot.data {
            let data_by_topic = app.topics_list.get(snapshot.topic_id.as_str()).await;

            if data_by_topic.is_none() {
                continue;
            }

            let data_by_topic = data_by_topic.unwrap();

            let topic_info_model = get_topics_model(snapshot, data_by_topic.as_ref(), now).await;

            topics.push(topic_info_model)
        }

        let mut sys_info = sysinfo::System::new_all();

        // First we update all information of our system struct.
        sys_info.refresh_all();

        let model = StatusModel {
            initialing: match app.is_initialized() {
                true => None,
                false => Some(true),
            },
            queues_snapshot_id: topics_snapshot.snapshot_id,
            active_operations: Vec::new(),
            awaiting_operations: Vec::new(),
            topics,
            system: SystemStatusModel {
                totalmem: sys_info.total_memory(),
                usedmem: sys_info.used_memory(),
            },
        };

        return model;
    }
}
async fn get_loaded_pages(topic_data: &TopicData) -> Vec<LoadedPageModel> {
    let mut result: Vec<LoadedPageModel> = Vec::new();

    for page in topic_data.uncompressed_pages_list.get_all().await {
        let count = page.metrics.get_messages_count();
        let percent = (count as f64) / (MESSAGES_PER_PAGE as f64) * (100 as f64);

        let item = LoadedPageModel {
            page_id: page.page_id.value,
            percent: percent as usize,
            count,
            has_skipped_messages: page.metrics.get_has_skipped_messages(),
            write_position: page.metrics.get_write_position(),
        };

        result.push(item);
    }

    result.sort_by(|a, b| match a.page_id > b.page_id {
        true => return std::cmp::Ordering::Greater,
        _ => return std::cmp::Ordering::Less,
    });

    result
}

async fn get_topics_model(
    snapshot: &TopicSnapshotProtobufModel,
    cache_by_topic: &TopicData,
    now: DateTimeAsMicroseconds,
) -> TopicInfo {
    let active_pages = crate::uncompressed_page::get_active_pages(snapshot);

    let last_save_moment_since = now.duration_since(cache_by_topic.metrics.get_last_saved_moment());

    let queue_size = cache_by_topic.get_messages_amount_to_save().await;

    TopicInfo {
        topic_id: snapshot.topic_id.to_string(),
        message_id: snapshot.message_id,
        active_pages: active_pages.keys().into_iter().map(|i| *i).collect(),
        loaded_pages: get_loaded_pages(cache_by_topic).await,
        queues: get_queues(&snapshot.queues),
        last_save_chunk: cache_by_topic.metrics.get_last_saved_chunk(),
        last_save_duration: duration_to_string(cache_by_topic.metrics.get_last_saved_duration()),
        last_save_moment: duration_to_string(last_save_moment_since),
        saved_message_id: cache_by_topic.metrics.get_last_saved_message_id(),
        queue_size,
    }
}

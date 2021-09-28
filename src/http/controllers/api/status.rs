use std::usize;

use crate::{
    app::{AppContext, TopicData},
    http::{HttpFailResult, HttpOkResult},
    utils::duration_to_string,
};
use my_service_bus_shared::{
    date_time::DateTimeAsMicroseconds,
    protobuf_models::{QueueSnapshotProtobufModel, TopicSnapshotProtobufModel},
};
use serde::{Deserialize, Serialize};

use sysinfo::SystemExt;

pub async fn get(app: &AppContext) -> Result<HttpOkResult, HttpFailResult> {
    let model = get_model(app).await;
    return Ok(HttpOkResult::create_json_response(model));
}

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
struct StatusModel {
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

async fn get_loaded_pages(topic_data: &TopicData) -> Vec<LoadedPageModel> {
    let mut result: Vec<LoadedPageModel> = Vec::new();

    for page in topic_data.get_all().await {
        let read_access = page.data.lock().await;

        let item = LoadedPageModel {
            page_id: page.page_id.value,
            percent: read_access.percent(),
            count: read_access.message_count(),
            has_skipped_messages: read_access.has_skipped_messages(),
            write_position: read_access.get_write_position().await,
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
    let active_pages = crate::message_pages::utils::get_active_pages(snapshot);

    let metrics = cache_by_topic.get_metrics().await;

    let last_save_moment_since = now.duration_since(metrics.last_saved_moment);
    let queue_size = cache_by_topic.get_queue_size().await;

    TopicInfo {
        topic_id: snapshot.topic_id.to_string(),
        message_id: snapshot.message_id,
        active_pages: active_pages.keys().into_iter().map(|i| *i).collect(),
        loaded_pages: get_loaded_pages(cache_by_topic).await,
        queues: get_queues(&snapshot.queues),
        last_save_chunk: metrics.last_saved_chunk,
        last_save_duration: duration_to_string(metrics.last_saved_duration),
        last_save_moment: duration_to_string(last_save_moment_since),
        saved_message_id: metrics.last_saved_message_id,
        queue_size,
    }
}

async fn get_model(app: &AppContext) -> StatusModel {
    let topics_snapshot = app.topics_snapshot.get().await;

    let mut topics = Vec::new();
    let now = DateTimeAsMicroseconds::now();

    for snapshot in &topics_snapshot.snapshot.data {
        let data_by_topic = app.topics_data_list.get(snapshot.topic_id.as_str()).await;

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

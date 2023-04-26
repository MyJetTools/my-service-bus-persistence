use std::{collections::HashMap, usize};

use crate::{app::AppContext, topic_data::TopicData, utils::duration_to_string};
use my_service_bus_shared::{
    page_id::PageId,
    protobuf_models::{QueueSnapshotProtobufModel, TopicSnapshotProtobufModel},
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
                    from_id: r.get_from_id().get_value(),
                    to_id: r.get_to_id().get_value(),
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

    #[serde(rename = "lastSaveDur")]
    last_save_duration: String,

    #[serde(rename = "lastSaveMoment")]
    last_save_moment: String,

    #[serde(rename = "loadedPages")]
    loaded_pages: Vec<LoadedPageModel>,

    #[serde(rename = "activePages")]
    active_pages: Vec<i64>,

    queues: Vec<QueueStatusModel>,
}

#[derive(Serialize, Deserialize, Debug)]
struct LoadedPageModel {
    #[serde(rename = "pageId")]
    page_id: i64,

    #[serde(rename = "subPages")]
    sub_pages: Vec<i64>,

    count: usize,

    size: usize,
}

impl LoadedPageModel {
    pub async fn new(topic_data: &TopicData) -> Vec<Self> {
        let mut result: HashMap<i64, Self> = HashMap::new();

        for sub_page in topic_data.pages_list.get_all().await {
            let page_id: PageId = sub_page.get_id().into();

            if !result.contains_key(page_id.as_ref()) {
                result.insert(
                    page_id.get_value(),
                    Self {
                        page_id: page_id.get_value(),
                        sub_pages: vec![],
                        count: 0,
                        size: 0,
                    },
                );
            }

            let page_data = result.get_mut(page_id.as_ref()).unwrap();

            let size_and_amount = sub_page.get_size_and_amount().await;

            page_data.size += size_and_amount.size;
            page_data.count += size_and_amount.amount;

            page_data.sub_pages.push(sub_page.get_id().get_value());
        }

        result.into_iter().map(|itm| itm.1).collect()
    }
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
            initialing: match app.app_states.is_initialized() {
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

async fn get_topics_model(
    snapshot: &TopicSnapshotProtobufModel,
    topic_data: &TopicData,
    now: DateTimeAsMicroseconds,
) -> TopicInfo {
    let active_pages = crate::message_pages::utils::get_active_pages(snapshot);

    let last_save_moment_since = now.duration_since(topic_data.metrics.get_last_saved_moment());

    TopicInfo {
        topic_id: snapshot.topic_id.to_string(),
        message_id: snapshot.get_message_id().get_value(),
        active_pages: active_pages.keys().into_iter().map(|i| *i).collect(),
        loaded_pages: LoadedPageModel::new(topic_data).await,
        queues: get_queues(&snapshot.queues),

        last_save_duration: duration_to_string(topic_data.metrics.get_last_saved_duration()),
        last_save_moment: duration_to_string(last_save_moment_since.as_positive_or_zero()),
    }
}

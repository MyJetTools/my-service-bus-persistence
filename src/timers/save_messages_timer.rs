use std::sync::Arc;

use rust_extensions::{date_time::DateTimeAsMicroseconds, MyTimerTick};

use crate::{app::AppContext, topic_data::TopicData};

const MAX_PERSIST_SIZE: usize = 1024 * 1024 * 4;

pub struct SaveMessagesTimer {
    app: Arc<AppContext>,
}

impl SaveMessagesTimer {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

#[async_trait::async_trait]
impl MyTimerTick for SaveMessagesTimer {
    async fn tick(&self) {
        let topics = self.app.topics_snapshot.get().await;

        for topic in &topics.snapshot.data {
            let mut topic_data = self.app.topics_list.get(&topic.topic_id).await;

            if topic_data.is_none() {
                self.app.logs.add_info(
                    Some(topic.topic_id.as_str()),
                    "Saving messages",
                    format!("Topic data {} is not found. Creating it", topic.topic_id),
                );

                topic_data = crate::operations::init_new_topic(
                    self.app.as_ref(),
                    topic.topic_id.as_str(),
                    topic.message_id,
                )
                .await
                .into();
            }

            let topic_data = topic_data.unwrap();

            flush_minute_index_data(topic_data.as_ref()).await;

            let pages_with_data_to_save = topic_data
                .uncompressed_pages_list
                .get_pages_with_data_to_save()
                .await;

            for page in pages_with_data_to_save {
                if let Some(result) = page.flush_to_storage(MAX_PERSIST_SIZE).await {
                    topic_data
                        .metrics
                        .update_last_saved_duration(result.duration);

                    topic_data
                        .metrics
                        .update_last_saved_moment(DateTimeAsMicroseconds::now());

                    topic_data
                        .metrics
                        .update_last_saved_chunk(result.last_saved_chunk);

                    topic_data
                        .metrics
                        .update_last_saved_message_id(result.last_saved_message_id);
                }
            }
        }
    }
}

async fn flush_minute_index_data(topic_data: &TopicData) {
    let mut index_by_minute = topic_data.yearly_index_by_minute.lock().await;

    for item in index_by_minute.values_mut() {
        item.flush_to_storage().await;
    }
}

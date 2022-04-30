use std::sync::Arc;

use rust_extensions::MyTimerTick;

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
            let topic_data = self.app.topics_list.get(&topic.topic_id).await;

            if topic_data.is_none() {
                self.app.logs.add_info(
                    Some(topic.topic_id.as_str()),
                    "Saving messages",
                    format!("Topic data {} is not found", topic.topic_id),
                );
                crate::operations::init_new_topic(self.app.as_ref(), topic.topic_id.as_str()).await;
                continue;
            }

            let topic_data = topic_data.unwrap();

            flush_minute_index_data(topic_data.as_ref()).await;

            let pages_with_data_to_save = topic_data.pages_list.get_pages_with_data_to_save().await;

            for page in pages_with_data_to_save {
                let uncompressed_page = page.unwrap_as_uncompressed_page();

                let duration = uncompressed_page.flush_to_storage(MAX_PERSIST_SIZE).await;

                //TODO - Uncomment
                //page.update_metrics(&page.metrics).await;
                topic_data.metrics.update_last_saved_duration(duration);
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

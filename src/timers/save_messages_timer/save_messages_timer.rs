use std::sync::Arc;

use rust_extensions::{MyTimerTick, StopWatch};

use crate::app::AppContext;

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

            let pages_with_data_to_save = topic_data.pages_list.get_pages_with_data_to_save().await;

            for page in pages_with_data_to_save {
                let uncompressed_page = page.unwrap_as_uncompressed_page();

                let messages_to_persist = uncompressed_page
                    .get_messages_to_persist(MAX_PERSIST_SIZE)
                    .await;

                let duration = {
                    let mut storages = topic_data.storages.lock().await;

                    crate::operations::init_page_storage::init(
                        self.app.as_ref(),
                        topic.topic_id.as_str(),
                        uncompressed_page,
                        &mut storages,
                        true,
                    )
                    .await
                    .unwrap();

                    let storage = storages.get_mut(&uncompressed_page.page_id).unwrap();

                    let mut sw = StopWatch::new();
                    sw.start();

                    let mut upload_container = storage.issue_payloads_to_upload_container();

                    let page_id = page.get_page_id();

                    for msg_to_persist in &messages_to_persist {
                        upload_container.append(page_id, msg_to_persist);
                    }

                    storage.append_payload(upload_container).await.unwrap();

                    uncompressed_page
                        .confirm_persisted(messages_to_persist.as_slice())
                        .await;

                    sw.pause();

                    sw.duration()
                };

                //TODO - Uncomment
                //page.update_metrics(&page.metrics).await;
                topic_data.metrics.update_last_saved_duration(duration);
            }
        }
    }
}

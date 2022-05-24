use std::sync::Arc;

use rust_extensions::MyTimerTick;

use crate::app::AppContext;

pub struct SaveAndGcMessagesTimer {
    app: Arc<AppContext>,
}

impl SaveAndGcMessagesTimer {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

#[async_trait::async_trait]
impl MyTimerTick for SaveAndGcMessagesTimer {
    async fn tick(&self) {
        let topics = self.app.topics_snapshot.get().await;

        for topic_snapshot in &topics.snapshot.data {
            let topic_data_result = self.app.topics_list.get(&topic_snapshot.topic_id).await;

            let topic_data = crate::operations::persist_topic_pages(
                self.app.as_ref(),
                topic_snapshot.topic_id.as_str(),
                topic_snapshot.message_id,
                topic_data_result,
            )
            .await;

            let active_pages = crate::operations::get_active_pages(topic_snapshot);

            let gc_pages_result = crate::operations::gc_pages(
                self.app.as_ref(),
                topic_data.clone(),
                active_pages.as_slice(),
            )
            .await;

            if let Err(e) = gc_pages_result {
                self.app.logs.add_error(
                    Some(topic_snapshot.topic_id.as_str()),
                    "SaveMessagesTimer",
                    "GC Pages Error".to_string(),
                    format!("{:?}", e),
                );
            }

            crate::operations::gc_yearly_index(self.app.as_ref(), topic_data.as_ref()).await;

            topic_data.auto_gc_sub_pages().await;
        }
    }
}

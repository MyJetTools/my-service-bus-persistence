use std::sync::Arc;

use rust_extensions::MyTimerTick;

use crate::app::AppContext;

pub struct SaveMinIndexTimer {
    app: Arc<AppContext>,
}

impl SaveMinIndexTimer {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

#[async_trait::async_trait]
impl MyTimerTick for SaveMinIndexTimer {
    async fn tick(&self) {
        todo!("Uncomment");
        /*
        let topics_snapshot = self.app.topics_snapshot.get().await;
        for topic in &topics_snapshot.snapshot.data {
            let index_handler = self.app.index_by_minute.get(topic.topic_id.as_str()).await;

            index_handler
                .save_to_storage(&self.app.index_by_minute_utils)
                .await;
        }
         */
    }
}

use std::{sync::Arc, time::Duration};

use tokio::task::JoinHandle;

use crate::app::AppContext;

pub fn start(app: Arc<AppContext>) -> JoinHandle<()> {
    return tokio::spawn(three_sec_loop(app));
}

pub async fn three_sec_loop(app: Arc<AppContext>) {
    let duration = Duration::from_secs(3);

    loop {
        let topics = app.get_topics_snapshot().await;
        let topics = Arc::new(topics);

        let topics_snapshot = Arc::new(topics.snapshot.clone());

        super::topics_snapshot_saver::execute(app.clone(), topics.clone()).await;
        super::pages_gc::execute(app.clone(), topics_snapshot.clone()).await;
        super::save_min_index::execute(app.clone(), topics_snapshot.clone()).await;
        super::metrics_updater::execute(app.clone(), topics_snapshot.clone()).await;
        super::save_messages::execute(app.clone(), topics_snapshot).await;

        tokio::time::sleep(duration).await;
    }
}

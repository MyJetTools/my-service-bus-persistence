use std::{sync::Arc, time::Duration};

use my_azure_page_blob::MyAzurePageBlob;
use tokio::task::JoinHandle;

use crate::app::AppContext;

pub fn start(app: Arc<AppContext>, topics_blob: MyAzurePageBlob) -> JoinHandle<()> {
    return tokio::spawn(three_sec_loop(app, topics_blob));
}

pub async fn three_sec_loop(app: Arc<AppContext>, mut topics_blob: MyAzurePageBlob) {
    let duration = Duration::from_secs(3);

    loop {
        let topics = app.get_topics_snapshot().await;
        super::topics_snapshot_saver::execute(app.as_ref(), &topics, &mut topics_blob).await;
        super::pages_gc::execute(app.as_ref(), &topics.snapshot).await;
        super::save_min_index::execute(app.as_ref(), &topics.snapshot).await;
        super::metrics_updater::execute(app.as_ref(), &topics.snapshot).await;
        super::save_messages::execute(app.clone(), &topics.snapshot).await;

        tokio::time::sleep(duration).await;
    }
}

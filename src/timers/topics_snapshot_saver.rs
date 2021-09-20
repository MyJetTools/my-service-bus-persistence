use std::sync::Arc;

use crate::{app::AppContext, toipics_snapshot::CurrentTopicsSnapshot};

pub async fn execute(app: Arc<AppContext>, snapshot: Arc<CurrentTopicsSnapshot>) {
    if snapshot.last_saved_snapshot_id == snapshot.snapshot_id {
        return;
    }
    let timer_result = tokio::spawn(process(app.clone(), snapshot)).await;

    if let Err(err) = timer_result {
        app.logs
            .add_fatal_error("topics_snapshot_saver_timer", err)
            .await;
    }
}

async fn process(app: Arc<AppContext>, snapshot: Arc<CurrentTopicsSnapshot>) {
    let mut topics_blob = app.settings.get_topics_snapshot_page_blob();

    let result = crate::toipics_snapshot::blob_repository::write_to_blob(
        &mut topics_blob,
        &snapshot.snapshot,
    )
    .await;

    if let Err(err) = result {
        app.logs
            .add_error_str(
                None,
                "Write Topics Snapshot",
                format!("Can not snapshot iwth ID #{}", snapshot.snapshot_id),
                format!("{:?}", err),
            )
            .await
    } else {
        let mut write_access = app.topics_snapshot.write().await;
        write_access.saved(snapshot.snapshot_id);
    }
}

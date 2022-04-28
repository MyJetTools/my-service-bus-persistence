use std::sync::Arc;

use rust_extensions::MyTimerTick;

use crate::{app::AppContext, toipics_snapshot::current_snapshot::TopicsSnapshotData};

pub struct TopicsSnapshotSaverTimer {
    app: Arc<AppContext>,
}

impl TopicsSnapshotSaverTimer {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

#[async_trait::async_trait]
impl MyTimerTick for TopicsSnapshotSaverTimer {
    async fn tick(&self) {
        if let Some(snapshot) = self
            .app
            .topics_snapshot
            .get_snapshot_if_there_are_chages()
            .await
        {
            let mut topics_blob = self.app.settings.get_topics_snapshot_page_blob();

            let result = crate::toipics_snapshot::blob_repository::write_to_blob(
                &mut topics_blob,
                &snapshot.snapshot,
            )
            .await;

            if let Err(err) = result {
                self.app.logs.add_error_str(
                    None,
                    "Write Topics Snapshot",
                    format!("Can not snapshot iwth ID #{}", snapshot.snapshot_id),
                    format!("{:?}", err),
                );
            } else {
                self.app
                    .topics_snapshot
                    .update_snapshot_id_as_saved(snapshot.snapshot_id)
                    .await;
            }
        }
    }
}

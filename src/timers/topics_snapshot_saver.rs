use std::sync::Arc;

use rust_extensions::MyTimerTick;

use crate::app::AppContext;

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
        let snapshot = self
            .app
            .topics_snapshot
            .get_snapshot_if_there_are_chages()
            .await;

        if snapshot.is_none() {
            return;
        }

        let snapshot = snapshot.unwrap();

        let result = self
            .app
            .topics_snapshot
            .blob
            .write(&snapshot.snapshot)
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

use my_azure_page_blob::MyAzurePageBlob;

use crate::{app::AppContext, toipics_snapshot::CurrentTopicsSnapshot};

pub async fn execute(
    app: &AppContext,
    snapshot: &CurrentTopicsSnapshot,
    topics_blob: &mut MyAzurePageBlob,
) {
    if snapshot.last_saved_snapshot_id == snapshot.snapshot_id {
        return;
    }

    let result =
        crate::toipics_snapshot::blob_repository::write_to_blob(topics_blob, &snapshot.snapshot)
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

use std::{sync::Arc, time::Duration};

use crate::{
    app::AppContext,
    message_pages::{MessagesPage, MessagesPageStorage},
};

pub async fn initialize_uncompressed_blob(
    storage: &mut MessagesPageStorage,
    page: &MessagesPage,
    app: Arc<AppContext>,
) {
    let mut attempt_no: usize = 1;
    while attempt_no < 5 {
        let result = storage
            .initialize_uncompressed_blob(page.topic_id.to_string(), page.page_id, app.clone())
            .await;

        if result.is_ok() {
            return;
        }

        let err = result.err().unwrap();

        app.logs
            .add_error_str(
                Some(page.topic_id.as_str()),
                "Initializing uncompressed page blob",
                format!("Attemped #{} failed", attempt_no),
                format!("{:?}", err),
            )
            .await;

        attempt_no += 1;

        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    app.logs
        .add_error_str(
            Some(page.topic_id.as_str()),
            "Initializing uncompressed page blob",
            format!("Attemped #{} failed", attempt_no),
            "Could not initialize page blob. Skipping save iteration".to_string(),
        )
        .await;
}

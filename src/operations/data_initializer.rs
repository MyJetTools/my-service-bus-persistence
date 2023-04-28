use std::sync::Arc;

use rust_extensions::{Logger, StopWatch};

use crate::app::AppContext;

pub async fn init(app: Arc<AppContext>) {
    let mut sw = StopWatch::new();

    sw.start();

    restore_pages(&app).await;

    sw.pause();

    app.logs.write_info(
        "Initialization".to_string(),
        format!("Application is initialized in {:?}", sw.duration()),
        None,
    );

    app.app_states.set_initialized();
}

async fn restore_pages(app: &Arc<AppContext>) {
    app.logs.write_info(
        "Initialization".to_string(),
        "Loading messages since last shutdown".to_string(),
        None,
    );
    let sub_pages = crate::operations::current_sub_pages_io::restore(&app)
        .await
        .unwrap();

    if let Some(sub_pages) = sub_pages {
        let mut sw = StopWatch::new();

        for (topic_id, sub_page_inner) in sub_pages {
            sw.start();
            let topic_data = app.topics_list.init_topic_data(topic_id.as_str()).await;

            sw.pause();

            app.logs.write_info(
                "Initialization".to_string(),
                format!(
                    "Loaded sub page {} for topic {}in {}",
                    sub_page_inner.sub_page_id.get_value(),
                    topic_id,
                    sw.duration_as_string()
                ),
                None,
            );
            topic_data.pages_list.add_new(sub_page_inner).await;
        }
    } else {
        app.logs.write_info(
            "Initialization".to_string(),
            format!("No sub page data loaded"),
            None,
        );
    }
}

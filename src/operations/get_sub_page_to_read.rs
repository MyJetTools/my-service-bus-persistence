use std::sync::Arc;

use my_logger::LogEventCtx;
use my_service_bus::shared::sub_page::SubPageId;

use crate::{
    app::AppContext,
    message_pages::{SubPage, SubPageInner},
};

pub async fn get_sub_page_to_read(
    app: &AppContext,
    topic_id: &str,
    sub_page_id: SubPageId,
) -> Arc<SubPage> {
    match read(app, topic_id, sub_page_id).await {
        Some(sub_page) => sub_page,
        None => {
            let sub_page = SubPage::create_missing(sub_page_id);
            Arc::new(sub_page)
        }
    }
}

async fn read(app: &AppContext, topic_id: &str, sub_page_id: SubPageId) -> Option<Arc<SubPage>> {
    let topic = app.topics_list.get(topic_id).await?;

    if let Some(sub_page) = topic.pages_list.get(sub_page_id).await {
        return Some(sub_page);
    }

    let archive_file_no = sub_page_id.into();

    let archive_storage = app
        .archive_storage_list
        .try_get_or_open(archive_file_no, topic_id, app)
        .await?;

    let payload = archive_storage.read_sub_page_payload(sub_page_id).await;

    match payload {
        Ok(payload) => {
            let sub_page = SubPageInner::from_compressed_payload(sub_page_id, payload?.as_slice());

            if let Err(err) = &sub_page {
                my_logger::LOGGER.write_warning(
                    "get_sub_page_to_read",
                    format!("{:?}", err),
                    LogEventCtx::new().add("topicId", topic_id),
                );
            }

            let sub_page = sub_page.unwrap();

            let sub_page = Arc::new(SubPage::restore_from_archive(sub_page));
            Some(sub_page)
        }
        Err(err) => {
            my_logger::LOGGER.write_warning(
                "get_sub_page_to_read",
                format!("{:?}", err),
                LogEventCtx::new().add("topicId", topic_id),
            );
            None
        }
    }
}

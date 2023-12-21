use std::sync::Arc;

use my_service_bus::shared::sub_page::SubPageId;

use crate::{app::AppContext, message_pages::SubPage, topic_data::TopicData};

pub async fn get_page_to_read(
    app: &AppContext,
    topic_data: &TopicData,
    sub_page_id: SubPageId,
) -> Arc<SubPage> {
    loop {
        let page = topic_data.pages_list.get(sub_page_id).await;

        if let Some(page) = page {
            return page;
        };

        let sub_page =
            crate::operations::archive_io::restore_sub_page(app, topic_data, sub_page_id).await;

        match sub_page {
            Ok(sub_page) => {
                topic_data.pages_list.restore_from_archive(sub_page).await;
            }
            Err(err) => {
                println!(
                    "Can not restore sub page {}  for topic {}. Err: {:?}",
                    sub_page_id, topic_data.topic_id, err
                );
                topic_data.pages_list.add_missing(sub_page_id).await;
            }
        }
    }
}

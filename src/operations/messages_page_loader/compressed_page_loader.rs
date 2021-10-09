use crate::{
    app::{AppContext, TopicData},
    message_pages::{MessagePageId, MessagesPageData},
};

pub async fn load(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: MessagePageId,
) -> Option<MessagesPageData> {
    let read_result = topic_data.pages_cluster.read(page_id).await;

    match read_result {
        Ok(payload) => {
            if let Some(zip_archive) = payload {
                return MessagesPageData::resored_compressed(page_id, zip_archive);
            }

            return None;
        }
        Err(err) => {
            app.logs
                .add_error(
                    Some(topic_data.topic_id.as_str()),
                    "Load compressed page",
                    "Can not restore from compressed page",
                    format!("{:?}", err),
                )
                .await;

            return None;
        }
    }
}

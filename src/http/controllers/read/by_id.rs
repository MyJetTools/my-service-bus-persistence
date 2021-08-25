use crate::{
    app::AppContext,
    http::{HttpContext, HttpFailResult, HttpOkResult},
    message_pages::MessagePageId,
};

use super::models::GetMessageResponseModel;

pub async fn get(ctx: HttpContext, app: &AppContext) -> Result<HttpOkResult, HttpFailResult> {
    let q = ctx.get_query_string();

    let topic_id = q.get_required_query_parameter("topicId")?;
    let message_id = q.get_required_query_parameter("messageId")?;

    let message_id = message_id.parse::<i64>().unwrap();

    let pages_cache = app.get_data_by_topic(topic_id).await;

    if pages_cache.is_none() {
        return Err(HttpFailResult::not_found(format!(
            "Topic {} not found",
            topic_id
        )));
    }

    let pages_cache = pages_cache.unwrap();

    let page_id = MessagePageId::from_message_id(message_id);

    let data_by_topic = pages_cache.get(page_id).await;

    let read_access = data_by_topic.data.read().await;

    let read_access = read_access.get(0).unwrap();

    let message = read_access.messages.get(&message_id);

    let model = GetMessageResponseModel::create(message);

    Ok(HttpOkResult::create_json_response(model))
}

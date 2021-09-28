use std::sync::Arc;

use crate::{
    app::AppContext,
    http::{HttpContext, HttpFailResult, HttpOkResult},
    message_pages::MessagePageId,
};

use super::models::GetMessageResponseModel;

pub async fn get(ctx: HttpContext, app: Arc<AppContext>) -> Result<HttpOkResult, HttpFailResult> {
    let q = ctx.get_query_string();

    let topic_id = q.get_required_query_parameter("topicId")?;
    let message_id = q.get_required_query_parameter("messageId")?;

    let message_id = message_id.parse::<i64>().unwrap();

    let topic_data = app.topics_data_list.get(topic_id).await;

    if topic_data.is_none() {
        return Err(HttpFailResult::not_found(format!(
            "Topic {} not found",
            topic_id
        )));
    }

    let topic_data = topic_data.unwrap();

    let current_page_id = app.get_current_page_id(topic_id).await.unwrap();

    let page_id = MessagePageId::from_message_id(message_id);

    let page = crate::operations::pages::get_or_restore(
        app,
        topic_data,
        page_id,
        page_id.value >= current_page_id.value,
    )
    .await;

    let message = page.get_message(message_id).await;

    if let Err(err) = message {
        return Err(HttpFailResult::error(format!("{:?}", err)));
    }

    match message.as_ref().unwrap() {
        Some(msg) => {
            let model = GetMessageResponseModel::create(msg);
            return Ok(HttpOkResult::create_json_response(model));
        }
        None => {
            return Err(HttpFailResult::not_found(format!(
                "Message {} not found",
                message_id
            )));
        }
    }
}

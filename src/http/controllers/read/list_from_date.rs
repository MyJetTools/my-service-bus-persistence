use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{
    app::AppContext,
    http::{HttpContext, HttpFailResult, HttpOkResult},
    message_pages::MessagePageId,
};

use super::models::GetMessagesResponseModel;

pub async fn get(ctx: HttpContext, app: &AppContext) -> Result<HttpOkResult, HttpFailResult> {
    let q = ctx.get_query_string();

    let topic_id = q.get_required_query_parameter("topicId")?;

    let topic_data = app.topics_data_list.get(topic_id).await;

    if topic_data.is_none() {
        return Err(HttpFailResult::not_found(format!(
            "Topic {} not found",
            topic_id
        )));
    }

    let topic_data = topic_data.unwrap();

    let max_amount: usize = q.get_query_parameter_or_defaul("maxAmount", 1);

    let handler = app.index_by_minute.get(topic_id).await;

    let from_date_str = q.get_required_query_parameter("fromDate")?;

    let from_date = DateTimeAsMicroseconds::parse_iso_string(from_date_str);

    if from_date.is_none() {
        return Ok(format!("Invalid date string: {}", from_date_str).into());
    }

    let from_date = from_date.unwrap();

    let mut message_id = handler
        .get_message_id(&app.index_by_minute_utils, from_date)
        .await?;

    let page_id = MessagePageId::from_message_id(message_id);

    let page = topic_data.get(page_id).await;

    let mut messages = Vec::new();

    if page.is_none() {
        let model = GetMessagesResponseModel::create(messages);
        return Ok(HttpOkResult::create_json_response(model));
    }

    let page = page.unwrap();

    let read_access = page.data.read().await;

    while let Some(message) = read_access.get_message(message_id).unwrap() {
        messages.push(message);

        if messages.len() >= max_amount {
            break;
        }

        message_id += 1;
    }

    let model = GetMessagesResponseModel::create(messages);

    Ok(HttpOkResult::create_json_response(model))
}

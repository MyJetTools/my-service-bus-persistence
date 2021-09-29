use std::sync::Arc;

use crate::{
    app::AppContext,
    http::{HttpContext, HttpFailResult, HttpOkResult},
};

use super::models::GetMessageResponseModel;

pub async fn get(ctx: HttpContext, app: Arc<AppContext>) -> Result<HttpOkResult, HttpFailResult> {
    let q = ctx.get_query_string();

    let topic_id = q.get_required_query_parameter("topicId")?;
    let message_id = q.get_required_query_parameter("messageId")?;

    let message_id = message_id.parse::<i64>().unwrap();

    let message = crate::operations::messages::get_message(app, topic_id, message_id).await?;

    match message {
        Some(msg) => {
            let model = GetMessageResponseModel::create(&msg);
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

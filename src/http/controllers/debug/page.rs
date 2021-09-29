use crate::{
    app::AppContext,
    http::{HttpContext, HttpFailResult, HttpOkResult},
    message_pages::MessagePageId,
};

pub async fn get(ctx: HttpContext, app: &AppContext) -> Result<HttpOkResult, HttpFailResult> {
    let q = ctx.get_query_string();

    let topic_id = q.get_required_query_parameter("topicId")?;
    let page_id = q.get_required_query_parameter("pageId")?;

    let page_id = page_id.parse::<i64>().unwrap();

    let topic_data = app.topics_data_list.get(topic_id).await;

    if topic_data.is_none() {
        return Err(HttpFailResult::not_found("Topic not found".to_string()));
    }

    let page = topic_data.unwrap().get(MessagePageId::new(page_id)).await;

    if page.is_none() {
        return Err(HttpFailResult::not_found("Page not found".to_string()));
    }

    let page = page.unwrap();

    let data = page.data.lock().await;

    return Ok(format!("{}", data.get_page_type()).into());
}

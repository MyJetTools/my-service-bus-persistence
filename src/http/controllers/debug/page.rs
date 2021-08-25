use crate::{
    app::AppContext,
    http::{HttpContext, HttpFailResult, HttpOkResult},
};

pub async fn get(ctx: HttpContext, app: &AppContext) -> Result<HttpOkResult, HttpFailResult> {
    let q = ctx.get_query_string();

    let topic_id = q.get_required_query_parameter("topicId")?;
    let page_id = q.get_required_query_parameter("pageId")?;

    let page_id = page_id.parse::<i64>().unwrap();

    let topic_data = app.get_data_by_topic(topic_id).await.unwrap();

    let pages_access = topic_data.pages.lock().await;

    let page = pages_access.get(&page_id).unwrap().as_ref();

    let data = page.data.read().await;

    let text = match data.get(0) {
        Some(itm) => format!(
            "min_id: {:?}, max_id: {:?}",
            itm.min_message_id, itm.max_message_id
        ),
        None => "No page data".to_string(),
    };

    return Ok(text.into());
}

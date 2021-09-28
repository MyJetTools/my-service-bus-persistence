use crate::{
    app::AppContext,
    http::{HttpContext, HttpFailResult, HttpOkResult},
};

pub async fn get(ctx: HttpContext, app: &AppContext) -> Result<HttpOkResult, HttpFailResult> {
    todo!("Debug it later");

    /*
    let q = ctx.get_query_string();

    let topic_id = q.get_required_query_parameter("topicId")?;
    let page_id = q.get_required_query_parameter("pageId")?;

    let page_id = page_id.parse::<i64>().unwrap();

    let topic_data = app.topics_data_list.get(topic_id).await;

    if topic_data.is_none() {
        return Err(HttpFailResult::not_found("Topic not found".to_string()));
    }

    let pages_access = topic_data.unwrap().pages.lock().await;

    let page = pages_access.get(&page_id).unwrap().as_ref();

    let data = page.data.lock().await;

    return Ok("Not Supported yet".to_string().into());
     */
}

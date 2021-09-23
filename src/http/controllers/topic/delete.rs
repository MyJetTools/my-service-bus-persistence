use crate::{
    app::AppContext,
    http::{HttpContext, HttpFailResult, HttpOkResult},
};

pub async fn delete_topic(
    ctx: HttpContext,
    app: &AppContext,
) -> Result<HttpOkResult, HttpFailResult> {
    let q = ctx.get_query_string();

    let topic_id = q.get_required_query_parameter("topicId")?;
    let api_key = q.get_required_query_parameter("apiKey")?;

    if app.settings.delete_topic_secret_key.as_str() != api_key {
        return Err(HttpFailResult::not_authorized("Invalid Secret Key"));
    }

    let mut topics_snapshot = app.topics_snapshot.get().await;

    let index = topics_snapshot
        .snapshot
        .data
        .iter()
        .position(|topic| topic.topic_id.as_str() == topic_id);

    if index.is_none() {
        return Err(HttpFailResult::not_found(format!(
            "Topic {} not found",
            topic_id
        )));
    }

    let index = index.unwrap();

    topics_snapshot.snapshot.data.remove(index);

    app.topics_snapshot.update(topics_snapshot.snapshot).await;

    return Ok("Deleted ok".to_string().into());
}

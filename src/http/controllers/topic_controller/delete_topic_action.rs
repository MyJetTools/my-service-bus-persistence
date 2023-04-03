use std::sync::Arc;

use my_http_server::{HttpContext, HttpFailResult, HttpOkResult, HttpOutput};

use crate::app::AppContext;

use super::contracts::*;

#[my_http_server_swagger::http_route(
    method: "DELETE",
    route: "/Topic",
    input_data: "DeleteTopicHttpContract",
    description: "Deletes Topic",
    summary: "Delete Topic",
    controller: "Queues",
    result:[
        {status_code: 202, description: "Topic is deleted"},
        {status_code: 404, description: "Topic not found"},
    ]
)]
pub struct DeleteTopicAction {
    app: Arc<AppContext>,
}

impl DeleteTopicAction {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

async fn handle_request(
    action: &DeleteTopicAction,
    input_data: DeleteTopicHttpContract,
    _ctx: &HttpContext,
) -> Result<HttpOkResult, HttpFailResult> {
    if action.app.settings.delete_topic_secret_key.as_str() != input_data.api_key {
        return Err(HttpFailResult::as_unauthorized(
            "Invalid Secret Key".to_string().into(),
        ));
    }

    let mut topics_snapshot = action.app.topics_snapshot.get().await;

    let index = topics_snapshot
        .snapshot
        .data
        .iter()
        .position(|topic| topic.topic_id.as_str() == input_data.topic_id);

    if index.is_none() {
        return Err(HttpFailResult::as_not_found(
            format!("Topic {} not found", input_data.topic_id),
            true,
        ));
    }

    let index = index.unwrap();

    topics_snapshot.snapshot.data.remove(index);

    action
        .app
        .topics_snapshot
        .update(topics_snapshot.snapshot)
        .await;

    return HttpOutput::Empty.into_ok_result(true).into();
}

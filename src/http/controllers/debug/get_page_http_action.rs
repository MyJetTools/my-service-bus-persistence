use std::sync::Arc;

use my_http_server::{HttpContext, HttpFailResult, HttpOkResult};

use crate::{app::AppContext, message_pages::MessagePageId};

use super::contracts::GetDebugPageContract;

#[my_http_server_swagger::http_route(method: "GET", route: "/debug/page", controller:"Debug", description:"Debug page", input_data:"GetDebugPageContract",)]
pub struct GetPageHttpAction {
    app: Arc<AppContext>,
}

impl GetPageHttpAction {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

async fn handle_request(
    action: &GetPageHttpAction,
    input_data: GetDebugPageContract,
    _ctx: &HttpContext,
) -> Result<HttpOkResult, HttpFailResult> {
    let topic_data = action
        .app
        .topics_list
        .get(input_data.topic_id.as_str())
        .await;

    if topic_data.is_none() {
        return Err(HttpFailResult::as_not_found(
            "Topic not found".to_string(),
            true,
        ));
    }

    let page = topic_data
        .unwrap()
        .get(MessagePageId::new(input_data.page_id))
        .await;

    if page.is_none() {
        return Err(HttpFailResult::not_found("Page not found".to_string()));
    }

    let page = page.unwrap();

    let data = page.data.read().await;

    return Ok(format!("{}", data.get_page_type()).into());
}

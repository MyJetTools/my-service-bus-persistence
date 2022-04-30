use std::sync::Arc;

use my_http_server::{HttpContext, HttpFailResult, HttpOkResult, HttpOutput};

use crate::app::AppContext;

use super::contracts::*;

#[my_http_server_swagger::http_route(method:"GET",
route:"/Read/ById",
controller:"Read",
description:"Read message by Id",
input_data:"GetMessageByIdInputContract",
result:[
    {status_code: 202, description: "Found message"},
    {status_code: 404, description: "Topic not found"},
]
)]

pub struct ByIdAction {
    app: Arc<AppContext>,
}

impl ByIdAction {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

async fn handle_request(
    action: &ByIdAction,
    input_data: GetMessageByIdInputContract,
    ctx: &mut HttpContext,
) -> Result<HttpOkResult, HttpFailResult> {
    let message = crate::operations::get_message_by_id(
        action.app.as_ref(),
        input_data.topic_id.as_str(),
        input_data.message_id,
    )
    .await?;

    match message {
        Some(msg) => {
            let model = GetMessageResponseModel::create(&msg);
            return Ok(HttpOutput::as_json(model).into_ok_result(true).into());
        }
        None => {
            return Err(HttpFailResult::as_not_found(
                format!("Message {} not found", input_data.message_id),
                true,
            ));
        }
    }
}

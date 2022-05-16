use super::contracts::*;
use crate::app::AppContext;
use my_http_server::{HttpContext, HttpFailResult, HttpOkResult, HttpOutput};
use rust_extensions::date_time::DateTimeAsMicroseconds;
use std::sync::Arc;

#[my_http_server_swagger::http_route(method:"GET",
route:"/Read/ListFromDate",
controller:"Read",
description:"Read message by Id",
input_data:"GetMessagesByIdInputContract",
result:[
    {status_code: 200, description: "Found messages"},
    {status_code: 404, description: "Topic not found"},
]
)]
pub struct ListFromDateAction {
    app: Arc<AppContext>,
}

impl ListFromDateAction {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

async fn handle_request(
    action: &ListFromDateAction,
    input_data: GetMessagesByIdInputContract,
    _ctx: &mut HttpContext,
) -> Result<HttpOkResult, HttpFailResult> {
    let messages = crate::operations::get_messages_from_date(
        &action.app,
        input_data.topic_id.as_str(),
        DateTimeAsMicroseconds::parse_iso_string(input_data.from_date.as_str()).unwrap(),
        input_data.max_amount,
    )
    .await?;

    let model = GetMessagesResponseModel::create(messages);

    HttpOutput::as_json(model).into_ok_result(true).into()
}

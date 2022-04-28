use std::sync::Arc;

use my_http_server::{HttpContext, HttpFailResult, HttpOkResult, HttpOutput};


use crate::app::AppContext;

use super::contracts::*;

use my_swagger::http_route;

/*
#[http_route(method="GET", 
route="/Read/ById", 
controller="Read", 
description="Read message by Id", 
input_data="GetMessageByIdInputContract",
result:[
    {status_code: 202, description: "Found message", model_as_array ="String"},
    {status_code: 404, description: "Topic not found"},
]
)]

 */

pub struct ByIdAction {
    app: Arc<AppContext>,
}

impl ByIdAction {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

/*
#[async_trait::async_trait]
impl GetAction for ByIdAction {
    fn get_route(&self) -> &str {
        "/read/byid"
    }

    fn get_description(&self) -> Option<HttpActionDescription> {
        HttpActionDescription {
            controller_name: "Read",
            description: "Read message by Id",

            input_params: GetMessageByIdInputContract::get_input_params().into(),
            results: vec![
                HttpResult {
                    http_code: 200,
                    description: "Found message".to_string(),
                    nullable: false,
                    data_type: GetMessageResponseModel::get_http_data_structure()
                        .into_http_data_type_object(),
                },
                response::topic_not_found(),
            ],
        }
        .into()
    }

 
}
*/

async fn handle_request(action: &ByIdAction, input_data: GetMessageByIdInputContract, ctx: &mut HttpContext) -> Result<HttpOkResult, HttpFailResult> {

    let message = crate::operations::messages::get_message(
        self.app,
        input_data.topic_id,
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
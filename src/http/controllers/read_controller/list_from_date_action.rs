use std::sync::Arc;

use my_http_server::{HttpContext, HttpFailResult, HttpOkResult};
use my_http_server_controllers::controllers::{
    actions::GetAction,
    documentation::{out_results::HttpResult, HttpActionDescription},
};
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{app::AppContext, message_pages::MessagePageId};

use super::contracts::*;
pub struct ListFromDateAction {
    app: Arc<AppContext>,
}

impl ListFromDateAction {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

/*
#[async_trait::async_trait]
impl GetAction for ListFromDateAction {
    fn get_route(&self) -> &str {
        "/read/listfromdate"
    }
    fn get_description(&self) -> Option<HttpActionDescription> {
        HttpActionDescription {
            controller_name: "Read",
            description: "Read messages from date",

            input_params: GetMessagesByIdInputContract::get_input_params().into(),
            results: vec![
                HttpResult {
                    http_code: 200,
                    description: "List of messages".to_string(),
                    nullable: false,
                    data_type: GetMessagesResponseModel::get_http_data_structure()
                        .into_http_data_type_array(),
                },
                response::topic_not_found(),
            ],
        }
        .into()
    }


}

 */

async fn handle_request(
    action: &ListFromDateAction,
    input_data: GetMessagesByIdInputContract,
    _ctx: &mut HttpContext
) -> Result<HttpOkResult, HttpFailResult> {
    let topic_data = action.app.topics_list.get(input_data.topic_id).await;

    if topic_data.is_none() {
        return Err(HttpFailResult::as_not_found(
            format!("Topic {} not found", input_data.topic_id),
            true,
        ));
    }

    let topic_data = topic_data.unwrap();

    let handler = action.app.index_by_minute.get(input_data.topic_id).await;

    let from_date = DateTimeAsMicroseconds::parse_iso_string(input_data.from_date);

    if from_date.is_none() {
        return Ok(format!("Invalid date string: {}", input_data.from_date).into());
    }

    let from_date = from_date.unwrap();

    let mut message_id = handler
        .get_message_id(&action.app.index_by_minute_utils, from_date)
        .await?;

    let page_id = MessagePageId::from_message_id(message_id);

    let page = topic_data.get(page_id).await;

    let mut messages = Vec::new();

    if page.is_none() {
        let model = GetMessagesResponseModel::create(messages);
        return Ok(HttpOkResult::create_json_response(model));
    }

    let page = page.unwrap();

    let read_access = page.data.read().await;

    while let Some(message) = read_access.get_message(message_id).unwrap() {
        messages.push(message);

        if messages.len() >= input_data.max_amount {
            break;
        }

        message_id += 1;
    }

    let model = GetMessagesResponseModel::create(messages);

    Ok(HttpOkResult::create_json_response(model))
}

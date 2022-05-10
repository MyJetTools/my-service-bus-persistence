use std::sync::Arc;

use my_service_bus_shared::protobuf_models::MessageProtobufModel;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{
    app::AppContext,
    index_by_minute::{MinuteWithinYear, YearlyIndexByMinute},
    message_pages::MessagePageId,
    topic_data::TopicData,
};

use super::OperationError;

pub async fn get_messages_from_date(
    app: &AppContext,
    topic_id: &str,
    get_messages_from_date: DateTimeAsMicroseconds,
    max_amount: usize,
) -> Result<Vec<Arc<MessageProtobufModel>>, OperationError> {
    let (minute, year) = app
        .index_by_minute_utils
        .get_minute_within_the_year(get_messages_from_date);

    let topic_data = super::topics::get_topic(app, topic_id).await?;

    let minute_index = topic_data.yearly_index_by_minute.lock().await;

    if let Some(yearly_index) = minute_index.get(&year) {
        let result =
            read_from_yearly_index(app, topic_data.as_ref(), yearly_index, &minute, max_amount);
        return result.await;
    }

    Ok(vec![])
}

async fn read_from_yearly_index(
    app: &AppContext,
    topic_data: &TopicData,
    yearly_index: &YearlyIndexByMinute,
    minute: &MinuteWithinYear,
    max_amount: usize,
) -> Result<Vec<Arc<MessageProtobufModel>>, OperationError> {
    let message_id = yearly_index.get_message_id(minute);

    if message_id.is_none() {
        return Ok(vec![]);
    }

    let message_id = message_id.unwrap();

    let page_id = MessagePageId::from_message_id(message_id);

    if let Some(page) = super::get_uncompressed_page_to_read(app, topic_data, &page_id).await {
        let result = page.read_from_message_id(message_id, max_amount).await;
        return Ok(result);
    }

    Ok(vec![]) //TODO - did not plug compressed page reading
}

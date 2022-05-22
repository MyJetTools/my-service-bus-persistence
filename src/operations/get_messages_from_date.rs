use std::sync::Arc;

use my_service_bus_shared::{protobuf_models::MessageProtobufModel, MessageId};
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{
    app::AppContext, index_by_minute::MinuteWithinYear, topic_data::TopicData, typing::Year,
};

use super::{
    read_page::{MessagesReader, ReadCondition},
    OperationError,
};

pub async fn get_messages_from_date(
    app: &Arc<AppContext>,
    topic_id: &str,
    get_messages_from_date: DateTimeAsMicroseconds,
    max_amount: usize,
) -> Result<Vec<MessageProtobufModel>, OperationError> {
    let (minute, year) = app
        .index_by_minute_utils
        .get_minute_within_the_year(get_messages_from_date);

    let topic_data = super::topics::get_topic(app, topic_id).await?;

    let message_id = get_message_id_from_yearly_index(app, &topic_data, minute, year).await?;

    if message_id.is_none() {
        return Ok(vec![]);
    }

    let mut result = Vec::new();

    let mut messages_reader = MessagesReader::new(
        app.clone(),
        topic_data.clone(),
        ReadCondition::Range {
            from_id: message_id.unwrap(),
            to_id: None,
            max_amount: Some(max_amount),
        },
    );

    while let Some(messages) = messages_reader.get_next_chunk().await {
        result.extend(messages)
    }

    Ok(result)
}

async fn get_message_id_from_yearly_index(
    app: &Arc<AppContext>,
    topic_data: &Arc<TopicData>,
    minute: MinuteWithinYear,
    year: Year,
) -> Result<Option<MessageId>, OperationError> {
    let mut minute_index = topic_data.yearly_index_by_minute.lock().await;

    if !minute_index.contains_key(&year) {
        let storage = app
            .open_yearly_index_storage_if_exists(topic_data.topic_id.as_str(), year)
            .await;

        if storage.is_none() {
            return Ok(None);
        }

        minute_index.insert(year, storage.unwrap());
    }

    let yearly_index_by_minute = minute_index.get_mut(&year).unwrap();

    let result = yearly_index_by_minute.get_message_id(&minute);

    return Ok(result);
}

use std::sync::Arc;

use my_service_bus_shared::{protobuf_models::MessageProtobufModel, MessageId};
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{app::AppContext, topic_data::TopicData};

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

    let message_id =
        get_message_id_from_yearly_index(app, &topic_data, get_messages_from_date).await?;

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
    get_messages_from_date: DateTimeAsMicroseconds,
) -> Result<Option<MessageId>, OperationError> {
    todo!("Implement");
    /*
    let minute_index = topic_data.yearly_index_by_minute.lock().await;

    yearly_index.get_message_id(minute)
     */
}

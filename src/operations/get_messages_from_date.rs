use my_service_bus_shared::protobuf_models::MessageProtobufModel;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::app::AppContext;

use super::OperationError;

pub async fn get_messages_from_date(
    app: &AppContext,
    topic_id: &str,
    get_messages_from_date: DateTimeAsMicroseconds,
    max_amount: usize,
) -> Result<Vec<MessageProtobufModel>, OperationError> {
    let topic_data = super::topics::get_topic(app, topic_id).await?;

    todo!("Implement");
}

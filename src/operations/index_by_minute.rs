use std::collections::HashMap;

use my_service_bus_shared::{bcl::BclToUnixMicroseconds, protobuf_models::MessageProtobufModel};

use crate::{
    app::AppContext,
    index_by_minute::{MinuteWithinYear, YearlyIndexByMinute},
    topic_data::TopicData,
};

pub async fn new_messages(
    app: &AppContext,
    topic_data: &TopicData,
    messages: &[MessageProtobufModel],
) {
    let mut index_by_minute = topic_data.yearly_index_by_minute.lock().await;

    for msg in messages {
        if let Some((minute_within_year, year)) = extract_year_and_minute_within_year(app, msg) {
            if !index_by_minute.contains_key(&year) {
                init_yearly_index(
                    app,
                    topic_data.topic_id.as_str(),
                    &mut index_by_minute,
                    year,
                )
                .await;
            }

            index_by_minute
                .get_mut(&year)
                .unwrap()
                .update_minute_index_if_new(&minute_within_year, msg.message_id);
        }
    }
}

fn extract_year_and_minute_within_year(
    app: &AppContext,
    msg: &MessageProtobufModel,
) -> Option<(MinuteWithinYear, u32)> {
    if let Some(created) = &msg.created {
        if let Ok(dt) = created.to_date_time() {
            return Some(app.index_by_minute_utils.get_minute_within_the_year(dt));
        }
    }

    None
}

async fn init_yearly_index(
    app: &AppContext,
    topic_id: &str,
    index_by_minute: &mut HashMap<u32, YearlyIndexByMinute>,
    year: u32,
) {
    let storage = app.create_index_storage(topic_id, year).await;
    let yearly_index_by_minute = YearlyIndexByMinute::new(year, storage).await;
    index_by_minute.insert(year, yearly_index_by_minute);
}

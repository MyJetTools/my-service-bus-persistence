use my_service_bus_shared::protobuf_models::MessageProtobufModel;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{
    app::TopicData, message_pages::MessagePageId,
    uncompressed_messages::messages_page_blob::MessagesPageBlob, utils::StopWatch,
};

pub async fn save_to_blob(
    page_blob: &mut MessagesPageBlob,
    data_by_topic: &TopicData,
    messages_to_save: &[MessageProtobufModel],
    page_id: MessagePageId,
) {
    let max_message_id = get_max_message_id(&messages_to_save);
    let mut sw = StopWatch::new();
    sw.start();
    let save_result = page_blob.save_messages(&messages_to_save[..]).await;
    sw.pause();
    match save_result {
        Ok(()) => {
            data_by_topic
                .metrics
                .update_last_saved_duration(sw.duration());

            data_by_topic
                .metrics
                .update_last_saved_chunk(messages_to_save.len());

            data_by_topic
                .metrics
                .update_last_saved_moment(DateTimeAsMicroseconds::now());

            data_by_topic
                .metrics
                .update_last_saved_message_id(max_message_id);
        }
        Err(error) => {
            data_by_topic
                .app
                .logs
                .add_info_string(
                    Some(data_by_topic.topic_id.as_str()),
                    "Saving messages",
                    format!(
                        "Can no save messages {}/#{} . Amount:{}, Reason: {:?}",
                        data_by_topic.topic_id,
                        page_id.value,
                        messages_to_save.len(),
                        error
                    ),
                )
                .await
        }
    }
}

fn get_max_message_id(msgs: &[MessageProtobufModel]) -> i64 {
    let mut max = msgs[0].message_id;

    for msg in msgs {
        if max < msg.message_id {
            max = msg.message_id;
        }
    }

    max
}

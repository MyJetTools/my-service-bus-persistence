use crate::{app::AppContext, topics_snapshot::DeletedTopicProtobufModel};

pub async fn restore_topic(app: &AppContext, topic_id: &str) -> Option<DeletedTopicProtobufModel> {
    app.topics_snapshot.remove_deleted_topic(topic_id).await
}

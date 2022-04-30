use crate::app::AppContext;

pub async fn init_new_topic(app: &AppContext, topic_id: &str) {
    if  app.topics_list.create_topic_data(topic_id).await {
        app.create_topic_folder(topic_id).await;
    }
}

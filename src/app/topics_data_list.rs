use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use super::{AppContext, TopicData};

pub struct TopicsDataList {
    data: RwLock<HashMap<String, Arc<TopicData>>>,
}

impl TopicsDataList {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get(&self, topic_id: &str) -> Option<Arc<TopicData>> {
        let read_access = self.data.read().await;

        let result = read_access.get(topic_id)?;

        return Some(result.clone());
    }

    pub async fn get_or_create_data_by_topic(
        &self,
        topic_id: &str,
        app: Arc<AppContext>,
    ) -> Arc<TopicData> {
        let data_by_topic = self.get(topic_id).await;
        if data_by_topic.is_some() {
            return data_by_topic.unwrap();
        }

        let mut write_access = self.data.write().await;

        let result = write_access.get(topic_id);

        if result.is_some() {
            return result.unwrap().clone();
        }

        let result = TopicData::new(topic_id, app);

        let result = Arc::new(result);

        write_access.insert(topic_id.to_string(), result.clone());

        result
    }
}

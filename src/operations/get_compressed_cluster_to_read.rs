use crate::compressed_page::*;
use crate::{app::AppContext, topic_data::TopicData};
use std::sync::Arc;

pub async fn get_compressed_cluster_to_read(
    app: &AppContext,
    topic_data: &TopicData,
    cluster_id: &CompressedClusterId,
) -> Option<Arc<CompressedCluster>> {
    let mut clusters = topic_data.compressed_clusters.lock().await;

    if clusters.contains_key(&cluster_id.value) {
        let result = clusters.get(&cluster_id.value).unwrap().clone();
        return Some(result);
    }

    let cluster = app
        .open_compressed_cluster_if_exists(topic_data.topic_id.as_str(), cluster_id.clone())
        .await?;

    clusters.insert(cluster_id.value, Arc::new(cluster));

    let result = clusters.get(&cluster_id.value).unwrap().clone();
    return Some(result);
}

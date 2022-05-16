use std::sync::Arc;

use my_service_bus_shared::{protobuf_models::MessageProtobufModel, MessageId};

use crate::compressed_page::*;

use crate::{
    app::AppContext,
    sub_page::{SubPage, SubPageId},
    topic_data::TopicData,
    uncompressed_page::UncompressedPageId,
};

use super::{ReadCondition, ReadFromCompressedClusters, ReadFromUncompressedPage};

pub struct MessagesReader {
    pub app: Arc<AppContext>,
    topic_data: Arc<TopicData>,
    current_uncompressed_page: Option<ReadFromUncompressedPage>,
    current_compressed_cluster: Option<ReadFromCompressedClusters>,
    read_condition: ReadCondition,
    current_message_id: MessageId,
    read_messages_amount: usize,
}

impl MessagesReader {
    pub fn new(
        app: Arc<AppContext>,
        topic_data: Arc<TopicData>,
        read_condition: ReadCondition,
    ) -> Self {
        Self {
            app,
            current_message_id: read_condition.get_from_message_id(),
            read_condition,
            current_uncompressed_page: None,
            current_compressed_cluster: None,
            topic_data,
            read_messages_amount: 0,
        }
    }

    pub async fn get_message(&mut self) -> Option<MessageProtobufModel> {
        let compressed_cluster_id = CompressedClusterId::from_message_id(self.current_message_id);
        let uncompressed_page_id = UncompressedPageId::from_message_id(self.current_message_id);

        self.read_subpage_if_needed(compressed_cluster_id, uncompressed_page_id)
            .await;

        let (sub_page, message_id, _) = self.get_next_sub_page_with_messages().await?;

        sub_page.get_message(message_id).await
    }

    async fn read_subpage_if_needed(
        &mut self,
        compressed_cluster_id: CompressedClusterId,
        uncompressed_page_id: UncompressedPageId,
    ) {
        let update_compressed_cluster =
            if let Some(current_compressed_cluster) = &self.current_compressed_cluster {
                current_compressed_cluster.cluster_id.value != compressed_cluster_id.value
            } else {
                true
            };

        if update_compressed_cluster {
            let compressed_cluster = crate::operations::get_compressed_cluster_to_read(
                self.app.as_ref(),
                self.topic_data.as_ref(),
                &compressed_cluster_id,
            )
            .await;

            self.current_compressed_cluster = Some(ReadFromCompressedClusters::new(
                compressed_cluster,
                compressed_cluster_id,
            ));
        }

        let update_uncompressed_page =
            if let Some(current_uncompressed_page) = &self.current_uncompressed_page {
                current_uncompressed_page.page_id.value != uncompressed_page_id.value
            } else {
                true
            };

        if update_uncompressed_page {
            let uncompressed_page = crate::operations::get_uncompressed_page_to_read(
                self.app.as_ref(),
                self.topic_data.as_ref(),
                &uncompressed_page_id,
            )
            .await;

            self.current_uncompressed_page = Some(ReadFromUncompressedPage::new(
                uncompressed_page,
                uncompressed_page_id,
            ));
        }
    }

    pub async fn get_next_sub_page_with_messages(
        &mut self,
    ) -> Option<(Arc<SubPage>, MessageId, MessageId)> {
        loop {
            if self
                .read_condition
                .we_reached_the_end(self.current_message_id, self.read_messages_amount)
            {
                return None;
            }

            let compressed_cluster_id =
                CompressedClusterId::from_message_id(self.current_message_id);
            let uncompressed_page_id = UncompressedPageId::from_message_id(self.current_message_id);

            self.read_subpage_if_needed(compressed_cluster_id, uncompressed_page_id)
                .await;

            let sub_page_id = SubPageId::from_message_id(self.current_message_id);

            if let Some(current_uncompressed_page) = &self.current_uncompressed_page {
                if let Some(sub_page) = current_uncompressed_page.get_sub_page(&sub_page_id).await {
                    let from_message_id = self.current_message_id;
                    let to_message_id = sub_page_id.get_first_message_id_of_next_page() - 1;
                    self.current_message_id = to_message_id + 1;
                    return Some((sub_page, from_message_id, to_message_id));
                }
            }

            if let Some(current_compressed_cluster) = &self.current_compressed_cluster {
                if let Some(sub_page) = current_compressed_cluster
                    .get_sub_page(
                        &sub_page_id,
                        self.topic_data.topic_id.as_str(),
                        self.app.logs.as_ref(),
                    )
                    .await
                {
                    let from_message_id = self.current_message_id;
                    let to_message_id = sub_page_id.get_first_message_id_of_next_page() - 1;
                    self.current_message_id = to_message_id + 1;
                    return Some((Arc::new(sub_page), from_message_id, to_message_id));
                }
            }

            self.current_message_id = sub_page_id.get_first_message_id_of_next_page();
        }
    }

    //TODO - Unit Test It
    pub async fn get_next_chunk(&mut self) -> Option<Vec<MessageProtobufModel>> {
        loop {
            let (sub_page, from_message_id, to_message_id) =
                self.get_next_sub_page_with_messages().await?;

            let result = sub_page.get_messages(from_message_id, to_message_id).await;

            if result.is_some() {
                return result;
            }
        }
    }
}

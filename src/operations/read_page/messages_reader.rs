use std::{collections::BTreeMap, sync::Arc};

use my_service_bus_shared::{protobuf_models::MessageProtobufModel, MessageId};

use crate::{
    app::AppContext,
    message_pages::{CompressedCluster, CompressedClusterId, MessagesPage},
    topic_data::TopicData,
};

pub const ITERATION_CHUNK: i64 = 1000;

pub enum ReadOpeartionMode {
    NotInitialized,
    Uncompressed {
        page: Arc<MessagesPage>,
        current_message_id: MessageId,
    },
    Compressed {
        cluster: Arc<CompressedCluster>,
        current_message_id: MessageId,
    },
    Finished,
}

impl ReadOpeartionMode {
    pub fn is_finished(&self) -> bool {
        match self {
            ReadOpeartionMode::Finished => true,
            _ => false,
        }
    }
}

pub struct MessagesReader {
    pub app: Arc<AppContext>,

    topic_data: Arc<TopicData>,
    read_mode: ReadOpeartionMode,
    from_message_id: MessageId,
    to_message_id: MessageId,
}

impl MessagesReader {
    pub fn new(
        app: Arc<AppContext>,
        topic_data: Arc<TopicData>,
        from_message_id: MessageId,
        to_message_id: MessageId,
    ) -> Self {
        Self {
            app,
            from_message_id,
            to_message_id,
            read_mode: ReadOpeartionMode::NotInitialized,
            topic_data,
        }
    }

    async fn init_if_needed(&mut self) {
        if let ReadOpeartionMode::NotInitialized = &self.read_mode {
            if let Some(page) = crate::operations::get_uncompressed_page_to_read(
                self.app.as_ref(),
                self.topic_data.as_ref(),
                &self.page_id,
            )
            .await
            {
                self.read_mode = ReadOpeartionMode::Uncompressed {
                    page,
                    current_message_id: self.page_id.get_first_message_id(),
                };
                return;
            }

            let cluster_id = CompressedClusterId::from_uncompressed_page_id(&self.page_id);

            if let Some(cluster) = crate::operations::get_compressed_cluster_to_read(
                self.app.as_ref(),
                self.topic_data.as_ref(),
                &cluster_id,
            )
            .await
            {
                self.read_mode = ReadOpeartionMode::Compressed {
                    cluster,
                    current_message_id: self.page_id.get_first_message_id(),
                };
                return;
            }
        }
    }

    pub async fn get_next_chunk(&mut self) -> Option<BTreeMap<MessageId, MessageProtobufModel>> {
        todo!("Implement");
        /*
        self.init_if_needed().await;

        match &mut self.read_mode {
            ReadOpeartionMode::NotInitialized => panic!("Should not be here"),
            ReadOpeartionMode::Uncompressed {
                page,
                current_message_id,
            } => {
                let to_message_id = *current_message_id + ITERATION_CHUNK - 1;

                let result = page.read_range(*current_message_id, to_message_id).await;

                *current_message_id += ITERATION_CHUNK;
                if MessagePageId::from_message_id(*current_message_id).value > self.page_id.value {
                    self.read_mode = ReadOpeartionMode::Finished;
                }

                return Some(result);
            }
            ReadOpeartionMode::Compressed {
                cluster,
                current_message_id,
            } => {
                let compressed_page_id = CompressedPageId::from_message_id(*current_message_id);

                let result = if let Some(items) = cluster
                    .get_compressed_page_messages(&compressed_page_id)
                    .await
                    .unwrap()
                {
                    items
                } else {
                    BTreeMap::new()
                };

                *current_message_id += ITERATION_CHUNK;
                if MessagePageId::from_message_id(*current_message_id).value > self.page_id.value {
                    self.read_mode = ReadOpeartionMode::Finished;
                }

                return Some(result);
            }
            ReadOpeartionMode::Finished => return None,
        }
         */
    }
}

fn to_btree_map(pages: Vec<MessageProtobufModel>) -> BTreeMap<MessageId, MessageProtobufModel> {
    let mut result = BTreeMap::new();

    for page in pages {
        result.insert(page.message_id, page);
    }

    result
}

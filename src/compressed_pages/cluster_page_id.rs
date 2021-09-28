use crate::message_pages::MessagePageId;

const PAGES_IN_CLUSTER: i64 = 100;

#[derive(Clone, Copy)]
pub struct ClusterPageId {
    pub value: i64,
}

impl ClusterPageId {
    pub fn from_page_id(page_id: &MessagePageId) -> Self {
        Self {
            value: page_id.value / PAGES_IN_CLUSTER,
        }
    }

    pub fn get_first_page_id_on_compressed_page(&self) -> MessagePageId {
        let page_id = self.value * PAGES_IN_CLUSTER;

        MessagePageId { value: page_id }
    }
}

use my_service_bus_shared::MessageId;

use crate::message_pages::MessagePageId;

use super::{utils::*, CompressedPageId};

#[derive(Debug, Clone)]
pub struct CompressedClusterId {
    pub value: usize,
}

impl CompressedClusterId {
    pub fn from_message_id(message_id: MessageId) -> Self {
        Self {
            value: (message_id as usize) / MESSAGES_PER_CLUSTER,
        }
    }
    pub fn from_compressed_page_id(compressed_page_id: &CompressedPageId) -> Self {
        Self {
            value: compressed_page_id.value / PAGES_PER_CLUSTER,
        }
    }

    pub fn from_uncompressed_page_id(page_id: &MessagePageId) -> Self {
        todo!("Implement")
    }
}

#[cfg(test)]
mod test {
    use crate::message_pages::CompressedPageId;

    use super::*;

    #[test]
    fn test_from_compressed_page_id() {
        let compressed_page_id = CompressedPageId::new(0);
        assert_eq!(
            0,
            CompressedClusterId::from_compressed_page_id(&compressed_page_id).value
        )
    }
}

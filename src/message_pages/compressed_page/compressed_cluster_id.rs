use my_service_bus_shared::MessageId;

use crate::sub_page::SubPageId;

use super::utils::*;

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

    pub fn from_sub_page_id(compressed_page_id: &SubPageId) -> Self {
        Self {
            value: compressed_page_id.value / SUB_PAGES_PER_CLUSTER,
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_from_sub_page_id() {
        assert_eq!(
            0,
            CompressedClusterId::from_sub_page_id(&SubPageId::new(0)).value
        );

        assert_eq!(
            0,
            CompressedClusterId::from_sub_page_id(&SubPageId::new(1)).value
        );

        assert_eq!(
            0,
            CompressedClusterId::from_sub_page_id(&SubPageId::new(99_999)).value
        );

        assert_eq!(
            1,
            CompressedClusterId::from_sub_page_id(&SubPageId::new(100_000)).value
        );
    }
}

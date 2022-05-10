use my_service_bus_shared::MessageId;

use super::utils::*;

pub struct CompressedPageId {
    pub value: usize,
}

impl CompressedPageId {
    pub fn new(value: usize) -> Self {
        Self { value }
    }
    pub fn from_message_id(message_id: MessageId) -> Self {
        Self {
            value: (message_id as usize) / MESSAGES_PER_COMPRESSED_PAGE,
        }
    }

    pub fn get_first_message_id(&self) -> MessageId {
        (self.value * MESSAGES_PER_COMPRESSED_PAGE) as MessageId
    }

    pub fn get_page_id_within_cluster(&self) -> usize {
        let sub_result = self.value / PAGES_PER_CLUSTER;

        self.value - sub_result * PAGES_PER_CLUSTER
    }
}

#[cfg(test)]
mod test {
    use crate::message_pages::*;

    #[test]
    fn test_first_message_id() {
        assert_eq!(0, CompressedPageId::new(0).get_first_message_id());
        assert_eq!(1000, CompressedPageId::new(1).get_first_message_id());
        assert_eq!(2000, CompressedPageId::new(2).get_first_message_id());
    }

    #[test]
    fn test_page_id_within_cluster() {
        assert_eq!(0, CompressedPageId::new(0).get_page_id_within_cluster());
        assert_eq!(1, CompressedPageId::new(1).get_page_id_within_cluster());
        assert_eq!(2, CompressedPageId::new(2).get_page_id_within_cluster());
        assert_eq!(
            99_999,
            CompressedPageId::new(99_999).get_page_id_within_cluster()
        );

        assert_eq!(
            0,
            CompressedPageId::new(100_000).get_page_id_within_cluster()
        );
        assert_eq!(
            1,
            CompressedPageId::new(100_001).get_page_id_within_cluster()
        );
        assert_eq!(
            2,
            CompressedPageId::new(100_002).get_page_id_within_cluster()
        );
        assert_eq!(
            99_999,
            CompressedPageId::new(199_999).get_page_id_within_cluster()
        );
    }
}

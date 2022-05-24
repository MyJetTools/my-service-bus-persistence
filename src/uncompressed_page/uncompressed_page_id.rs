use my_service_bus_shared::sub_page::SubPageId;

use my_service_bus_shared::{page_id::PageId, sub_page::SUB_PAGE_MESSAGES_AMOUNT, MessageId};

use crate::toc::PayloadNo;

use super::utils::MESSAGES_PER_PAGE;

#[derive(Clone, Copy)]
pub struct UncompressedPageId {
    pub value: PageId,
}

impl UncompressedPageId {
    pub fn new(page_id: PageId) -> Self {
        Self { value: page_id }
    }
    pub fn from_message_id(message_id: i64) -> Self {
        Self {
            value: super::utils::get_message_page_id(message_id),
        }
    }

    pub fn get_first_sub_page_id(&self) -> SubPageId {
        let result = (self.value * MESSAGES_PER_PAGE) as usize / SUB_PAGE_MESSAGES_AMOUNT;
        SubPageId::new(result)
    }

    #[cfg(test)]
    pub fn from_sub_page_id(sub_page_id: &SubPageId) -> Self {
        Self::from_message_id(sub_page_id.get_first_message_id())
    }

    pub fn get_payload_no_inside_uncompressed_file(&self, message_id: MessageId) -> PayloadNo {
        let first_message_id = self.value * MESSAGES_PER_PAGE as i64;
        let result = message_id - first_message_id;
        PayloadNo::new(result as usize)
    }
}

#[cfg(test)]
mod test {
    use my_service_bus_shared::sub_page::SubPageId;

    use super::UncompressedPageId;

    #[test]
    fn test_get_payload_no_inside_uncompressed_file() {
        let page_id = UncompressedPageId::new(0);

        let mut result = 0;
        for message_id in 0..100_000 {
            assert_eq!(
                result,
                page_id
                    .get_payload_no_inside_uncompressed_file(message_id)
                    .value
            );

            result += 1;
        }

        let page_id = UncompressedPageId::new(1);
        let mut result = 0;
        for message_id in 100_000..200_000 {
            assert_eq!(
                result,
                page_id
                    .get_payload_no_inside_uncompressed_file(message_id)
                    .value
            );

            result += 1;
        }

        let page_id = UncompressedPageId::new(2);
        let mut result = 0;
        for message_id in 200_000..300_000 {
            assert_eq!(
                result,
                page_id
                    .get_payload_no_inside_uncompressed_file(message_id)
                    .value
            );

            result += 1;
        }
    }

    #[test]
    pub fn test_from_sub_page_id() {
        for sub_page_id in 0..100 {
            assert_eq!(
                0,
                UncompressedPageId::from_sub_page_id(&SubPageId::new(sub_page_id)).value
            );
        }

        for sub_page_id in 100..200 {
            assert_eq!(
                1,
                UncompressedPageId::from_sub_page_id(&SubPageId::new(sub_page_id)).value
            );
        }

        for sub_page_id in 200..300 {
            assert_eq!(
                2,
                UncompressedPageId::from_sub_page_id(&SubPageId::new(sub_page_id)).value
            );
        }
    }
}

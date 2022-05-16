use my_service_bus_shared::MessageId;

use super::{SubPageId, SUB_PAGE_MESSAGES_AMOUNT};

pub struct SubPageMessagesIterator {
    pub current_message_id: MessageId,
    pub max_message_id: MessageId,
}

impl SubPageMessagesIterator {
    pub fn new(sub_page_id: &SubPageId) -> Self {
        let current_message_id = sub_page_id.value * SUB_PAGE_MESSAGES_AMOUNT;

        Self {
            current_message_id: current_message_id as MessageId,
            max_message_id: (current_message_id + SUB_PAGE_MESSAGES_AMOUNT) as MessageId,
        }
    }
}

impl Iterator for SubPageMessagesIterator {
    type Item = MessageId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_message_id >= self.max_message_id {
            return None;
        }

        let result = self.current_message_id;
        self.current_message_id += 1;
        return Some(result);
    }
}

#[cfg(test)]
mod test {
    use my_service_bus_shared::MessageId;

    use crate::sub_page::*;

    #[test]
    pub fn test_iterator() {
        let sub_page_id = SubPageId::new(0);

        let result: Vec<MessageId> = sub_page_id.iterate_through_messages().collect();

        assert_eq!(SUB_PAGE_MESSAGES_AMOUNT, result.len());
        assert_eq!(0, result[0]);
        assert_eq!(999, result[999]);

        let sub_page_id = SubPageId::new(1);

        let result: Vec<MessageId> = sub_page_id.iterate_through_messages().collect();

        assert_eq!(SUB_PAGE_MESSAGES_AMOUNT, result.len());
        assert_eq!(1_000, result[0]);
        assert_eq!(1_999, result[999]);
    }
}

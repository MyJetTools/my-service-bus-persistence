use my_service_bus_shared::MessageId;

pub enum ReadCondition {
    SingleMessage(MessageId),
    Range {
        from_id: MessageId,
        to_id: Option<MessageId>,
        max_amount: Option<usize>,
    },
}

impl ReadCondition {
    pub fn as_from_to(from_id: MessageId, to_id: MessageId) -> Self {
        ReadCondition::Range {
            from_id,
            to_id: Some(to_id),
            max_amount: None,
        }
    }
    pub fn get_from_message_id(&self) -> MessageId {
        match self {
            ReadCondition::SingleMessage(message_id) => *message_id,
            ReadCondition::Range { from_id, .. } => *from_id,
        }
    }

    pub fn we_reached_the_end(
        &self,
        current_message_id: MessageId,
        read_messages_amount: usize,
    ) -> bool {
        match self {
            ReadCondition::SingleMessage(message_id) => read_messages_amount > 0,
            ReadCondition::Range {
                from_id,
                to_id,
                max_amount,
            } => {
                if let Some(to_id) = to_id {
                    if current_message_id > *to_id {
                        return true;
                    }
                }

                if let Some(max_amount) = max_amount {
                    *max_amount >= read_messages_amount
                } else {
                    false
                }
            }
        }
    }
}

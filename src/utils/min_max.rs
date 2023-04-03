use my_service_bus_abstractions::MessageId;

pub struct MinMax {
    min: i64,
    max: i64,
}

impl MinMax {
    pub fn new(message_id: MessageId) -> Self {
        MinMax {
            min: message_id.get_value(),
            max: message_id.get_value(),
        }
    }
    pub fn update(&mut self, message_id: MessageId) {
        let value = message_id.get_value();
        if self.min > value {
            self.min = value;
        }

        if self.max < value {
            self.max = value;
        }
    }
    pub fn get_min(&self) -> MessageId {
        MessageId::new(self.min)
    }

    pub fn get_max(&self) -> MessageId {
        MessageId::new(self.max)
    }
}

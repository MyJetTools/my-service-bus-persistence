use std::collections::HashMap;

use my_service_bus_abstractions::MessageId;

use crate::uncompressed_page_storage::toc::MessageContentOffset;

pub struct ReadInterval {
    pub messages: HashMap<i64, MessageContentOffset>,
    pub start_pos: usize,
    pub len: usize,
}

impl ReadInterval {
    pub fn new(offset: MessageContentOffset, message_id: MessageId) -> Self {
        let mut result = Self {
            messages: HashMap::new(),
            start_pos: offset.offset,
            len: offset.size,
        };

        result.messages.insert(message_id.into(), offset);

        result
    }
    pub fn is_my_interval_to_append(&self, offset: &MessageContentOffset) -> bool {
        self.start_pos + self.len == offset.offset
    }

    pub fn append(&mut self, offset: MessageContentOffset, message_id: MessageId) {
        self.len += offset.size;
        self.messages.insert(message_id.into(), offset);
    }

    pub fn read_payload<'s>(&self, message_id: MessageId, payload: &'s [u8]) -> Option<&'s [u8]> {
        let offset = self.messages.get(message_id.as_ref())?;

        let start_offset = offset.offset - self.start_pos;

        let result = &payload[start_offset..start_offset + offset.size];

        Some(result)
    }
}

pub struct ReadIntervalsCompiler {
    pub intervals: Vec<ReadInterval>,
}

impl ReadIntervalsCompiler {
    pub fn new() -> Self {
        Self {
            intervals: Vec::new(),
        }
    }

    fn find_my_interval(&mut self, offset: &MessageContentOffset) -> Option<&mut ReadInterval> {
        for interval in &mut self.intervals {
            if interval.is_my_interval_to_append(offset) {
                return Some(interval);
            }
        }

        None
    }

    pub fn add_new_interval(&mut self, message_id: MessageId, offset: MessageContentOffset) {
        if let Some(my_interval) = self.find_my_interval(&offset) {
            my_interval.append(offset, message_id);
            return;
        }

        self.intervals.push(ReadInterval::new(offset, message_id));
    }

    pub fn read_payload<'s>(&self, message_id: MessageId, payload: &'s [u8]) -> &'s [u8] {
        for interval in &self.intervals {
            if let Some(result) = interval.read_payload(message_id, payload) {
                return result;
            }
        }

        panic!("Payload for message id {} is not found.", { message_id });
    }
}

#[cfg(test)]
mod test {
    use crate::uncompressed_page_storage::toc::MessageContentOffset;

    use super::ReadIntervalsCompiler;

    #[test]
    fn test_interval_join() {
        let mut read_interval_compilers = ReadIntervalsCompiler::new();

        read_interval_compilers.add_new_interval(1.into(), MessageContentOffset::new(0, 10));
        read_interval_compilers.add_new_interval(2.into(), MessageContentOffset::new(10, 5));
        read_interval_compilers.add_new_interval(3.into(), MessageContentOffset::new(15, 3));

        assert_eq!(1, read_interval_compilers.intervals.len());
    }

    #[test]
    fn test_intervals_reading() {
        let mut read_interval_compilers = ReadIntervalsCompiler::new();

        read_interval_compilers.add_new_interval(1.into(), MessageContentOffset::new(2, 10));
        read_interval_compilers.add_new_interval(2.into(), MessageContentOffset::new(12, 5));
        read_interval_compilers.add_new_interval(3.into(), MessageContentOffset::new(17, 3));

        let payload0 = vec![1u8; 10];
        let payload1 = vec![2u8; 5];
        let payload2 = vec![3u8; 3];

        let mut payload = Vec::new();

        payload.extend_from_slice([0u8; 2].as_slice());
        payload.extend_from_slice(payload0.as_slice());
        payload.extend_from_slice(payload1.as_slice());
        payload.extend_from_slice(payload2.as_slice());

        let result_payload0 = read_interval_compilers.read_payload(1.into(), &payload[2..]);
        let result_payload1 = read_interval_compilers.read_payload(2.into(), &payload[2..]);
        let result_payload2 = read_interval_compilers.read_payload(3.into(), &payload[2..]);

        assert_eq!(payload0.as_slice(), result_payload0);
        assert_eq!(payload1.as_slice(), result_payload1);
        assert_eq!(payload2.as_slice(), result_payload2);
    }
}

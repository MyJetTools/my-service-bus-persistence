use super::page_blob_utils;

pub struct PageBlobBuffer {
    buffer: Vec<u8>,
    position: usize,
    page_size: usize,
}

impl PageBlobBuffer {
    pub fn new(page_size: usize, capacity_in_pages: usize) -> Self {
        let result = Self {
            buffer: Vec::with_capacity(capacity_in_pages * page_size),
            position: 0,
            page_size,
        };
        result
    }

    pub fn available_to_read_size(&self) -> usize {
        self.buffer.len() - self.position
    }

    pub fn get_next_message_size(&mut self) -> Option<usize> {
        let len = self.available_to_read_size();

        if len < 4 {
            self.gc();
            return None;
        }

        let mut array = [0u8; 4];
        let slice = &self.buffer[self.position..self.position + 4];

        array.copy_from_slice(slice);

        let result = u32::from_le_bytes(array);

        if result > 0 {
            self.position += 4;
        }

        Some(result as usize)
    }

    pub fn get_payload<'t>(&'t mut self, data_size: usize) -> Option<&'t [u8]> {
        let len = self.available_to_read_size();

        if len < data_size {
            return None;
        }

        let result = &self.buffer[self.position..self.position + data_size];

        self.position += data_size;
        return Some(result);
    }

    pub fn get_remaining_payload<'t>(&'t mut self) -> &'t [u8] {
        let data_size = self.available_to_read_size();

        let result = &self.buffer[self.position..self.position + data_size];

        self.position += data_size;

        return result;
    }

    pub fn get_buffer_size_to_append(&self) -> usize {
        self.buffer.capacity() - self.buffer.len()
    }

    fn gc(&mut self) {
        if self.position < self.page_size {
            return;
        }

        if self.position == self.capacity() {
            self.position = 0;
            self.buffer.clear();
            return;
        }

        let pages_to_gc = self.position / self.page_size;

        let bytes_to_gc = pages_to_gc * self.page_size;

        let slice_to_move = &self.buffer[bytes_to_gc..].to_vec();

        self.buffer[..slice_to_move.len()].copy_from_slice(slice_to_move);

        let current_len = self.buffer.len();

        unsafe { self.buffer.set_len(current_len - bytes_to_gc) }

        self.position -= bytes_to_gc;
    }

    pub fn append(&mut self, items: &[u8]) {
        self.gc();
        let new_size = self.buffer.len() + items.len();
        if new_size > self.buffer.capacity() {
            panic!("We are trying to insert the array which will make data size {} which is bigger than capacity can hold {}", new_size, self.buffer.capacity());
        }

        self.buffer.extend(items);
    }

    pub fn init_buffer_and_set_position(&mut self, items: &[u8], position: usize) {
        self.buffer.clear();
        self.buffer.extend(items);
        self.position = position;
    }

    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    pub fn get_last_page(&self) -> &[u8] {
        let page_no =
            page_blob_utils::get_page_no_from_page_blob_position(self.position, self.page_size);

        let pages_start = page_no * self.page_size;

        &self.buffer[pages_start..self.position]
    }

    pub fn set_last_page(&mut self, last_page: &[u8]) {
        self.buffer.clear();
        self.buffer.extend(last_page);
        self.position = last_page.len();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_size_to_append() {
        let mut buffer = PageBlobBuffer::new(16, 2);

        let buffer_size_to_append = buffer.get_buffer_size_to_append();

        assert_eq!(16 * 2, buffer_size_to_append);

        buffer.append(&[0u8, 0u8]);

        let buffer_size_to_append = buffer.get_buffer_size_to_append();

        assert_eq!(16 * 2 - 2, buffer_size_to_append);
    }

    #[test]
    fn test_flow_in_one_page() {
        let mut buffer = PageBlobBuffer::new(16, 2);

        buffer.append(&[
            2u8, 0u8, 0u8, 0u8, 15u8, 16u8, //First Message
            3u8, 0u8, 0u8, 0u8, 17u8, 18u8, 19u8, //Second Message
            0u8, 0u8, 0u8, 0u8,
        ]);

        {
            let msg_size = buffer.get_next_message_size().unwrap();

            assert_eq!(msg_size, 2);

            let buffer = buffer.get_payload(msg_size).unwrap();

            assert_eq!(buffer.len(), 2);
            assert_eq!(buffer[0], 15);
            assert_eq!(buffer[1], 16);
        }

        {
            let msg_size = buffer.get_next_message_size().unwrap();

            assert_eq!(msg_size, 3);

            let buffer = buffer.get_payload(msg_size).unwrap();

            assert_eq!(buffer.len(), 3);
            assert_eq!(buffer[0], 17);
            assert_eq!(buffer[1], 18);
            assert_eq!(buffer[2], 19);
        }

        let msg_size = buffer.get_next_message_size().unwrap();

        assert_eq!(msg_size, 0);
    }

    #[test]
    fn test_gc_case() {
        let mut buffer = PageBlobBuffer::new(8, 2);

        buffer.append(&[
            9u8, 0u8, 0u8, 0u8, // First Message Len
            15u8, 16u8, 17u8, 18u8, // First page
            19u8, 20u8, 21u8, 22u8, 23u8, 3u8, 0u8, 0u8, // Second Message Len unfinished
        ]);

        assert_eq!(16, buffer.buffer.len());

        let msg_size = buffer.get_next_message_size().unwrap();

        assert_eq!(9, msg_size);

        let msg = buffer.get_payload(msg_size).unwrap();

        assert_eq!(9, msg.len());

        let msg_size = buffer.get_next_message_size();

        assert_eq!(true, msg_size.is_none());
        assert_eq!(3, buffer.available_to_read_size());

        buffer.append(&[0u8, 17u8, 18u8, 19u8, 0u8, 0u8, 0u8, 0u8]);

        assert_eq!(11, buffer.available_to_read_size());

        let msg_size = buffer.get_next_message_size();

        assert_eq!(3, msg_size.unwrap());
    }

    #[test]
    fn test_get_last_page() {
        let mut buffer = PageBlobBuffer::new(4, 16);

        buffer.append(&[1u8, 1u8, 1u8, 1u8, 2u8, 2u8]);

        buffer.position = 6;

        let last_page = buffer.get_last_page();

        println!("{:?}", last_page);

        assert_bytes(last_page, &[2u8, 2u8])
    }

    fn assert_bytes(left: &[u8], right: &[u8]) {
        assert_eq!(left.len(), right.len());

        for i in 0..left.len() {
            let left_b = left[i];
            let right_b = right[i];
            assert_eq!(left_b, right_b);
        }
    }
}

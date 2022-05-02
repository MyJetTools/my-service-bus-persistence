use my_service_bus_shared::{page_id::PageId, MessageId};

pub fn get_file_no_inside_uncompressed_file(page_id: PageId, message_id: MessageId) -> usize {
    let first_message_id = page_id * 100_000;
    let result = message_id - first_message_id;
    result as usize
}

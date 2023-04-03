use my_service_bus_abstractions::MessageId;
use my_service_bus_shared::page_id::PageId;

pub fn get_file_no_inside_uncompressed_file(page_id: PageId, message_id: MessageId) -> usize {
    let first_message_id = page_id.get_value() * 100_000;
    let result = message_id.get_value() - first_message_id;
    result as usize
}

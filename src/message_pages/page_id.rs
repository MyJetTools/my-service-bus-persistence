#[derive(Clone, Copy)]
pub struct MessagePageId {
    pub value: i64,
}

impl MessagePageId {
    pub fn new(page_id: i64) -> Self {
        Self { value: page_id }
    }
    pub fn from_message_id(message_id: i64) -> Self {
        Self {
            value: super::utils::get_message_page_id(message_id),
        }
    }
}

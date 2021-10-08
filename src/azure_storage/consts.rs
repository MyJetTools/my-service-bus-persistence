use crate::message_pages::MessagePageId;

pub const SYSTEM_BLOB_NAME: &str = "system";

pub fn generate_blob_name(page_id: &MessagePageId) -> String {
    return format!("{:019}", page_id.value);
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_name() {
        let page_id = MessagePageId { value: 1 };

        let blob_name = generate_blob_name(&page_id);

        assert_eq!("0000000000000000001", blob_name);

        let page_id = MessagePageId { value: 154 };

        let blob_name = generate_blob_name(&page_id);

        assert_eq!("0000000000000000154", blob_name);
    }
}

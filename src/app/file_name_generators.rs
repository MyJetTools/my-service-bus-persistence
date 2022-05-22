use crate::uncompressed_page::UncompressedPageId;

pub const SYSTEM_FILE_NAME: &str = "system";

pub fn generate_uncompressed_blob_name(page_id: &UncompressedPageId) -> String {
    return format!("{:019}.uncompressed", page_id.value);
}

pub fn generate_year_index_blob_name(year: u32) -> String {
    return format!(".{}.yearindex", year);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_name() {
        let page_id = UncompressedPageId::new(1);

        let blob_name = generate_uncompressed_blob_name(&page_id);

        assert_eq!("0000000000000000001.uncompressed", blob_name);

        let page_id = UncompressedPageId::new(154);

        let blob_name = generate_uncompressed_blob_name(&page_id);

        assert_eq!("0000000000000000154.uncompressed", blob_name);
    }
}

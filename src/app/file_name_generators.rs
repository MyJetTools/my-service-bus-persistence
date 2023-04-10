use my_service_bus_shared::page_id::PageId;

pub const SYSTEM_FILE_NAME: &str = "system";

pub const SOFT_DELETE_METADATA_FILE_NAME: &str = ".deleted";

pub fn generate_uncompressed_blob_name(page_id: PageId) -> String {
    return format!("{:019}.uncompressed", page_id.get_value());
}

pub fn generate_year_index_blob_name(year: u32) -> String {
    return format!(".{}.yearindex", year);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_name() {
        let page_id = PageId::new(1);

        let blob_name = generate_uncompressed_blob_name(page_id);

        assert_eq!("0000000000000000001.uncompressed", blob_name);

        let page_id = PageId::new(154);

        let blob_name = generate_uncompressed_blob_name(page_id);

        assert_eq!("0000000000000000154.uncompressed", blob_name);
    }
}

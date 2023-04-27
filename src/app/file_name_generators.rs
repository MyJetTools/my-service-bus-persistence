pub const SOFT_DELETE_METADATA_FILE_NAME: &str = ".deleted";

pub fn generate_year_index_blob_name(year: u32) -> String {
    return format!(".{}.yearindex", year);
}

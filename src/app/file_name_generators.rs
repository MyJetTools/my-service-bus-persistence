use crate::typing::Year;

pub fn generate_year_index_blob_name(year: Year) -> String {
    return format!(".{}.yearindex", year.get_value());
}

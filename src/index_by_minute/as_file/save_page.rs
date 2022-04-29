use crate::{file_random_access::FileRandomAccess, page_blob_utils::PageBlobPageId};

pub async fn save_page(as_file: &mut FileRandomAccess, page_no: &PageBlobPageId, content: &[u8]) {
    let pos = page_no.get_absolute_position();
    as_file.set_position(pos).await.unwrap();
    as_file.write_to_file(content).await.unwrap();
}

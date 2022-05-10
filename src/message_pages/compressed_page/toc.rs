use my_service_bus_shared::MessageId;

use crate::page_blob_random_access::PageBlobRandomAccess;

use super::{utils::PAGES_PER_CLUSTER, CompressedPageId};

pub struct CompressedTocOffset {
    pub start_pos: usize,
    pub size: usize,
}

pub fn get_compressed_page_offset_within_cluster(compressed_page_id: &CompressedPageId) -> usize {
    let cluster_page_id = compressed_page_id.value / PAGES_PER_CLUSTER;

    let offset_within_page = compressed_page_id.value - cluster_page_id * PAGES_PER_CLUSTER;

    offset_within_page * 8
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_compressed_page_offset_within_cluster() {
        assert_eq!(
            0,
            get_compressed_page_offset_within_cluster(&CompressedPageId::new(0))
        );

        assert_eq!(
            8,
            get_compressed_page_offset_within_cluster(&CompressedPageId::new(1))
        );

        assert_eq!(
            8,
            get_compressed_page_offset_within_cluster(&CompressedPageId::new(2))
        );

        assert_eq!(
            0,
            get_compressed_page_offset_within_cluster(&CompressedPageId::new(100_000))
        );
    }
}

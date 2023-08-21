use my_azure_page_blob_ext::MyAzurePageBlobStorageWithRetries;
use my_azure_page_blob_random_access::PageBlobRandomAccess;

use my_service_bus_shared::sub_page::SubPageId;
use rust_extensions::BinaryPayloadBuilder;

use super::{consts::TOC_STRUCTURE_SIZE, ArchiveFileNo};

pub struct SubPagePosition {
    pub offset: u64,
    pub length: u32,
}

pub async fn read_file_position(
    page_blob: &PageBlobRandomAccess<MyAzurePageBlobStorageWithRetries>,
    archive_file_no: ArchiveFileNo,
    sub_page_id: SubPageId,
) -> SubPagePosition {
    let offset = archive_file_no.get_toc_offset(sub_page_id);

    let result = page_blob.read(offset, TOC_STRUCTURE_SIZE).await.unwrap();

    let mut offset = [0u8; 8];
    let mut length = [0u8; 4];

    offset.clone_from_slice(result.as_slice()[0..8].as_ref());

    length.clone_from_slice(result.as_slice()[8..12].as_ref());

    let offset = u64::from_le_bytes(offset);
    let length = u32::from_le_bytes(length);

    SubPagePosition { offset, length }
}

pub async fn write_file_position(
    page_blob: &PageBlobRandomAccess<MyAzurePageBlobStorageWithRetries>,
    archive_file_no: ArchiveFileNo,
    sub_page_id: SubPageId,
    pos: SubPagePosition,
) {
    let offset = archive_file_no.get_toc_offset(sub_page_id);

    let mut payload = [0u8; TOC_STRUCTURE_SIZE];
    let mut buffer_builder = BinaryPayloadBuilder::new_as_slice(&mut payload);

    buffer_builder.write_u64(pos.offset);
    buffer_builder.write_u32(pos.length);

    page_blob.write(offset, buffer_builder).await.unwrap();
}

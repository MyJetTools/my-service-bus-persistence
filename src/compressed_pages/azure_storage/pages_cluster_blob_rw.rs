use std::sync::Arc;

use my_azure_page_blob::MyAzurePageBlob;
use my_azure_storage_sdk::AzureStorageConnection;

use crate::{compressed_pages::RestoreCompressedPageError, message_pages::MessagePageId};

use super::{super::ClusterPageId, PagesClusterAzureBlob};

pub struct PagesClusterBlobRw {
    pub topic_id: String,
    pub cluster_page_id: ClusterPageId,
    blob_data: PagesClusterAzureBlob<MyAzurePageBlob>,
}

impl PagesClusterBlobRw {
    pub fn new(
        connection: Arc<AzureStorageConnection>,
        topic_id: &str,
        cluster_page_id: ClusterPageId,
    ) -> Self {
        let blob_name = get_cluster_blob_name(&cluster_page_id);

        let my_page_blob = MyAzurePageBlob::new(connection, topic_id.to_string(), blob_name);

        let blob_data = PagesClusterAzureBlob::new(my_page_blob, &cluster_page_id);

        Self {
            cluster_page_id,
            blob_data: blob_data,
            topic_id: topic_id.to_string(),
        }
    }

    pub async fn read(
        &mut self,
        page_id: MessagePageId,
    ) -> Result<Option<Vec<u8>>, RestoreCompressedPageError> {
        let zip = self.blob_data.read(&page_id).await?;

        let zip_archive = recompress_if_needed(zip.get_result())?;

        match zip_archive {
            Some(src) => Ok(Some(src.to_vec())),
            None => Ok(None),
        }
    }

    pub async fn write(
        &mut self,
        page_id: MessagePageId,
        zip: &[u8],
    ) -> Result<(), RestoreCompressedPageError> {
        self.blob_data.write(page_id, zip).await?;
        Ok(())
    }
}

fn recompress_if_needed<'s>(
    zip_data: &'s [u8],
) -> Result<Option<Vec<u8>>, RestoreCompressedPageError> {
    if super::utils::check_zip_v2(zip_data)? {
        return Ok(Some(zip_data.to_vec()));
    }

    let pages = super::utils::decompress_v1(zip_data)?;

    if pages.is_none() {
        return Ok(None);
    }

    let pages = pages.unwrap();

    let mut pages_cluster_builder =
        my_service_bus_shared::page_compressor::CompressedPageBuilder::new();

    for page in pages {
        let mut dest_buf = Vec::new();
        page.serialize(&mut dest_buf)?;
        pages_cluster_builder.add_message(page.message_id, &dest_buf)?;
    }

    Ok(Some(pages_cluster_builder.get_payload()?))
}

fn get_cluster_blob_name(cluster: &ClusterPageId) -> String {
    format!("cluster-{:019}.zip", cluster.value)
}

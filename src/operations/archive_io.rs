use my_azure_page_blob_random_access::PageBlobRandomAccessError;
use my_service_bus_shared::{page_compressor::CompressedPageReaderError, sub_page::SubPageId};
use rust_extensions::{date_time::DateTimeAsMicroseconds, StopWatch};

use crate::{
    app::AppContext,
    message_pages::{SubPage, SubPageInner},
    topic_data::TopicData,
};

#[derive(Debug)]
pub enum RestoreSubPageError {
    NotFound,
    PageBlobRandomAccessError(PageBlobRandomAccessError),
    CompressedPageReaderError(CompressedPageReaderError),
}

impl From<CompressedPageReaderError> for RestoreSubPageError {
    fn from(err: CompressedPageReaderError) -> Self {
        Self::CompressedPageReaderError(err)
    }
}

impl From<PageBlobRandomAccessError> for RestoreSubPageError {
    fn from(err: PageBlobRandomAccessError) -> Self {
        Self::PageBlobRandomAccessError(err)
    }
}

pub async fn restore_sub_page(
    app: &AppContext,
    topic_data: &TopicData,
    sub_page_id: SubPageId,
) -> Result<SubPage, RestoreSubPageError> {
    let page_blob_storage = topic_data
        .archive_pages_list
        .try_get_or_open(sub_page_id.into(), topic_data.topic_id.as_str(), app)
        .await;

    if page_blob_storage.is_none() {
        return Err(RestoreSubPageError::NotFound);
    }

    let compressed_payload = page_blob_storage
        .unwrap()
        .read_sub_page_payload(sub_page_id)
        .await?;

    if compressed_payload.is_none() {
        return Err(RestoreSubPageError::NotFound);
    }

    let compressed_payload = compressed_payload.unwrap();

    let result = SubPageInner::from_compressed_payload(sub_page_id, compressed_payload.as_slice())?;

    Ok(SubPage::restore_from_archive(result))
}

pub async fn save_sub_page(app: &AppContext, topic_data: &TopicData, sub_page: &SubPage) {
    let sub_page_id = sub_page.get_id();
    if let Some(zip_payload) = sub_page.to_compressed_payload().await {
        let storage = app
            .archive_storage_list
            .get_or_create(sub_page_id.into(), topic_data.topic_id.as_str(), app)
            .await;

        let mut sw = StopWatch::new();

        sw.start();

        storage
            .write_payload(sub_page_id, zip_payload.as_slice())
            .await;

        sw.pause();

        topic_data.metrics.update_last_saved_duration(sw.duration());

        topic_data
            .metrics
            .update_last_saved_moment(DateTimeAsMicroseconds::now());
    }
}

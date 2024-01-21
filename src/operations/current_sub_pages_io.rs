use std::{sync::Arc, time::Duration};

use my_azure_page_blob_ext::MyAzurePageBlobStorageWithRetries;
use my_azure_storage_sdk::{
    block_blob::BlockBlobApi,
    page_blob::{AzurePageBlobStorage, MyAzurePageBlobStorage},
    AzureStorageError,
};
use my_service_bus::shared::sub_page::SubPageId;

use crate::{app::AppContext, message_pages::SubPageInner};

#[derive(Debug)]
pub enum RestorePagesError {
    AzureStorageError(AzureStorageError),
    Other(String),
}

impl RestorePagesError {
    pub fn into_err<TOk>(self) -> Result<TOk, Self> {
        Err(self)
    }
}

const CONTAINER_NAME: &str = "topics";
const BLOB_NAME: &str = ".active-pages";

#[allow(non_snake_case)]
#[derive(Clone, prost::Message)]
pub struct ActiveSubPageModel {
    #[prost(string, tag = "1")]
    pub topic_id: String,
    #[prost(int64, tag = "2")]
    pub sub_page_id: i64,
    #[prost(bytes, tag = "3")]
    pub payload: Vec<u8>,
}

#[derive(Clone, prost::Message)]
pub struct ActivePages {
    #[prost(message, repeated, tag = "1")]
    pub sub_pages: Vec<ActiveSubPageModel>,
}

pub async fn restore(
    app: &AppContext,
) -> Result<Option<Vec<(String, SubPageInner)>>, RestorePagesError> {
    let connection = app.get_storage_for_active_pages();

    let page_blob = AzurePageBlobStorage::new(connection, CONTAINER_NAME, BLOB_NAME).await;

    let page_blob = MyAzurePageBlobStorageWithRetries::new(page_blob, 3, Duration::from_secs(3));

    let page_blob = Arc::new(page_blob);

    let data = page_blob.download().await;

    match data {
        Ok(data) => {
            let result: Result<ActivePages, _> = prost::Message::decode(data.as_slice());

            match result {
                Ok(active_pages_contract) => {
                    let mut result = Vec::new();

                    for sub_page in active_pages_contract.sub_pages {
                        let sub_page_inner_result = SubPageInner::from_compressed_payload(
                            SubPageId::new(sub_page.sub_page_id),
                            sub_page.payload.as_slice(),
                        );

                        match sub_page_inner_result {
                            Ok(sub_page_inner) => {
                                result.push((sub_page.topic_id, sub_page_inner));
                            }
                            Err(err) => {
                                return RestorePagesError::Other(format!(
                                    "Can not decode active sub pages data. Err: {:?}",
                                    err
                                ))
                                .into_err()
                            }
                        }
                    }

                    let page_blob = page_blob.clone();

                    tokio::spawn(async move {
                        let _ = page_blob.delete().await;
                    });

                    return Ok(Some(result));
                }
                Err(err) => {
                    return RestorePagesError::Other(format!(
                        "Can not decode active sub pages data. Err: {:?}",
                        err
                    ))
                    .into_err()
                }
            }
        }
        Err(err) => {
            if let AzureStorageError::BlobNotFound = err {
                println!("Blob with active pages not found. Creating empty active pages");
                return Ok(None);
            }

            return Err(RestorePagesError::AzureStorageError(err));
        }
    }
}

pub async fn write(app: &AppContext) {
    let topics = app.topics_list.get_all().await;

    let mut result: ActivePages = ActivePages {
        sub_pages: Vec::new(),
    };

    for topic in topics {
        let sub_page = topic.pages_list.get_active_sub_page().await;

        if let Some(sub_page) = sub_page {
            let payload = sub_page.to_compressed_payload().await;

            if let Some(payload) = payload {
                result.sub_pages.push(ActiveSubPageModel {
                    topic_id: topic.topic_id.clone(),
                    sub_page_id: sub_page.get_id().get_value(),
                    payload,
                });
            }
        }
    }

    let mut content_to_upload = Vec::new();
    prost::Message::encode(&result, &mut content_to_upload).unwrap();

    let conn_string = app.get_storage_for_active_pages();

    loop {
        let result = conn_string
            .upload_block_blob("topics", ".active-pages", content_to_upload.clone())
            .await;

        if result.is_ok() {
            break;
        }

        println!("Can not write active pages: {:?}", result.unwrap_err());

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

use std::sync::Arc;

use my_azure_page_blob::MyAzurePageBlob;
use my_azure_page_blob_append::PageBlobAppendError;
use my_azure_storage_sdk::AzureConnection;
use my_service_bus_shared::{
    date_time::DateTimeAsMicroseconds, protobuf_models::MessageProtobufModel,
};

use crate::{
    app::{AppContext, AppError},
    azure_storage::consts::generate_blob_name,
    message_pages::MessagePageId,
    operations::MessagesStream,
};

const BLOB_AUTO_RESSIZE_IN_PAGES: usize = 16384;

pub struct MessagesPageBlob {
    pub topic_id: String,
    pub page_id: MessagePageId,
    pub messages_stream: MessagesStream<MyAzurePageBlob>,
    pub app: Arc<AppContext>,
}

impl MessagesPageBlob {
    pub fn new(topic_id: String, page_id: MessagePageId, app: Arc<AppContext>) -> Self {
        let connection =
            AzureConnection::from_conn_string(app.settings.messages_connection_string.as_str());
        let blob_name = generate_blob_name(&page_id);

        let blob = MyAzurePageBlob::new(connection, topic_id.clone(), blob_name);

        let messages_stream = MessagesStream::new(
            blob,
            app.settings.load_blob_pages_size,
            BLOB_AUTO_RESSIZE_IN_PAGES,
            1024 * 1024 * 5,
        );
        Self {
            topic_id,
            page_id,
            app,
            messages_stream: messages_stream,
        }
    }

    pub async fn load(
        &mut self,
        blob_is_current: bool,
    ) -> Result<Vec<MessageProtobufModel>, PageBlobAppendError> {
        let mut result = Vec::new();

        loop {
            let get_message_result = self.messages_stream.get_next_message().await;

            match get_message_result {
                Ok(message) => {
                    if message.is_none() {
                        return Ok(result);
                    }

                    result.push(message.unwrap());
                }
                Err(err) => {
                    self.handle_load_error(blob_is_current, err).await?;
                    return Ok(result);
                }
            }
        }
    }

    async fn handle_load_error(
        &mut self,
        blob_is_current: bool,
        err: PageBlobAppendError,
    ) -> Result<(), PageBlobAppendError> {
        match err {
            PageBlobAppendError::Corrupted(corrupted_reason) => {
                println!(
                    "Can not load from uncompressed page {}/{}. Blob is corrupted. We start writing at the position {}. Reason: {:?}",
                    self.topic_id, self.page_id.value,
                    corrupted_reason.broken_pos,
                    corrupted_reason.msg
                );

                let now = DateTimeAsMicroseconds::now();
                let conn_string = AzureConnection::from_conn_string(
                    self.app.settings.messages_connection_string.as_str(),
                );
                let mut backup_blob = MyAzurePageBlob::new(
                    conn_string,
                    self.topic_id.to_string(),
                    format!(
                        "{}.{}",
                        generate_blob_name(&self.page_id),
                        &now.to_rfc3339()[0..23]
                    ),
                );

                self.init(&mut backup_blob).await?;

                return Ok(());
            }
            PageBlobAppendError::NotInitialized => return Err(err),

            PageBlobAppendError::AzureStorageError(azure_error) => match azure_error {
                my_azure_storage_sdk::AzureStorageError::ContainerNotFound => {
                    if blob_is_current {
                        self.messages_stream.init_new_blob().await?;
                        println!(
                            "New blob is created for {}",
                            self.messages_stream.get_blob_formal_name()
                        );
                        return Ok(());
                    } else {
                        return Err(PageBlobAppendError::AzureStorageError(azure_error));
                    }
                }
                my_azure_storage_sdk::AzureStorageError::BlobNotFound => {
                    if blob_is_current {
                        self.messages_stream.init_new_blob().await?;
                        println!(
                            "New blob is created for {}",
                            self.messages_stream.get_blob_formal_name()
                        );
                        return Ok(());
                    } else {
                        return Err(PageBlobAppendError::AzureStorageError(azure_error));
                    }
                }
                _ => {
                    return Err(PageBlobAppendError::AzureStorageError(azure_error));
                }
            },
            _ => {
                println!("{:?}", err);
                return Err(err);
            }
        }
    }

    pub async fn save_messages(
        &mut self,
        messages: &[MessageProtobufModel],
    ) -> Result<(), AppError> {
        self.messages_stream.append(&messages).await?;
        Ok(())
    }

    pub fn get_write_position(&self) -> usize {
        self.messages_stream.get_write_position()
    }

    pub async fn init(
        &mut self,
        backup_blob: &mut MyAzurePageBlob,
    ) -> Result<(), PageBlobAppendError> {
        self.messages_stream.init(backup_blob).await
    }
}

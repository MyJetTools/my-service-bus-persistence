use std::sync::Arc;

use my_azure_page_blob::MyAzurePageBlob;
use my_azure_page_blob_append::PageBlobAppendError;
use my_azure_storage_sdk::AzureConnection;
use my_service_bus_shared::protobuf_models::MessageProtobufModel;

use crate::{
    app::{AppContext, AppError},
    azure_storage::consts::generage_blob_name,
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
        let blob_name = generage_blob_name(&page_id);

        let blob = MyAzurePageBlob::new(connection, topic_id.clone(), blob_name);

        let messages_stream = MessagesStream::new(
            blob,
            app.settings.load_blob_pages_size,
            BLOB_AUTO_RESSIZE_IN_PAGES,
            1024 * 1024 * 1024,
        );
        Self {
            topic_id,
            page_id,
            app,
            messages_stream: messages_stream,
        }
    }

    pub async fn load(&mut self) -> Result<Vec<MessageProtobufModel>, PageBlobAppendError> {
        let mut result = Vec::new();

        while let Some(message) = self.messages_stream.get_next_message().await? {
            result.push(message);
        }

        Ok(result)
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
}

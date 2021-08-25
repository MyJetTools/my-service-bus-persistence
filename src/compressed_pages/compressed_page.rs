use crate::{
    app::AppError,
    message_pages::{MessagesPage, MessagesPageData},
    messages_protobuf::MessagesProtobufModel,
};

pub struct CompressedPage {
    pub zip: Vec<u8>,
}

impl CompressedPage {
    pub async fn from_messages_page(page: &MessagesPage) -> Result<Self, AppError> {
        let data = page.dispose().await;

        match data {
            Some(data) => {
                let result = Self {
                    zip: to_zip(data).await?,
                };
                Ok(result)
            }
            None => {
                let err = AppError::Other {
                    msg: "Can not create compressed page. Page is already disposed".to_string(),
                };
                Err(err)
            }
        }
    }
}

pub async fn to_zip(data: MessagesPageData) -> Result<Vec<u8>, AppError> {
    let mut protobuf_model = MessagesProtobufModel {
        messages: Vec::new(),
    };

    for itm in data.messages {
        protobuf_model.messages.push(itm.1);
    }

    let payload = protobuf_model.serialize();

    let compressed = crate::compression::zip::compress_payload(&payload)?;

    Ok(compressed)
}

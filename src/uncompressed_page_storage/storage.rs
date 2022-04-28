use my_service_bus_shared::protobuf_models::MessageProtobufModel;

use super::{
    as_file::UncompressedPageStorageAsFile, load_trait::LoadFromStorage,
    upload_payload::PayloadsToUploadContainer, UncompressedStorageError,
};

pub enum UncompressedPageStorage {
    AsFile(UncompressedPageStorageAsFile),
}

impl UncompressedPageStorage {
    pub async fn open_or_append_as_file(path: &str) -> Result<Self, UncompressedStorageError> {
        let result = UncompressedPageStorageAsFile::opend_or_append(path).await?;
        Ok(Self::AsFile(result))
    }
    pub async fn init_and_load_messages(
        &mut self,
        max_message_size: usize,
    ) -> Result<Vec<MessageProtobufModel>, UncompressedStorageError> {
        match self {
            UncompressedPageStorage::AsFile(as_file) => {
                load_messages_from_storage(as_file, max_message_size).await
            }
        }
    }

    pub fn issue_payloads_to_upload_container(&self) -> PayloadsToUploadContainer {
        match self {
            UncompressedPageStorage::AsFile(as_file) => {
                PayloadsToUploadContainer::new(as_file.get_write_position())
            }
        }
    }

    pub async fn append_payload(
        &mut self,
        payload_to_upload: PayloadsToUploadContainer,
    ) -> Result<(), UncompressedStorageError> {
        todo!("Implement")
    }
}

async fn load_messages_from_storage<TLoadFromStorage: LoadFromStorage>(
    as_file: &mut TLoadFromStorage,
    max_message_size: usize,
) -> Result<Vec<MessageProtobufModel>, UncompressedStorageError> {
    let mut result = Vec::new();

    let mut pos = 0;

    loop {
        let mut len = [0u8; 4];

        if !as_file.read(len.as_mut()).await? {
            as_file.reset_file(pos).await?;
        }

        let len = u32::from_le_bytes(len) as usize;

        if len == 0 {
            return Ok(result);
        }

        if len > max_message_size {
            as_file.reset_file(pos).await?;
            return Err(UncompressedStorageError::Corrupted);
        }

        let mut payload = vec![0u8; len];

        if !as_file.read(payload.as_mut()).await? {
            as_file.reset_file(pos).await?;
        }

        let model = prost::Message::decode(payload.as_slice())?;

        result.push(model);

        pos += (len as u64) + 4;
    }
}

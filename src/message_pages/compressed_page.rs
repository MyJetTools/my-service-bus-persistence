use std::io::{Cursor, Read};

use my_service_bus_shared::{
    date_time::DateTimeAsMicroseconds, protobuf_models::MessageProtobufModel, MessageId,
};
use zip::result::ZipError;

use super::MessagePageId;

pub struct CompressedPage {
    pub zip_data: Vec<u8>,
    pub last_access: DateTimeAsMicroseconds,
    pub len: usize,
    pub max_message_id: Option<i64>,
    pub page_id: MessagePageId,
}

impl CompressedPage {
    pub fn new(page_id: MessagePageId, zip_data: Vec<u8>) -> Option<Self> {
        let max_message_id = get_max_msg_id(page_id, zip_data.as_slice())?;

        let result = Self {
            zip_data,
            last_access: DateTimeAsMicroseconds::now(),
            len: max_message_id.0,
            page_id,
            max_message_id: max_message_id.1,
        };

        Some(result)
    }

    pub fn get(&self, message_id: MessageId) -> Result<Option<MessageProtobufModel>, ZipError> {
        let c = Cursor::new(self.zip_data.as_slice());
        let mut zip = zip::ZipArchive::new(c).unwrap();

        let mut unzipped: Vec<u8> = Vec::new();

        let mut zip_file = zip.by_name(format!("{}", message_id).as_str());

        if zip_file.is_err() {
            return Ok(None);
        }

        let zip_file = zip_file.as_mut().unwrap();

        let mut buffer = [0u8; 1024 * 1024];

        loop {
            let read_size = zip_file.read(&mut buffer[..])?;
            if read_size == 0 {
                break;
            }

            unzipped.extend(&buffer[..read_size]);
        }

        let result = prost::Message::decode(unzipped.as_slice()).unwrap();

        Ok(Some(result))
    }
}

fn get_max_msg_id(page_id: MessagePageId, zip_data: &[u8]) -> Option<(usize, Option<i64>)> {
    let c = Cursor::new(zip_data);
    let zip = zip::ZipArchive::new(c);

    if let Err(err) = zip {
        println!(
            "Can not open zip for page_id {}. Err: {:?}",
            page_id.value, err
        );
        return None;
    }

    let mut zip = zip.unwrap();
    let amount = zip.len();

    let mut result = None;

    for file_number in 0..amount {
        let file = zip.by_index(file_number);

        if file.is_err() {
            return None;
        }

        let file = file.unwrap();

        let id = file.name().parse::<MessageId>();

        if let Ok(id) = id {
            match result {
                Some(max_message_id) => {
                    if id > max_message_id {
                        result = Some(id)
                    }
                }
                None => result = Some(id),
            }
        }
    }

    Some((amount, result))
}

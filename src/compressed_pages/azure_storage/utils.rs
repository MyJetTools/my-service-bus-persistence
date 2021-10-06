use std::io::Cursor;

use my_service_bus_shared::protobuf_models::{MessageProtobufModel, MessagesProtobufModel};
use zip::result::ZipError;

use super::super::ReadCompressedPageError;

pub fn decompress_v1(
    payload: &[u8],
) -> Result<Option<Vec<MessageProtobufModel>>, ReadCompressedPageError> {
    let unzipped = my_service_bus_shared::page_compressor::zip::decompress_payload(payload)?;
    let protobuf_messages = MessagesProtobufModel::parse(unzipped.as_slice())?;
    Ok(Some(protobuf_messages.messages))
}

pub fn check_zip_v2(payload: &[u8]) -> Result<bool, ZipError> {
    let c = Cursor::new(payload.to_vec());

    let mut zip = zip::ZipArchive::new(c)?;

    for i in 0..zip.len() {
        if i > 10 {
            break;
        }
        let zip_file = zip.by_index(i)?;

        let parse_result = zip_file.name().parse::<i64>();

        if parse_result.is_err() {
            return Ok(false);
        }
    }

    Ok(true)
}

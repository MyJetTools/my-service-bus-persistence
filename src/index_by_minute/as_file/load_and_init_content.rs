use crate::{file_random_access::FileRandomAccess, index_by_minute::utils::MINUTE_INDEX_FILE_SIZE};

pub async fn load_and_init_content(as_file: &mut FileRandomAccess) -> Vec<u8> {
    let file_size = as_file.get_file_size().await.unwrap();

    if file_size < MINUTE_INDEX_FILE_SIZE {
        if file_size > 0 {
            as_file.reduce_size(0).await.unwrap();
        }

        let index_content = vec![0u8; MINUTE_INDEX_FILE_SIZE];

        as_file.write_to_file(&index_content).await.unwrap();

        return index_content;
    }

    let mut result = Vec::with_capacity(MINUTE_INDEX_FILE_SIZE);

    unsafe {
        result.set_len(MINUTE_INDEX_FILE_SIZE);
    }

    as_file.read_from_file(&mut result).await.unwrap();

    result
}

use std::{path::PathBuf, sync::Arc};

use my_logger::LogEventCtx;
use rust_extensions::{MyTimerTick, RepeatTimerIteration};

use crate::{
    app::{storage_layout, AppContext},
    archive_storage::ArchiveFileNo,
    file_storage::{delete_file_if_exists, FileStorage},
    topic_key::{TopicKey, TopicKeyRef},
};

/// Moves sealed files to the cold tier.
///
/// Per topic, per tick: whichever archive carries the highest number is the one being written, so
/// it stays; **every** other archive is sealed and goes up - not just the one below the current,
/// since a backlog of two or three is normal after the cold tier was unreachable, after a restart
/// before the upload ran, or on a topic busy enough to roll through several files quickly. Year
/// indexes follow the same rule: the highest year is live, every earlier one is sealed.
///
/// Nothing is persisted and nothing needs to be. "The highest number on disk is the current one"
/// stays true by itself: a rollover turns the previous current into a sealed file that the next
/// tick picks up, and after a restart the same listing yields the same answer.
pub struct ColdStorageUploaderTimer {
    app: Arc<AppContext>,
}

impl ColdStorageUploaderTimer {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

#[async_trait::async_trait]
impl MyTimerTick for ColdStorageUploaderTimer {
    async fn tick(&self) -> RepeatTimerIteration {
        if self.app.get_cold_storage().is_none() {
            return RepeatTimerIteration::WithInterval;
        }

        for topic_folder in get_topic_folders(self.app.get_data_folder()).await {
            upload_sealed_files(self.app.as_ref(), &topic_folder).await;
        }

        RepeatTimerIteration::WithInterval
    }
}

struct TopicFolder {
    topic_key: TopicKey,
    path: PathBuf,
}

impl TopicFolder {
    fn get_topic_key(&self) -> TopicKeyRef<'_> {
        self.topic_key.to_ref()
    }
}

async fn get_topic_folders(data_folder: &str) -> Vec<TopicFolder> {
    crate::operations::scan_topic_folders(data_folder)
        .await
        .into_iter()
        .map(|topic_key| {
            let path = storage_layout::get_topic_folder(data_folder, topic_key.to_ref());
            TopicFolder { topic_key, path }
        })
        .collect()
}

async fn upload_sealed_files(app: &AppContext, topic_folder: &TopicFolder) {
    let mut archives: Vec<(i64, String)> = Vec::new();
    let mut year_indexes: Vec<(u32, String)> = Vec::new();

    let Ok(mut entries) = tokio::fs::read_dir(topic_folder.path.as_path()).await else {
        return;
    };

    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let Some(file_name) = path.file_name().and_then(|itm| itm.to_str()) else {
            continue;
        };

        if let Some(archive_file_no) = storage_layout::parse_archive_file_name(file_name) {
            archives.push((archive_file_no.get_value(), file_name.to_string()));
            continue;
        }

        if let Some(year) = storage_layout::parse_year_index_file_name(file_name) {
            year_indexes.push((year.get_value(), file_name.to_string()));
        }
    }

    for (archive_file_no, file_name) in take_all_but_the_highest(archives) {
        if upload_and_drop(app, topic_folder, file_name.as_str()).await {
            // The cached handle points at a file that is gone now - the next read has to reopen
            // it against the cold tier. Only this one: the handle of the archive still being
            // written must survive, see `ArchiveStorageList::forget_archive`.
            app.archive_storage_list.forget_archive(
                topic_folder.get_topic_key(),
                ArchiveFileNo::new(archive_file_no),
            );
        }
    }

    for (_, file_name) in take_all_but_the_highest(year_indexes) {
        upload_and_drop(app, topic_folder, file_name.as_str()).await;
    }
}

/// The highest-numbered file is the live one; everything below it is sealed.
fn take_all_but_the_highest<TOrder: Ord>(
    mut items: Vec<(TOrder, String)>,
) -> Vec<(TOrder, String)> {
    if items.len() < 2 {
        return Vec::new();
    }

    items.sort_by(|left, right| left.0.cmp(&right.0));
    items.pop();

    items
}

/// Upload, then delete locally - and only in that order, so a crash in between costs a repeated
/// upload rather than the file. Uploading the same file twice is idempotent.
async fn upload_and_drop(app: &AppContext, topic_folder: &TopicFolder, file_name: &str) -> bool {
    let Some(cold_storage) = app.get_cold_storage() else {
        return false;
    };

    let key = storage_layout::get_relative_path(topic_folder.get_topic_key(), file_name);

    let mut path = topic_folder.path.clone();
    path.push(file_name);

    let file = match FileStorage::open_if_exists(&path).await {
        Ok(Some(file)) => file,
        Ok(None) => return false,
        Err(err) => {
            write_error(&key, format!("Can not open the file. Err: {}", err));
            return false;
        }
    };

    let content = match file.read_all().await {
        Ok(content) => content,
        Err(err) => {
            write_error(&key, format!("Can not read the file. Err: {}", err));
            return false;
        }
    };

    if let Err(err) = cold_storage.upload(key.as_str(), content).await {
        write_error(&key, format!("Can not upload. Err: {}", err));
        return false;
    }

    drop(file);

    if let Err(err) = delete_file_if_exists(&path).await {
        write_error(
            &key,
            format!("Uploaded, but can not delete the local copy. Err: {}", err),
        );
        return false;
    }

    println!("Moved {} to the cold storage", key);

    true
}

fn write_error(key: &str, message: String) {
    my_logger::LOGGER.write_error(
        "ColdStorageUploader",
        message,
        LogEventCtx::new().add("key", key.to_string()),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_highest_one_stays() {
        let items = vec![
            (2i64, "2".to_string()),
            (10, "10".to_string()),
            (1, "1".to_string()),
        ];

        assert_eq!(
            vec![(1i64, "1".to_string()), (2, "2".to_string())],
            take_all_but_the_highest(items)
        );
    }

    /// A backlog of several sealed files all goes, not only the one below the current.
    #[test]
    fn a_backlog_goes_in_full() {
        let items = vec![
            (1i64, "1".to_string()),
            (2, "2".to_string()),
            (3, "3".to_string()),
            (4, "4".to_string()),
        ];

        assert_eq!(
            vec![
                (1i64, "1".to_string()),
                (2, "2".to_string()),
                (3, "3".to_string())
            ],
            take_all_but_the_highest(items)
        );
    }

    #[test]
    fn a_lone_file_is_the_current_one_and_stays() {
        let items = vec![(7i64, "7".to_string())];
        assert!(take_all_but_the_highest(items).is_empty());

        let items: Vec<(i64, String)> = Vec::new();
        assert!(take_all_but_the_highest(items).is_empty());
    }
}

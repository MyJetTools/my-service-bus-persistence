use std::sync::Arc;

use rust_extensions::AppStates;

use crate::{
    archive_storage::{ArchiveFileNo, ArchiveFileOpener, ArchiveStorage, ArchiveStorageList},
    cold_storage::ColdStorage,
    file_storage::FileStorage,
    index_by_minute::{IndexByMinuteUtils, YearlyIndexByMinute},
    settings::SettingsModel,
    topic_data::TopicsDataList,
    topic_key::{TopicKeyRef, DEFAULT_NAMESPACE},
    topics_snapshot::current_snapshot::CurrentTopicsSnapshot,
    typing::Year,
};

use super::{storage_layout, PrometheusMetrics, StorageLocks};

pub const APP_VERSION: &'static str = env!("CARGO_PKG_VERSION");

pub struct AppContext {
    pub app_states: Arc<AppStates>,
    pub topics_snapshot: CurrentTopicsSnapshot,

    pub topics_list: TopicsDataList,
    pub settings: SettingsModel,

    pub metrics_keeper: PrometheusMetrics,
    pub index_by_minute_utils: IndexByMinuteUtils,

    pub archive_storage_list: ArchiveStorageList,

    /// Held while an archive is read or uploaded; taken exclusively to delete it. See
    /// [`StorageLocks`] - this is what keeps the background archiver from pulling a file out from
    /// under a reader.
    pub archive_locks: StorageLocks,
    /// The same, for the per-year minute index.
    pub index_locks: StorageLocks,

    /// `None` when no S3 section is configured - then nothing is ever uploaded and every file
    /// stays local forever.
    cold_storage: Option<Arc<ColdStorage>>,
}

impl AppContext {
    pub async fn new(settings: SettingsModel) -> AppContext {
        let cold_storage = settings
            .get_s3_connection()
            .map(|s3| Arc::new(ColdStorage::new(&s3)));

        // Doubles as the connectivity check: a wrong endpoint, region or key pair fails here
        // rather than silently at the first upload hours later. Other namespaces get their bucket
        // on first touch.
        if let Some(cold_storage) = cold_storage.as_ref() {
            cold_storage
                .ensure_bucket(DEFAULT_NAMESPACE)
                .await
                .unwrap_or_else(|err| panic!("Can not reach the cold storage: {}", err));
        }

        let topics_snapshot = CurrentTopicsSnapshot::read_or_create(settings.data.clone()).await;

        AppContext {
            topics_snapshot,
            topics_list: TopicsDataList::new(),
            settings,

            metrics_keeper: PrometheusMetrics::new(),
            index_by_minute_utils: IndexByMinuteUtils::new(),
            app_states: Arc::new(AppStates::create_un_initialized()),
            archive_storage_list: ArchiveStorageList::new(),
            archive_locks: StorageLocks::new(),
            index_locks: StorageLocks::new(),
            cold_storage,
        }
    }

    pub fn get_env_info(&self) -> String {
        let env_info = std::env::var("ENV_INFO");

        match env_info {
            Ok(info) => info,
            Err(err) => format!("{:?}", err),
        }
    }

    pub fn get_data_folder(&self) -> &str {
        self.settings.data.as_str()
    }

    pub fn get_cold_storage(&self) -> Option<&Arc<ColdStorage>> {
        self.cold_storage.as_ref()
    }

    pub async fn create_topic_folder(&self, topic_key: TopicKeyRef<'_>) {
        let folder = storage_layout::get_topic_folder(self.get_data_folder(), topic_key);

        tokio::fs::create_dir_all(folder.as_path())
            .await
            .unwrap_or_else(|err| {
                panic!("Can not create the folder of topic {}: {}", topic_key, err)
            });
    }

    pub async fn open_or_create_index_by_minute(
        &self,
        topic_key: TopicKeyRef<'_>,
        year: Year,
    ) -> YearlyIndexByMinute {
        // A closed year may have been uploaded and dropped locally; pull it back so a late write
        // lands on the real index rather than on an empty one.
        self.restore_year_index_from_cold_storage(topic_key, year)
            .await;

        YearlyIndexByMinute::open_or_create(self.get_year_index_path(topic_key, year)).await
    }

    pub async fn try_open_index_by_minute(
        &self,
        topic_key: TopicKeyRef<'_>,
        year: Year,
    ) -> Option<Arc<YearlyIndexByMinute>> {
        let path = self.get_year_index_path(topic_key, year);

        if let Some(result) = YearlyIndexByMinute::load_if_exists(&path).await {
            return Some(Arc::new(result));
        }

        self.restore_year_index_from_cold_storage(topic_key, year)
            .await;

        let result = YearlyIndexByMinute::load_if_exists(&path).await?;

        Some(Arc::new(result))
    }

    fn get_year_index_path(&self, topic_key: TopicKeyRef<'_>, year: Year) -> std::path::PathBuf {
        storage_layout::get_local_path(
            self.get_data_folder(),
            storage_layout::get_year_index_relative_path(topic_key, year).as_str(),
        )
    }

    /// Materializes a cold year index back onto the local disk.
    ///
    /// The index is 4 MB and addressed by offset, so instead of reading it over ranged GETs we
    /// bring the whole file back. That also gives the read-modify-write story for free: a late
    /// write for a closed year updates the local copy, and the uploader sends it back up.
    async fn restore_year_index_from_cold_storage(&self, topic_key: TopicKeyRef<'_>, year: Year) {
        let Some(cold_storage) = self.get_cold_storage() else {
            return;
        };

        let path = self.get_year_index_path(topic_key, year);

        if path.exists() {
            return;
        }

        let key = storage_layout::get_year_index_cold_key(topic_key, year);

        let content = cold_storage
            .download(topic_key.namespace, key.as_str())
            .await
            .unwrap_or_else(|err| panic!("Can not read {} from the cold storage: {}", key, err));

        let Some(content) = content else {
            return;
        };

        println!("Restoring {} from the cold storage", key);

        let file = FileStorage::open_or_create(&path)
            .await
            .expect("Can not create the year index file");

        file.write_all(content.as_slice())
            .await
            .expect("Can not write the restored year index");
    }
}

#[async_trait::async_trait]
impl ArchiveFileOpener for AppContext {
    async fn open_or_create_archive(
        &self,
        topic_key: TopicKeyRef<'_>,
        archive_file_no: ArchiveFileNo,
    ) -> ArchiveStorage {
        let relative_path = storage_layout::get_archive_relative_path(topic_key, archive_file_no);
        let path = storage_layout::get_local_path(self.get_data_folder(), relative_path.as_str());

        ArchiveStorage::open_or_create_local(archive_file_no, path)
            .await
            .unwrap_or_else(|err| panic!("Can not open the archive {}: {:?}", relative_path, err))
    }

    async fn try_open_archive(
        &self,
        topic_key: TopicKeyRef<'_>,
        archive_file_no: ArchiveFileNo,
    ) -> Option<ArchiveStorage> {
        let relative_path = storage_layout::get_archive_relative_path(topic_key, archive_file_no);
        let path = storage_layout::get_local_path(self.get_data_folder(), relative_path.as_str());

        let local = ArchiveStorage::open_local_if_exists(archive_file_no, path)
            .await
            .unwrap_or_else(|err| panic!("Can not open the archive {}: {:?}", relative_path, err));

        if local.is_some() {
            return local;
        }

        // Not on disk - it may have been sealed and uploaded. Reads then go over ranged GETs.
        let cold_storage = self.get_cold_storage()?;

        let cold_key = storage_layout::get_archive_cold_key(topic_key, archive_file_no);

        let exists = cold_storage
            .exists(topic_key.namespace, cold_key.as_str())
            .await
            .unwrap_or_else(|err| {
                panic!("Can not check {} in the cold storage: {}", cold_key, err)
            });

        if !exists {
            return None;
        }

        Some(ArchiveStorage::open_cold(
            archive_file_no,
            cold_storage.clone(),
            topic_key.namespace.to_string(),
            cold_key,
        ))
    }
}

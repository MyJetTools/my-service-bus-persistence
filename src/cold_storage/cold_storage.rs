use std::{path::Path, time::Duration};

use ahash::AHashSet;
use my_s3::S3Client;
use parking_lot::Mutex;
use tokio::io::AsyncReadExt;

use crate::{
    settings::{S3BucketMode, S3ConnectionSettings},
    topic_key::TopicKeyRef,
};

/// Read from the file and handed to the request one chunk at a time, so peak memory is a chunk
/// rather than the object. An archive is hundreds of megabytes; reading one whole was an OOM kill
/// in a 512 MB container - and an OOM arrives as SIGKILL, so it left no panic and no log line.
///
/// An upload happens only when an archive seals, so this is a rare burst rather than a hot path -
/// worth a comfortable chunk. Anything past a megabyte or so starts trading the point away again:
/// memory in flight is the chunk times the channel depth.
const UPLOAD_CHUNK_SIZE: usize = 512 * 1024;

/// How many chunks may sit between the reader and the socket. Four is ~2 MB in flight.
const UPLOAD_CHANNEL_SIZE: usize = 4;

/// Generous: it covers pushing the whole body out, not just waiting for the answer.
const UPLOAD_TIMEOUT: Duration = Duration::from_secs(600);

/// A streamed upload is sent exactly once, so retrying is ours to do - and it is safe, because
/// `PutObject` replaces the object atomically.
const UPLOAD_RETRIES: usize = 3;

/// The cold tier: sealed archives and closed year indexes, uploaded once and read back over ranged
/// GETs. Nothing here is ever modified in place - S3 objects can only be replaced whole, which is
/// exactly why only sealed files get here.
///
/// Two layouts, chosen by the connection string and never guessed - see [`S3BucketMode`]:
///
/// ```text
/// Bucket=sb-data         /sb-data/{namespace}/{topic}/{file}
/// BucketPrefix=sb-data   /sb-data-{namespace}/{topic}/{file}
/// ```
///
/// Both spell the same thing; they differ only in where the namespace sits - inside the key, or in
/// the bucket name. Callers never see the difference: they hand over a topic key and a file name,
/// and this is the only place that knows which layout is in force.
pub struct ColdStorage {
    client: S3Client,
    bucket_mode: S3BucketMode,
    /// Bucket names this process has already created-or-confirmed. Keyed by the bucket rather than
    /// by the namespace, so the shared layout naturally ensures once for everything.
    ensured: Mutex<AHashSet<String>>,
}

impl ColdStorage {
    pub fn new(settings: &S3ConnectionSettings) -> Self {
        // The region argument is `impl Into<S3Region>`: `S3Region` knows the AWS and Hetzner
        // regions by name and keeps anything else as `Other`, so an unfamiliar endpoint still
        // signs correctly.
        let client = S3Client::new(
            settings.access_key.clone(),
            settings.secret_key.clone(),
            settings.region.clone(),
            settings.endpoint.clone(),
        );

        // `Debug=1` in the connection string. Every request is then traced to stdout - verb, url
        // and body size going out, and the whole answer body when it failed, which is the
        // `<Error><Code>` saying why. A successful answer is printed as a size, since it is the
        // archive that was just downloaded. The `Authorization` header is never printed.
        let client = if settings.debug {
            println!("S3 request tracing is ON (Debug in s3_conn_string)");
            client.debug_to_console()
        } else {
            client
        };

        Self {
            client,
            bucket_mode: settings.bucket_mode.clone(),
            ensured: Mutex::new(AHashSet::new()),
        }
    }

    pub fn get_bucket(&self, namespace: &str) -> String {
        match &self.bucket_mode {
            S3BucketMode::Shared(bucket) => bucket.clone(),
            S3BucketMode::PerNamespace(prefix) => format!("{}-{}", prefix, namespace),
        }
    }

    /// Where a file lives: which bucket, and under which key inside it.
    fn resolve(&self, topic_key: TopicKeyRef<'_>, file_name: &str) -> (String, String) {
        match &self.bucket_mode {
            S3BucketMode::Shared(bucket) => (
                bucket.clone(),
                format!(
                    "{}/{}/{}",
                    topic_key.namespace, topic_key.topic_id, file_name
                ),
            ),
            S3BucketMode::PerNamespace(prefix) => (
                format!("{}-{}", prefix, topic_key.namespace),
                format!("{}/{}", topic_key.topic_id, file_name),
            ),
        }
    }

    /// Creates the bucket unless this process already did. **Best effort - it never fails the
    /// caller.**
    ///
    /// Every operation goes through it, so a namespace that appears at runtime gets its bucket on
    /// first touch. After the first attempt it is a set lookup.
    ///
    /// Not being able to *create* a bucket says very little about being able to *use* it, which is
    /// why a failure here is reported and stepped over rather than raised. An access key scoped to
    /// one bucket is routinely denied `CreateBucket` while reading and writing inside that bucket
    /// perfectly well, and a bucket made by hand ahead of time is the normal case in a managed
    /// deployment. So the operation goes ahead: if the bucket really is unusable, the upload or the
    /// read says so on its own terms, about the file it was actually working on.
    pub async fn ensure_bucket(&self, namespace: &str) {
        let bucket = self.get_bucket(namespace);

        if self.ensured.lock().contains(bucket.as_str()) {
            return;
        }

        if let Err(err) = validate_bucket_name(bucket.as_str()) {
            // Nothing will work with this name, but that is the operator's to fix, and shouting
            // about it on every upload would bury it.
            self.report_bucket_problem(bucket, "use", err.as_str(), false);
            return;
        }

        // In the single-bucket layout the name is a fixed one out of the connection string: it is
        // made once and then used forever, so the question worth asking is whether it is *there*,
        // not whether we can make it. `HEAD /{bucket}` answers that in one round trip, and it is
        // the question a scoped key can actually answer - such a key is routinely allowed to use
        // its one bucket while being denied `CreateBucket`, which would otherwise log a permission
        // error on every start of a perfectly healthy deployment.
        //
        // The per-namespace layout is the opposite case: a bucket genuinely appears at runtime,
        // when a namespace is first written to, so there creating it is the whole point.
        if matches!(self.bucket_mode, S3BucketMode::Shared(_)) {
            match self.client.check_if_bucket_exists(bucket.as_str()).await {
                Ok(true) => {
                    println!("Cold storage bucket '{}' is there", bucket);
                    self.ensured.lock().insert(bucket);
                    return;
                }

                // Not there at all - a first start against empty storage. Fall through and make it.
                Ok(false) => {}

                // A `HEAD` has no body, so there is no `<Error><Code>` to go on: a 403 here means
                // either the name is somebody else's or these credentials are wrong, and both are
                // worth saying out loud rather than papering over with a `CreateBucket` that would
                // fail differently.
                Err(err) => {
                    self.report_bucket_problem(
                        bucket,
                        "check",
                        format!("{:?}", err).as_str(),
                        err.is_retryable(),
                    );
                    return;
                }
            }
        }

        // `create_bucket_if_not_exists` absorbs `BucketAlreadyOwnedByYou` - the answer to every
        // restart after the first one, and to a bucket created by hand ahead of time.
        let err = match self
            .client
            .create_bucket_if_not_exists(bucket.as_str())
            .await
        {
            Ok(_) => {
                println!("Cold storage bucket '{}' is ready", bucket);
                self.ensured.lock().insert(bucket);
                return;
            }
            Err(err) => err,
        };

        // `BucketAlreadyExists` wears similar words but means the opposite: the name is held by
        // *another account*. A bucket name is unique across every customer of the provider, so it
        // points at the name in `s3_conn_string` rather than at anything transient.
        let message = if err.is_bucket_already_exists() {
            format!(
                "the name belongs to another account - bucket names are unique across every customer of the provider, so check the one in s3_conn_string ({:?})",
                err
            )
        } else {
            format!("{:?}", err)
        };

        // A transient failure is worth another go on the next operation; a deterministic one -
        // denied permission, a name that is somebody else's - would only repeat itself, and
        // retrying it on every single upload turns one problem into a flood of requests.
        self.report_bucket_problem(bucket, "create", message.as_str(), err.is_retryable());
    }

    fn report_bucket_problem(
        &self,
        bucket: String,
        what_failed: &str,
        message: &str,
        retry_later: bool,
    ) {
        let tail = if retry_later {
            "Going on without it - it will be tried again on the next operation."
        } else {
            "Going on without it - the reason is not one a retry would change, so it will not be tried again until a restart."
        };

        my_logger::LOGGER.write_error(
            "ColdStorage::ensure_bucket",
            format!(
                "Can not {} the cold storage bucket '{}': {}. {}",
                what_failed, bucket, message, tail
            ),
            my_logger::LogEventCtx::new().add("bucket", bucket.as_str()),
        );

        if !retry_later {
            self.ensured.lock().insert(bucket);
        }
    }

    /// Streams a file up, one chunk at a time - the whole point being that memory does not depend
    /// on the size of the object.
    ///
    /// Each retry reopens the file from the beginning: a streamed body is consumed as it is sent,
    /// so a half-drained reader can not be reused. `PutObject` replaces the object atomically, so a
    /// failed attempt leaves either the previous object or nothing, never a partial one.
    pub async fn upload_file(
        &self,
        topic_key: TopicKeyRef<'_>,
        file_name: &str,
        path: &Path,
    ) -> Result<(), String> {
        self.ensure_bucket(topic_key.namespace).await;

        let (bucket, key) = self.resolve(topic_key, file_name);

        let content_length = tokio::fs::metadata(path)
            .await
            .map_err(|err| format!("Can not size {:?}: {}", path, err))?
            .len() as usize;

        let path = path.to_path_buf();

        self.client
            .upload_streamed_with_retries(
                bucket.as_str(),
                key.as_str(),
                content_length,
                UPLOAD_TIMEOUT,
                UPLOAD_RETRIES,
                || {
                    let (sender, receiver) = tokio::sync::mpsc::channel(UPLOAD_CHANNEL_SIZE);
                    let path = path.clone();

                    tokio::spawn(async move {
                        let Ok(mut file) = tokio::fs::File::open(path.as_path()).await else {
                            return;
                        };

                        let mut buffer = vec![0u8; UPLOAD_CHUNK_SIZE];

                        while let Ok(read) = file.read(&mut buffer).await {
                            if read == 0 {
                                break;
                            }

                            if sender.send(buffer[..read].to_vec()).await.is_err() {
                                break;
                            }
                        }
                    });

                    receiver
                },
            )
            .await
            .map_err(|err| format!("{:?}", err))
    }

    /// `from`/`to` are inclusive byte offsets, as in the HTTP `Range` header.
    pub async fn download_range(
        &self,
        topic_key: TopicKeyRef<'_>,
        file_name: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<u8>, String> {
        self.ensure_bucket(topic_key.namespace).await;

        let (bucket, key) = self.resolve(topic_key, file_name);

        self.client
            .download_file_range(bucket.as_str(), key.as_str(), from, Some(to))
            .await
            .map_err(|err| format!("{:?}", err))
    }

    pub async fn download(
        &self,
        topic_key: TopicKeyRef<'_>,
        file_name: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        self.ensure_bucket(topic_key.namespace).await;

        let (bucket, key) = self.resolve(topic_key, file_name);

        match self
            .client
            .download_file(bucket.as_str(), key.as_str())
            .await
        {
            Ok(content) => Ok(Some(content)),
            Err(err) => {
                if err.is_key_not_found() {
                    return Ok(None);
                }

                Err(format!("{:?}", err))
            }
        }
    }

    /// Cheapest existence probe the client can express: ask for a single byte and see whether the
    /// object answers.
    pub async fn exists(
        &self,
        topic_key: TopicKeyRef<'_>,
        file_name: &str,
    ) -> Result<bool, String> {
        self.ensure_bucket(topic_key.namespace).await;

        let (bucket, key) = self.resolve(topic_key, file_name);

        match self
            .client
            .download_file_range(bucket.as_str(), key.as_str(), 0, Some(0))
            .await
        {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.is_key_not_found() {
                    return Ok(false);
                }

                Err(format!("{:?}", err))
            }
        }
    }

    pub async fn delete(&self, topic_key: TopicKeyRef<'_>, file_name: &str) -> Result<(), String> {
        self.ensure_bucket(topic_key.namespace).await;

        let (bucket, key) = self.resolve(topic_key, file_name);

        match self.client.delete_file(bucket.as_str(), key.as_str()).await {
            Ok(_) => Ok(()),
            Err(err) => {
                if err.is_key_not_found() {
                    return Ok(());
                }

                Err(format!("{:?}", err))
            }
        }
    }
}

/// S3 bucket naming, the subset we can produce: 3-63 chars, lowercase letters, digits and hyphens,
/// starting and ending on a letter or a digit.
///
/// Reachable rather than theoretical in the per-namespace layout: a namespace is `[a-z0-9-]` and
/// may **end** with a hyphen, which a bucket may not.
fn validate_bucket_name(bucket: &str) -> Result<(), String> {
    let invalid = |reason: &str| {
        Err(format!(
            "'{}' is not a valid bucket name: {}",
            bucket, reason
        ))
    };

    if bucket.len() < 3 || bucket.len() > 63 {
        return invalid("it must be 3 to 63 chars long");
    }

    for value in bucket.chars() {
        let is_valid = value.is_ascii_lowercase() || value.is_ascii_digit() || value == '-';

        if !is_valid {
            return invalid("only lowercase letters, digits and hyphens are allowed");
        }
    }

    let starts_ok = bucket
        .chars()
        .next()
        .map(|itm| itm.is_ascii_alphanumeric())
        .unwrap_or(false);
    let ends_ok = bucket
        .chars()
        .next_back()
        .map(|itm| itm.is_ascii_alphanumeric())
        .unwrap_or(false);

    if !starts_ok || !ends_ok {
        return invalid("it must start and end with a letter or a digit");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cold_storage::fake_s3::FakeS3;
    use crate::settings::S3ConnectionSettings;

    fn orders(namespace: &str) -> TopicKeyRef<'_> {
        TopicKeyRef::new(namespace, "orders")
    }

    async fn connect_with(bucket_mode: S3BucketMode) -> (FakeS3, ColdStorage) {
        let fake = FakeS3::start().await;

        let cold_storage = ColdStorage::new(&S3ConnectionSettings {
            endpoint: fake.endpoint.clone(),
            region: "eu-central-1".to_string(),
            access_key: "AKIATEST".to_string(),
            secret_key: "secret".to_string(),
            bucket_mode,
            debug: false,
        });

        (fake, cold_storage)
    }

    async fn connect() -> (FakeS3, ColdStorage) {
        connect_with(S3BucketMode::PerNamespace("sb".to_string())).await
    }

    fn temp_file(name: &str, content: &[u8]) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("my-sb-persistence-upload-{}", name));
        std::fs::write(&path, content).unwrap();
        path
    }

    /// The whole round trip against a real socket: streamed upload, ranged read, whole read,
    /// exists, delete. This is the only place the `Range` header, 206, 204 and 404 are exercised.
    #[tokio::test]
    async fn upload_read_range_and_delete() {
        let (fake, cold_storage) = connect().await;

        let content: Vec<u8> = (0..=255u8).collect();
        let path = temp_file("round_trip", content.as_slice());
        let file_name = "0000000000000000000.archive";

        cold_storage
            .upload_file(orders("default"), file_name, path.as_path())
            .await
            .unwrap();

        assert_eq!(
            Some(content.clone()),
            cold_storage
                .download(orders("default"), file_name)
                .await
                .unwrap()
        );

        // Inclusive offsets, the way the archive TOC and a sub page are fetched
        let chunk = cold_storage
            .download_range(orders("default"), file_name, 10, 19)
            .await
            .unwrap();
        assert_eq!(content[10..=19].to_vec(), chunk);

        assert!(cold_storage
            .exists(orders("default"), file_name)
            .await
            .unwrap());
        assert!(!cold_storage
            .exists(orders("default"), "nope.archive")
            .await
            .unwrap());

        cold_storage
            .delete(orders("default"), file_name)
            .await
            .unwrap();
        assert_eq!(
            None,
            cold_storage
                .download(orders("default"), file_name)
                .await
                .unwrap()
        );
        // Deleting what is not there is fine
        cold_storage
            .delete(orders("default"), file_name)
            .await
            .unwrap();

        assert!(fake.object_paths().is_empty());

        let _ = std::fs::remove_file(&path);
    }

    /// The reason the streaming path exists: an object many chunks long has to arrive byte for
    /// byte, without the sender ever holding it whole.
    #[tokio::test]
    async fn a_multi_chunk_file_arrives_intact() {
        let (fake, cold_storage) = connect().await;

        // Several times UPLOAD_CHUNK_SIZE, with a pattern that would expose a lost or reordered
        // chunk rather than just a wrong length
        let content: Vec<u8> = (0..UPLOAD_CHUNK_SIZE * 3 + 7)
            .map(|itm| (itm % 251) as u8)
            .collect();

        let path = temp_file("multi_chunk", content.as_slice());
        let file_name = "0000000000000000001.archive";

        cold_storage
            .upload_file(orders("default"), file_name, path.as_path())
            .await
            .unwrap();

        assert_eq!(
            Some(content.clone()),
            cold_storage
                .download(orders("default"), file_name)
                .await
                .unwrap()
        );
        assert_eq!(
            content.len(),
            fake.get_object("/sb-default/orders/0000000000000000001.archive")
                .unwrap()
                .len()
        );

        let _ = std::fs::remove_file(&path);
    }

    /// `BucketPrefix=sb` - the namespace is the bucket, so the key starts at the topic.
    #[tokio::test]
    async fn the_per_namespace_layout_puts_the_namespace_in_the_bucket() {
        let (fake, cold_storage) = connect().await;

        let path = temp_file("per_ns", &[1, 2, 3]);

        cold_storage
            .upload_file(orders("alpha"), ".2025.yearindex", path.as_path())
            .await
            .unwrap();

        assert_eq!(
            vec!["/sb-alpha/orders/.2025.yearindex".to_string()],
            fake.object_paths()
        );

        // The same topic in another namespace is another bucket, so it is a different object
        assert_eq!(
            None,
            cold_storage
                .download(orders("default"), ".2025.yearindex")
                .await
                .unwrap()
        );

        let _ = std::fs::remove_file(&path);
    }

    /// `Bucket=sb-data` - one bucket for everything, the namespace being the first key segment.
    #[tokio::test]
    async fn the_shared_layout_puts_the_namespace_in_the_key() {
        let (fake, cold_storage) = connect_with(S3BucketMode::Shared("sb-data".to_string())).await;

        let path = temp_file("shared", &[1, 2, 3]);

        cold_storage
            .upload_file(orders("alpha"), ".2025.yearindex", path.as_path())
            .await
            .unwrap();
        cold_storage
            .upload_file(orders("default"), ".2025.yearindex", path.as_path())
            .await
            .unwrap();

        assert_eq!(
            vec![
                "/sb-data/alpha/orders/.2025.yearindex".to_string(),
                "/sb-data/default/orders/.2025.yearindex".to_string(),
            ],
            fake.object_paths()
        );

        // Still two different objects, told apart by the key rather than by the bucket
        assert_eq!(
            1,
            fake.requests()
                .iter()
                .filter(|itm| itm.as_str() == "PUT /sb-data")
                .count()
        );

        let _ = std::fs::remove_file(&path);
    }

    /// The bucket already exists because a previous run created it - or because an operator did,
    /// by hand, before the first start. A fresh process gets `BucketAlreadyOwnedByYou` on its very
    /// first call, which has to read as success.
    ///
    /// A second `ColdStorage` over the same server is what makes this a restart rather than a
    /// repeat: `ensure_bucket` short-circuits on its own set the second time round, so within one
    /// instance the 409 never comes back at all.
    #[tokio::test]
    async fn a_bucket_that_already_exists_is_accepted() {
        let (fake, first_run) = connect().await;

        first_run.ensure_bucket("default").await;

        let restarted = ColdStorage::new(&S3ConnectionSettings {
            endpoint: fake.endpoint.clone(),
            region: "eu-central-1".to_string(),
            access_key: "AKIATEST".to_string(),
            secret_key: "secret".to_string(),
            bucket_mode: S3BucketMode::PerNamespace("sb".to_string()),
            debug: false,
        });

        restarted.ensure_bucket("default").await;

        // ...and it is usable, not merely accepted
        let path = temp_file("after_restart", &[1, 2, 3]);
        restarted
            .upload_file(orders("default"), "active", path.as_path())
            .await
            .unwrap();

        assert_eq!(
            2,
            fake.requests()
                .iter()
                .filter(|itm| itm.as_str() == "PUT /sb-default")
                .count()
        );

        let _ = std::fs::remove_file(&path);
    }

    /// The other way a bucket can already exist: the name belongs to somebody else, because bucket
    /// names are unique across every customer of the provider.
    ///
    /// It is reported and stepped over, not raised - being unable to create a bucket says little
    /// about being able to use one. What must not happen is a retry: the answer is deterministic,
    /// so asking again on every upload would only multiply the requests.
    #[tokio::test]
    async fn a_bucket_owned_by_another_account_is_reported_and_stepped_over() {
        let (fake, cold_storage) = connect().await;

        fake.claim_bucket_for_another_account("sb-default");

        cold_storage.ensure_bucket("default").await;
        cold_storage.ensure_bucket("default").await;

        assert_eq!(
            1,
            fake.requests()
                .iter()
                .filter(|itm| itm.as_str() == "PUT /sb-default")
                .count(),
            "a deterministic failure must not be retried"
        );

        // And the work goes on: the upload is attempted, against the bucket that is there
        let path = temp_file("foreign_bucket", &[1, 2, 3]);

        cold_storage
            .upload_file(orders("default"), "active", path.as_path())
            .await
            .unwrap();

        assert_eq!(
            vec!["/sb-default/orders/active".to_string()],
            fake.object_paths()
        );

        let _ = std::fs::remove_file(&path);
    }

    /// The opposite half of the same rule: a 503 says nothing about the bucket, only about the
    /// moment, so the next operation tries again rather than writing the bucket off until restart.
    #[tokio::test]
    async fn a_transient_failure_is_tried_again() {
        let (fake, cold_storage) = connect().await;

        fake.fail_bucket_creation_times("sb-default", 1);

        cold_storage.ensure_bucket("default").await;
        cold_storage.ensure_bucket("default").await;

        assert_eq!(
            2,
            fake.requests()
                .iter()
                .filter(|itm| itm.as_str() == "PUT /sb-default")
                .count()
        );

        // The second attempt got through, so a third one is the set lookup again
        cold_storage.ensure_bucket("default").await;

        assert_eq!(
            2,
            fake.requests()
                .iter()
                .filter(|itm| itm.as_str() == "PUT /sb-default")
                .count()
        );
    }

    /// The single-bucket layout asks `HEAD /{bucket}` instead of trying to create what is already
    /// there. That is not a micro-optimisation: a key scoped to this one bucket is routinely
    /// allowed to use it and denied `CreateBucket`, so asking the other question would log a
    /// permission error on every start of a healthy deployment.
    #[tokio::test]
    async fn the_single_bucket_layout_checks_rather_than_creates() {
        let (fake, cold_storage) = connect_with(S3BucketMode::Shared("sb-data".to_string())).await;

        fake.create_bucket("sb-data");

        cold_storage.ensure_bucket("default").await;

        assert_eq!(vec!["HEAD /sb-data".to_string()], fake.requests());

        // Confirmed once, then it is a set lookup - including for another namespace, which shares
        // the bucket
        cold_storage.ensure_bucket("alpha").await;

        assert_eq!(1, fake.requests().len());
    }

    /// A first start against empty storage still has to make the bucket - the check is what comes
    /// first, not what replaces creating it.
    #[tokio::test]
    async fn the_single_bucket_layout_creates_a_bucket_that_is_missing() {
        let (fake, cold_storage) = connect_with(S3BucketMode::Shared("sb-data".to_string())).await;

        cold_storage.ensure_bucket("default").await;

        assert_eq!(
            vec!["HEAD /sb-data".to_string(), "PUT /sb-data".to_string()],
            fake.requests()
        );
    }

    /// The per-namespace layout is the opposite case - a bucket appears when a namespace is first
    /// written to, so creating it is the point and there is nothing to check first.
    #[tokio::test]
    async fn the_per_namespace_layout_goes_straight_to_creating() {
        let (fake, cold_storage) = connect().await;

        cold_storage.ensure_bucket("default").await;

        assert_eq!(vec!["PUT /sb-default".to_string()], fake.requests());
    }

    #[tokio::test]
    async fn the_bucket_is_created_once_per_bucket() {
        let (fake, cold_storage) = connect().await;

        cold_storage.ensure_bucket("default").await;
        cold_storage.ensure_bucket("default").await;

        assert_eq!(
            1,
            fake.requests()
                .iter()
                .filter(|itm| itm.as_str() == "PUT /sb-default")
                .count()
        );

        cold_storage.ensure_bucket("alpha").await;
        assert!(fake.requests().iter().any(|itm| itm == "PUT /sb-alpha"));
    }

    /// A namespace may end with a hyphen; a bucket may not.
    #[test]
    fn an_unusable_bucket_name_is_refused_before_it_is_created() {
        assert!(validate_bucket_name("sb-alpha").is_ok());
        assert!(validate_bucket_name("sb-alpha-").is_err());
        assert!(validate_bucket_name("sb").is_err());
        assert!(validate_bucket_name("sb-Alpha").is_err());
        assert!(validate_bucket_name("-sb-alpha").is_err());
    }
}

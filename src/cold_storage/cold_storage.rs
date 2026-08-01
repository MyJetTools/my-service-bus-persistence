use std::{path::Path, time::Duration};

use ahash::AHashSet;
use my_s3::S3Client;
use parking_lot::Mutex;
use tokio::io::AsyncReadExt;

use crate::settings::S3ConnectionSettings;

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
/// **One bucket per namespace.** That puts the isolation at the S3 level - lifecycle rules, storage
/// class, retention and IAM policies can differ per namespace - and it moves the namespace out of
/// the key: locally a file is `{namespace}/{topic}/{file}`, in the cold tier it is bucket
/// `{prefix}-{namespace}`, key `{topic}/{file}`.
///
/// The prefix is not decoration: a bucket name is unique across every customer of the provider -
/// AWS partition-wide, Hetzner "amongst all Hetzner Object Storage users and across all locations" -
/// so a bare `default` or `alpha` belongs to somebody else. Note the account-wide bucket limits this
/// implies: 100 on Hetzner, 100 (raisable) on AWS.
///
/// A bucket is created lazily, the first time that namespace is touched, and the fact is remembered
/// so the call happens once per namespace per process.
pub struct ColdStorage {
    client: S3Client,
    bucket_prefix: String,
    /// Namespaces whose bucket this process has already ensured.
    ensured: Mutex<AHashSet<String>>,
}

impl ColdStorage {
    pub fn new(settings: &S3ConnectionSettings) -> Self {
        Self {
            client: S3Client {
                access_key: settings.access_key.clone(),
                secret_key: settings.secret_key.clone(),
                region: settings.region.clone(),
                endpoint: settings.endpoint.clone(),
            },
            bucket_prefix: settings.bucket.clone(),
            ensured: Mutex::new(AHashSet::new()),
        }
    }

    pub fn get_bucket(&self, namespace: &str) -> String {
        format!("{}-{}", self.bucket_prefix, namespace)
    }

    /// Creates the namespace's bucket unless this process already did.
    ///
    /// Every operation goes through it, so a namespace that appears at runtime gets its bucket on
    /// first touch. After the first success it is a set lookup.
    pub async fn ensure_bucket(&self, namespace: &str) -> Result<(), String> {
        if self.ensured.lock().contains(namespace) {
            return Ok(());
        }

        let bucket = self.get_bucket(namespace);

        validate_bucket_name(bucket.as_str())?;

        match self.client.create_bucket(bucket.as_str()).await {
            Ok(_) => println!("Created the cold storage bucket '{}'", bucket),
            Err(err) => {
                // Covers both `BucketAlreadyExists` and `BucketAlreadyOwnedByYou`; the second is
                // what every restart after the first one gets.
                if !err.bucket_name_is_taken() {
                    return Err(format!("Can not create the bucket '{}': {:?}", bucket, err));
                }
            }
        }

        self.ensured.lock().insert(namespace.to_string());

        Ok(())
    }

    /// Streams a file up, one chunk at a time - the whole point being that memory does not depend
    /// on the size of the object.
    ///
    /// `key` is relative to the namespace - `{topic}/{file}` - because the namespace is the bucket.
    ///
    /// Each retry reopens the file from the beginning: a streamed body is consumed as it is sent,
    /// so a half-drained reader can not be reused. `PutObject` replaces the object atomically, so a
    /// failed attempt leaves either the previous object or nothing, never a partial one.
    pub async fn upload_file(&self, namespace: &str, key: &str, path: &Path) -> Result<(), String> {
        self.ensure_bucket(namespace).await?;

        let content_length = tokio::fs::metadata(path)
            .await
            .map_err(|err| format!("Can not size {:?}: {}", path, err))?
            .len() as usize;

        let path = path.to_path_buf();

        self.client
            .upload_streamed_with_retries(
                self.get_bucket(namespace).as_str(),
                key,
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
        namespace: &str,
        key: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<u8>, String> {
        self.ensure_bucket(namespace).await?;

        self.client
            .download_file_range(self.get_bucket(namespace).as_str(), key, from, Some(to))
            .await
            .map_err(|err| format!("{:?}", err))
    }

    pub async fn download(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.ensure_bucket(namespace).await?;

        match self
            .client
            .download_file(self.get_bucket(namespace).as_str(), key)
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
    pub async fn exists(&self, namespace: &str, key: &str) -> Result<bool, String> {
        self.ensure_bucket(namespace).await?;

        match self
            .client
            .download_file_range(self.get_bucket(namespace).as_str(), key, 0, Some(0))
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

    pub async fn delete(&self, namespace: &str, key: &str) -> Result<(), String> {
        self.ensure_bucket(namespace).await?;

        match self
            .client
            .delete_file(self.get_bucket(namespace).as_str(), key)
            .await
        {
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
/// A namespace is `[a-z0-9-]` and may **end** with a hyphen, which a bucket may not - so this is a
/// reachable misconfiguration, not a theoretical one, and it is worth failing on loudly rather than
/// discovering it on the first upload.
fn validate_bucket_name(bucket: &str) -> Result<(), String> {
    let invalid = |reason: &str| {
        Err(format!(
            "'{}' is not a valid bucket name: {}. It is the namespace, prefixed with Bucket= from s3_conn_string",
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

    async fn connect() -> (FakeS3, ColdStorage) {
        let fake = FakeS3::start().await;

        let cold_storage = ColdStorage::new(&S3ConnectionSettings {
            endpoint: fake.endpoint.clone(),
            region: "eu-central-1".to_string(),
            access_key: "AKIATEST".to_string(),
            secret_key: "secret".to_string(),
            bucket: "sb".to_string(),
        });

        (fake, cold_storage)
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

        let key = "orders/0000000000000000000.archive";
        let content: Vec<u8> = (0..=255u8).collect();
        let path = temp_file("round_trip", content.as_slice());

        cold_storage
            .upload_file("default", key, path.as_path())
            .await
            .unwrap();

        assert_eq!(
            Some(content.clone()),
            cold_storage.download("default", key).await.unwrap()
        );

        // Inclusive offsets, the way the archive TOC and a sub page are fetched
        let chunk = cold_storage
            .download_range("default", key, 10, 19)
            .await
            .unwrap();
        assert_eq!(content[10..=19].to_vec(), chunk);

        assert!(cold_storage.exists("default", key).await.unwrap());
        assert!(!cold_storage
            .exists("default", "orders/nope.archive")
            .await
            .unwrap());
        assert_eq!(
            None,
            cold_storage.download("default", "nope").await.unwrap()
        );

        cold_storage.delete("default", key).await.unwrap();
        assert_eq!(None, cold_storage.download("default", key).await.unwrap());
        // Deleting what is not there is fine
        cold_storage.delete("default", key).await.unwrap();

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
        let key = "orders/0000000000000000001.archive";

        cold_storage
            .upload_file("default", key, path.as_path())
            .await
            .unwrap();

        assert_eq!(
            Some(content.clone()),
            cold_storage.download("default", key).await.unwrap()
        );
        assert_eq!(
            content.len(),
            fake.get_object("/sb-default/orders/0000000000000000001.archive")
                .unwrap()
                .len()
        );

        let _ = std::fs::remove_file(&path);
    }

    /// A key contains `/`. If the client percent-encoded them the objects would not be browsable
    /// as folders in the bucket, and the cold layout would stop matching the local one.
    #[tokio::test]
    async fn a_key_with_slashes_stays_a_path() {
        let (fake, cold_storage) = connect().await;

        let key = "orders/.2025.yearindex";
        let path = temp_file("slashes", &[1, 2, 3]);

        cold_storage
            .upload_file("alpha", key, path.as_path())
            .await
            .unwrap();

        let paths = fake.object_paths();
        assert_eq!(1, paths.len());
        assert_eq!("/sb-alpha/orders/.2025.yearindex", paths[0]);

        assert_eq!(
            Some(vec![1, 2, 3]),
            cold_storage.download("alpha", key).await.unwrap()
        );
        // The same key in another namespace is another bucket, so it is a different object
        assert_eq!(None, cold_storage.download("default", key).await.unwrap());

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn the_bucket_is_created_once_per_namespace() {
        let (fake, cold_storage) = connect().await;

        cold_storage.ensure_bucket("default").await.unwrap();
        cold_storage.ensure_bucket("default").await.unwrap();

        assert_eq!(
            1,
            fake.requests()
                .iter()
                .filter(|itm| itm.as_str() == "PUT /sb-default")
                .count()
        );

        cold_storage.ensure_bucket("alpha").await.unwrap();
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

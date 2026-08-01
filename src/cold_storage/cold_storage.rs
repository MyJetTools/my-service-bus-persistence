use ahash::AHashSet;
use my_s3::{S3Client, S3Error};
use parking_lot::Mutex;

use crate::settings::S3ConnectionSettings;

/// The cold tier: sealed archives and closed year indexes, uploaded once and read back over ranged
/// GETs. Nothing here is ever modified in place - S3 objects can only be replaced whole, which is
/// exactly why only sealed files get here.
///
/// **One bucket per namespace.** That puts the isolation at the S3 level - lifecycle rules,
/// storage class, retention and IAM policies can differ per namespace - and it moves the namespace
/// out of the key: locally a file is `{namespace}/{topic}/{file}`, in the cold tier it is bucket
/// `{namespace}`, key `{topic}/{file}`.
///
/// The bucket is `{Bucket}-{namespace}`, where `Bucket` comes from the connection string. The
/// prefix is not decoration: a bucket name is unique across every customer of the provider - AWS
/// partition-wide, Hetzner "amongst all Hetzner Object Storage users and across all locations" -
/// so a bare `default` or `alpha` belongs to somebody else.
///
/// Note the account-wide bucket limits this implies: 100 on Hetzner, 100 (raisable) on AWS. One
/// bucket per namespace means roughly that many namespaces.
///
/// A bucket is created lazily, the first time that namespace is touched, and the fact is
/// remembered so the call happens once per namespace per process.
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
                if !already_ours(&err) {
                    return Err(format!("Can not create the bucket '{}': {:?}", bucket, err));
                }
            }
        }

        self.ensured.lock().insert(namespace.to_string());

        Ok(())
    }

    /// `key` is relative to the namespace - `{topic}/{file}` - because the namespace is the bucket.
    pub async fn upload(&self, namespace: &str, key: &str, content: Vec<u8>) -> Result<(), String> {
        self.ensure_bucket(namespace).await?;

        self.client
            .upload_file(self.get_bucket(namespace).as_str(), key, content)
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
                if is_not_found(&err) {
                    return Ok(None);
                }

                Err(format!("{:?}", err))
            }
        }
    }

    /// Cheapest existence probe the client can express today: ask for a single byte and see
    /// whether the object answers.
    pub async fn exists(&self, namespace: &str, key: &str) -> Result<bool, String> {
        self.ensure_bucket(namespace).await?;

        match self
            .client
            .download_file_range(self.get_bucket(namespace).as_str(), key, 0, Some(0))
            .await
        {
            Ok(_) => Ok(true),
            Err(err) => {
                if is_not_found(&err) {
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
                if is_not_found(&err) || is_no_content(&err) {
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
/// reachable misconfiguration, not a theoretical one, and it is worth failing on loudly rather
/// than discovering it on the first upload.
fn validate_bucket_name(bucket: &str) -> Result<(), String> {
    let invalid = |reason: &str| {
        Err(format!(
            "'{}' is not a valid bucket name: {}. It is the namespace, prefixed with Bucket= from s3_conn_string when that is set",
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

/// Both ways S3 can say "this bucket is already there".
///
/// `BucketAlreadyExists` means the name is taken globally - by anyone. `BucketAlreadyOwnedByYou`
/// is the one that actually happens on every restart, and `my-s3` does not model it, so it arrives
/// as `Other`. Treating only the first as success would fail every start after the first.
fn already_ours(err: &S3Error) -> bool {
    match err {
        S3Error::BucketAlreadyExists => true,
        S3Error::Other(text) => text.contains("BucketAlreadyOwnedByYou"),
        S3Error::FlUrlError(_) => false,
        S3Error::RangeNotSatisfiable => false,
    }
}

/// TODO: S3 answers a successful DELETE with **204 No Content**, but `my-s3` only treats 200 as
/// success, so every delete comes back as `Other("Status Code: 204...")`. Until the crate handles
/// it, recognise it here - otherwise hard delete never removes anything from the cold tier.
fn is_no_content(err: &S3Error) -> bool {
    match err {
        S3Error::Other(text) => text.contains("Status Code: 204"),
        S3Error::BucketAlreadyExists => false,
        S3Error::FlUrlError(_) => false,
        S3Error::RangeNotSatisfiable => false,
    }
}

/// TODO: `my-s3` folds every non-2xx into `S3Error::Other(String)`, so a missing key can only be
/// told apart by looking at the rendered status code. Replace this with a typed
/// `S3Error::KeyNotFound` once the crate grows one.
fn is_not_found(err: &S3Error) -> bool {
    match err {
        S3Error::Other(text) => text.contains("Status Code: 404"),
        S3Error::BucketAlreadyExists => false,
        S3Error::FlUrlError(_) => false,
        S3Error::RangeNotSatisfiable => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cold_storage::fake_s3::FakeS3;
    use crate::settings::S3ConnectionSettings;

    async fn connect_with_prefix(prefix: &str) -> (FakeS3, ColdStorage) {
        let fake = FakeS3::start().await;

        let cold_storage = ColdStorage::new(&S3ConnectionSettings {
            endpoint: fake.endpoint.clone(),
            region: "eu-central-1".to_string(),
            access_key: "AKIATEST".to_string(),
            secret_key: "secret".to_string(),
            bucket: prefix.to_string(),
        });

        (fake, cold_storage)
    }

    async fn connect() -> (FakeS3, ColdStorage) {
        connect_with_prefix("sb").await
    }

    /// The whole round trip against a real socket: upload, ranged read, whole read, exists,
    /// delete. This is the only place the `Range` header, the 206 and the 204 are exercised.
    #[tokio::test]
    async fn upload_read_range_and_delete() {
        let (fake, cold_storage) = connect().await;

        let key = "orders/0000000000000000000.archive";
        let content: Vec<u8> = (0..=255u8).collect();

        cold_storage
            .upload("default", key, content.clone())
            .await
            .unwrap();

        // Whole object
        assert_eq!(
            Some(content.clone()),
            cold_storage.download("default", key).await.unwrap()
        );

        // A ranged read - inclusive offsets, the way the archive TOC and a sub page are fetched
        let chunk = cold_storage
            .download_range("default", key, 10, 19)
            .await
            .unwrap();
        assert_eq!(content[10..=19].to_vec(), chunk);

        // Existence is a one-byte ranged read
        assert!(cold_storage.exists("default", key).await.unwrap());
        assert!(!cold_storage
            .exists("default", "orders/nope.archive")
            .await
            .unwrap());

        // A missing object reads as absent rather than as an error
        assert_eq!(
            None,
            cold_storage.download("default", "nope").await.unwrap()
        );

        // S3 answers a successful delete with 204, which must not read as a failure
        cold_storage.delete("default", key).await.unwrap();
        assert_eq!(None, cold_storage.download("default", key).await.unwrap());
        // Deleting what is not there is fine too
        cold_storage.delete("default", key).await.unwrap();

        assert!(fake.object_paths().is_empty());
    }

    /// A key contains `/`. If the client percent-encoded them the objects would not be browsable
    /// as folders in the bucket, and the layout would silently stop matching the local one.
    #[tokio::test]
    async fn a_key_with_slashes_stays_a_path() {
        let (fake, cold_storage) = connect().await;

        let key = "orders/.2025.yearindex";
        cold_storage
            .upload("alpha", key, vec![1, 2, 3])
            .await
            .unwrap();

        let paths = fake.object_paths();
        assert_eq!(1, paths.len());

        println!("what actually went over the wire: {}", paths[0]);
        assert_eq!("/sb-alpha/orders/.2025.yearindex", paths[0]);

        // and it reads back through the same spelling
        assert_eq!(
            Some(vec![1, 2, 3]),
            cold_storage.download("alpha", key).await.unwrap()
        );

        // The same key in another namespace is another bucket, so it is a different object
        assert_eq!(None, cold_storage.download("default", key).await.unwrap());
    }

    #[test]
    fn missing_key_is_detected_by_status_code() {
        assert!(is_not_found(&S3Error::Other(
            "Status Code: 404. Err: <Error><Code>NoSuchKey</Code></Error>".to_string()
        )));

        assert!(!is_not_found(&S3Error::Other(
            "Status Code: 500. Err: boom".to_string()
        )));

        assert!(!is_not_found(&S3Error::RangeNotSatisfiable));
    }

    #[test]
    fn both_spellings_of_an_existing_bucket_are_success() {
        assert!(already_ours(&S3Error::BucketAlreadyExists));
        // What a restart against your own bucket actually returns
        assert!(already_ours(&S3Error::Other(
            "Status Code: 409. Err: <Error><Code>BucketAlreadyOwnedByYou</Code></Error>"
                .to_string()
        )));
        // A real failure is not swallowed
        assert!(!already_ours(&S3Error::Other(
            "Status Code: 403. Err: <Error><Code>SignatureDoesNotMatch</Code></Error>".to_string()
        )));
    }

    #[tokio::test]
    async fn the_bucket_is_created_and_creating_it_twice_is_fine() {
        let (fake, cold_storage) = connect().await;

        cold_storage.ensure_bucket("default").await.unwrap();
        cold_storage.ensure_bucket("default").await.unwrap();

        // Created once per namespace per process, not once per call
        assert_eq!(
            1,
            fake.requests()
                .iter()
                .filter(|itm| itm.as_str() == "PUT /sb-default")
                .count()
        );

        // A second namespace is a second bucket
        cold_storage.ensure_bucket("alpha").await.unwrap();
        assert!(fake.requests().iter().any(|itm| itm == "PUT /sb-alpha"));

        cold_storage
            .upload("default", "a/x", vec![1])
            .await
            .unwrap();
        assert_eq!(
            Some(vec![1]),
            cold_storage.download("default", "a/x").await.unwrap()
        );
    }

    #[test]
    fn a_successful_delete_answers_204() {
        assert!(is_no_content(&S3Error::Other(
            "Status Code: 204. Err: ".to_string()
        )));
        assert!(!is_no_content(&S3Error::Other(
            "Status Code: 500. Err: boom".to_string()
        )));
    }
}

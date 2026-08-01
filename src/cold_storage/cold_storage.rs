use my_s3::{S3Client, S3Error};

use crate::settings::S3ConnectionSettings;

/// The cold tier: sealed archives and closed year indexes, uploaded once and read back over
/// ranged GETs. Nothing here is ever modified in place - S3 objects can only be replaced whole,
/// which is exactly why only sealed files ever get here.
pub struct ColdStorage {
    client: S3Client,
    bucket: String,
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
            bucket: settings.bucket.clone(),
        }
    }

    pub async fn upload(&self, key: &str, content: Vec<u8>) -> Result<(), String> {
        self.client
            .upload_file(self.bucket.as_str(), key, content)
            .await
            .map_err(|err| format!("{:?}", err))
    }

    /// `from`/`to` are inclusive byte offsets, as in the HTTP `Range` header.
    pub async fn download_range(&self, key: &str, from: u64, to: u64) -> Result<Vec<u8>, String> {
        self.client
            .download_file_range(self.bucket.as_str(), key, from, Some(to))
            .await
            .map_err(|err| format!("{:?}", err))
    }

    pub async fn download(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        match self.client.download_file(self.bucket.as_str(), key).await {
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
    pub async fn exists(&self, key: &str) -> Result<bool, String> {
        match self
            .client
            .download_file_range(self.bucket.as_str(), key, 0, Some(0))
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

    pub async fn delete(&self, key: &str) -> Result<(), String> {
        match self.client.delete_file(self.bucket.as_str(), key).await {
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
            bucket: "sb-persistence".to_string(),
        });

        (fake, cold_storage)
    }

    /// The whole round trip against a real socket: upload, ranged read, whole read, exists,
    /// delete. This is the only place the `Range` header, the 206 and the 204 are exercised.
    #[tokio::test]
    async fn upload_read_range_and_delete() {
        let (fake, cold_storage) = connect().await;

        let key = "default/orders/0000000000000000000.archive";
        let content: Vec<u8> = (0..=255u8).collect();

        cold_storage.upload(key, content.clone()).await.unwrap();

        // Whole object
        assert_eq!(
            Some(content.clone()),
            cold_storage.download(key).await.unwrap()
        );

        // A ranged read - inclusive offsets, the way the archive TOC and a sub page are fetched
        let chunk = cold_storage.download_range(key, 10, 19).await.unwrap();
        assert_eq!(content[10..=19].to_vec(), chunk);

        // Existence is a one-byte ranged read
        assert!(cold_storage.exists(key).await.unwrap());
        assert!(!cold_storage
            .exists("default/orders/nope.archive")
            .await
            .unwrap());

        // A missing object reads as absent rather than as an error
        assert_eq!(None, cold_storage.download("default/nope").await.unwrap());

        // S3 answers a successful delete with 204, which must not read as a failure
        cold_storage.delete(key).await.unwrap();
        assert_eq!(None, cold_storage.download(key).await.unwrap());
        // Deleting what is not there is fine too
        cold_storage.delete(key).await.unwrap();

        assert!(fake.object_paths().is_empty());
    }

    /// A key contains `/`. If the client percent-encoded them the objects would not be browsable
    /// as folders in the bucket, and the layout would silently stop matching the local one.
    #[tokio::test]
    async fn a_key_with_slashes_stays_a_path() {
        let (fake, cold_storage) = connect().await;

        let key = "alpha/orders/.2025.yearindex";
        cold_storage.upload(key, vec![1, 2, 3]).await.unwrap();

        let paths = fake.object_paths();
        assert_eq!(1, paths.len());

        println!("what actually went over the wire: {}", paths[0]);
        assert_eq!("/sb-persistence/alpha/orders/.2025.yearindex", paths[0]);

        // and it reads back through the same spelling
        assert_eq!(
            Some(vec![1, 2, 3]),
            cold_storage.download(key).await.unwrap()
        );
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
    fn a_successful_delete_answers_204() {
        assert!(is_no_content(&S3Error::Other(
            "Status Code: 204. Err: ".to_string()
        )));
        assert!(!is_no_content(&S3Error::Other(
            "Status Code: 500. Err: boom".to_string()
        )));
    }
}

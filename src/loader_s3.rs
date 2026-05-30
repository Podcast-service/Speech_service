use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::{primitives::ByteStream, Client};
use std::env;
use tracing::{info, warn};

use crate::storage::StorageBackend;

const DEFAULT_S3_REGION: &str = "ru-1";
const DEFAULT_S3_ENDPOINT_URL: &str = "https://s3.twcstorage.ru";

pub struct Config {
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            region: env_value("S3_REGION").unwrap_or_else(|| DEFAULT_S3_REGION.to_string()),
            access_key_id: env_value("S3_ACCESS_KEY_ID").context("S3_ACCESS_KEY_ID is required")?,
            secret_access_key: env_value("S3_SECRET_ACCESS_KEY")
                .context("S3_SECRET_ACCESS_KEY is required")?,
            endpoint_url: env_value("S3_ENDPOINT_URL")
                .unwrap_or_else(|| DEFAULT_S3_ENDPOINT_URL.to_string()),
        })
    }
}

pub struct S3Client {
    client: Client,
}

impl S3Client {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn ensure_bucket_internal(&self, bucket: &str) -> Result<()> {
        match self.client.head_bucket().bucket(bucket).send().await {
            Ok(_) => {
                info!("Bucket '{}' is available", bucket);
                return Ok(());
            }
            Err(err) if !should_create_bucket() => {
                return Err(err).with_context(|| {
                    format!(
                        "bucket '{bucket}' is not available; check S3_BUCKET and credentials, or set S3_CREATE_BUCKET=true for local S3-compatible storage"
                    )
                });
            }
            Err(err) => {
                warn!(
                    "Bucket '{}' is not available, trying to create it: code={:?}, message={:?}",
                    bucket,
                    err.code(),
                    err.message()
                );
            }
        }

        match self.client.create_bucket().bucket(bucket).send().await {
            Ok(_) => info!("Bucket '{}' created successfully", bucket),
            Err(err) if err.code() == Some("BucketAlreadyOwnedByYou") => {
                info!("Bucket '{}' already exists, skip create", bucket);
            }
            Err(err) => {
                return Err(err).with_context(|| format!("create_bucket failed for {bucket}"));
            }
        }
        Ok(())
    }

    pub async fn get_bytes_internal(&self, bucket: &str, object_key: &str) -> Result<Vec<u8>> {
        let response = self
            .client
            .get_object()
            .bucket(bucket)
            .key(object_key)
            .send()
            .await
            .with_context(|| {
                format!("error downloading object '{object_key}' from bucket '{bucket}'")
            })?;

        let bytes = response
            .body
            .collect()
            .await
            .with_context(|| format!("error reading object body '{object_key}'"))?
            .into_bytes();

        Ok(bytes.to_vec())
    }

    pub async fn list_objects_internal(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let response = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .send()
            .await
            .with_context(|| {
                format!(
                    "error listing objects in bucket '{}' with prefix '{}'",
                    bucket, prefix
                )
            })?;

        let keys = response
            .contents()
            .iter()
            .filter_map(|object| object.key().map(ToString::to_string))
            .collect::<Vec<_>>();

        Ok(keys)
    }

    pub async fn upload_bytes_internal(
        &self,
        bucket: &str,
        object_key: &str,
        bytes: Vec<u8>,
    ) -> Result<()> {
        self.client
            .put_object()
            .bucket(bucket)
            .key(object_key)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .with_context(|| format!("error uploading '{object_key}' to bucket '{bucket}'"))?;

        Ok(())
    }
}

pub async fn create_client(cfg: &Config) -> Result<S3Client> {
    let credentials = Credentials::new(
        cfg.access_key_id.clone(),
        cfg.secret_access_key.clone(),
        None,
        None,
        "s3-compatible",
    );

    let shared_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(cfg.region.clone()))
        .credentials_provider(credentials)
        .endpoint_url(cfg.endpoint_url.clone())
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
        .force_path_style(true)
        .build();

    Ok(S3Client::new(Client::from_conf(s3_config)))
}

fn env_value(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn should_create_bucket() -> bool {
    env::var("S3_CREATE_BUCKET")
        .map(|value| value.eq_ignore_ascii_case("true") || value == "1")
        .unwrap_or(false)
}

#[async_trait]
impl StorageBackend for S3Client {
    fn name(&self) -> &str {
        "s3"
    }

    async fn ensure_bucket(&self, bucket: &str) -> Result<()> {
        self.ensure_bucket_internal(bucket).await
    }

    async fn get_object(&self, bucket: &str, object_key: &str) -> Result<Vec<u8>> {
        self.get_bytes_internal(bucket, object_key).await
    }

    async fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        self.list_objects_internal(bucket, prefix).await
    }

    async fn upload_bytes(&self, bucket: &str, object_key: &str, bytes: Vec<u8>) -> Result<()> {
        self.upload_bytes_internal(bucket, object_key, bytes).await
    }
}

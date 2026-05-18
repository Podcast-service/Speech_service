use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    fn name(&self) -> &str;

    async fn ensure_bucket(&self, bucket: &str) -> Result<()>;

    async fn get_object(&self, bucket: &str, object_key: &str) -> Result<Vec<u8>>;

    async fn list_objects(&self, bucket: &str, prefix: &str) -> Result<Vec<String>>;

    async fn upload_bytes(&self, bucket: &str, object_key: &str, bytes: Vec<u8>) -> Result<()>;
}

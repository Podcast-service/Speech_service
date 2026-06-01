use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Backend-результат субтитров для podcast_core (BackendSubtitleReadyEvent).
const TOPIC_SUBTITLE: &str = "media.subtitle";
const PUBLIC_S3_BASE_URL: &str = "https://s3.twcstorage.ru";
/// Публичный результат субтитров для внешних потребителей.
const TOPIC_SUBTITLE_READY: &str = "media.subtitle.ready";
/// Публичные ошибки генерации субтитров.
const TOPIC_SUBTITLE_ERROR: &str = "media.subtitle.error";
/// Публичный поток worker-домена: связка HLS <-> субтитры.
const TOPIC_MEDIA_WORKER_EVENTS: &str = "media.worker.events";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubtitleRequestedEvent {
    pub file_id: String,
    pub source_bucket: String,
    pub source_object_key: String,
    pub language: Option<String>,
    pub num_speakers: Option<u32>,
    pub requested_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubtitleReadyEvent {
    pub file_id: String,
    pub bucket: String,
    pub vtt_object_key: String,
    pub srt_object_key: String,
    pub language: String,
    pub segments: usize,
    pub ready_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackendSubtitleContent {
    pub vtt_object_key: String,
    pub srt_object_key: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackendSubtitleReadyEvent {
    pub podcast_id: String,
    pub content: BackendSubtitleContent,
    pub ready_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubtitleErrorEvent {
    pub file_id: String,
    pub stage: String,
    pub error_message: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaWorkerSubtitleReadyEvent {
    pub file_id: String,
    pub hls_path: String,
    pub subtitle_vtt_path: String,
    pub subtitle_srt_path: String,
    pub language: String,
    pub subtitle_ready_at: String,
}

pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    pub fn new(brokers: &str) -> Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("message.send.max.retries", "10")
            .set("retry.backoff.ms", "500")
            .set("reconnect.backoff.ms", "500")
            .set("reconnect.backoff.max.ms", "10000")
            .set("socket.keepalive.enable", "true")
            .create::<FutureProducer>()
            .context("Failed to create Kafka producer")?;

        Ok(Self { producer })
    }

    pub async fn send_subtitle_ready(
        &self,
        file_id: Uuid,
        bucket: &str,
        vtt_object_key: &str,
        srt_object_key: &str,
        language: &str,
        segments: usize,
    ) -> Result<()> {
        let ready_at = Utc::now().to_rfc3339();
        let event = SubtitleReadyEvent {
            file_id: file_id.to_string(),
            bucket: bucket.to_string(),
            vtt_object_key: vtt_object_key.to_string(),
            srt_object_key: srt_object_key.to_string(),
            language: language.to_string(),
            segments,
            ready_at: ready_at.clone(),
        };
        let backend_event = backend_subtitle_ready_event(
            &event.file_id,
            bucket,
            vtt_object_key,
            srt_object_key,
            ready_at,
        );

        // Публичное событие — на отдельный топик; backend-результат с podcast_id — в media.subtitle для core.
        let subtitle_result = self
            .send_json_to_topic(TOPIC_SUBTITLE_READY, &event.file_id, &event)
            .await;
        let backend_result = self.send_json(&event.file_id, &backend_event).await;
        subtitle_result?;
        backend_result?;

        Ok(())
    }

    pub async fn send_subtitle_error(
        &self,
        file_id: Uuid,
        stage: &str,
        error_message: &str,
    ) -> Result<()> {
        let event = SubtitleErrorEvent {
            file_id: file_id.to_string(),
            stage: stage.to_string(),
            error_message: error_message.to_string(),
            timestamp: Utc::now().to_rfc3339(),
        };

        self.send_json_to_topic(TOPIC_SUBTITLE_ERROR, &event.file_id, &event)
            .await
    }

    pub async fn send_worker_subtitle_ready(
        &self,
        file_id: Uuid,
        hls_path: &str,
        subtitle_vtt_path: &str,
        subtitle_srt_path: &str,
        language: &str,
    ) -> Result<()> {
        let event = MediaWorkerSubtitleReadyEvent {
            file_id: file_id.to_string(),
            hls_path: hls_path.to_string(),
            subtitle_vtt_path: subtitle_vtt_path.to_string(),
            subtitle_srt_path: subtitle_srt_path.to_string(),
            language: language.to_string(),
            subtitle_ready_at: Utc::now().to_rfc3339(),
        };

        self.send_json_to_topic(TOPIC_MEDIA_WORKER_EVENTS, &event.file_id, &event)
            .await
    }

    async fn send_json<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        self.send_json_to_topic(TOPIC_SUBTITLE, key, value).await
    }

    async fn send_json_to_topic<T: Serialize>(
        &self,
        topic: &str,
        key: &str,
        value: &T,
    ) -> Result<()> {
        let payload = serde_json::to_string(value)?;
        let record = FutureRecord::to(topic).key(key).payload(&payload);

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(err, _)| anyhow::anyhow!("Kafka send failed: {}", err))?;

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.producer
            .flush(Duration::from_secs(10))
            .context("Failed to flush Kafka producer")?;
        Ok(())
    }
}

pub type SharedKafkaProducer = Arc<KafkaProducer>;

pub fn new_producer(brokers: &str) -> Result<SharedKafkaProducer> {
    Ok(Arc::new(KafkaProducer::new(brokers)?))
}

fn backend_subtitle_ready_event(
    podcast_id: &str,
    bucket: &str,
    vtt_object_key: &str,
    srt_object_key: &str,
    ready_at: String,
) -> BackendSubtitleReadyEvent {
    BackendSubtitleReadyEvent {
        podcast_id: podcast_id.to_string(),
        content: BackendSubtitleContent {
            vtt_object_key: public_s3_object_url(bucket, vtt_object_key),
            srt_object_key: public_s3_object_url(bucket, srt_object_key),
        },
        ready_at,
    }
}

fn public_s3_object_url(bucket: &str, object_key: &str) -> String {
    format!(
        "{}/{}/{}",
        PUBLIC_S3_BASE_URL,
        bucket.trim_matches('/'),
        object_key.trim_start_matches('/')
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_subtitle_ready_event_uses_public_urls_in_nested_content() {
        let event = backend_subtitle_ready_event(
            "11111111-1111-4111-8111-111111111111",
            "subtitle-bucket",
            "media/id/subtitles.vtt",
            "media/id/subtitles.srt",
            "2026-05-31T00:00:00Z".into(),
        );
        let value = serde_json::to_value(event).unwrap();

        assert_eq!(value["podcast_id"], "11111111-1111-4111-8111-111111111111");
        assert_eq!(
            value["content"]["vtt_object_key"],
            "https://s3.twcstorage.ru/subtitle-bucket/media/id/subtitles.vtt"
        );
        assert_eq!(
            value["content"]["srt_object_key"],
            "https://s3.twcstorage.ru/subtitle-bucket/media/id/subtitles.srt"
        );
    }
}

use std::sync::Arc;

use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio_stream::StreamExt;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::kafka::{SharedKafkaProducer, SubtitleRequestedEvent};
use crate::pipeline;
use crate::storage::StorageBackend;
use crate::transcriber::SharedTranscriber;

const TOPIC: &str = "media.subtitle";
const GROUP_ID: &str = "media-subtitle-worker-service";

pub async fn run_subtitle_consumer(
    brokers: &str,
    storage: Arc<dyn StorageBackend>,
    kafka: SharedKafkaProducer,
    transcriber: SharedTranscriber,
    subtitle_bucket: String,
    max_retries: u32,
) -> Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", GROUP_ID)
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "latest")
        .set("reconnect.backoff.ms", "500")
        .set("reconnect.backoff.max.ms", "10000")
        .set("retry.backoff.ms", "500")
        .set("socket.keepalive.enable", "true")
        .create()
        .context("Failed to create Kafka consumer")?;

    consumer
        .subscribe(&[TOPIC])
        .context("Failed to subscribe to subtitle topic")?;

    info!(
        "Subtitle consumer started: topic='{}', group='{}'",
        TOPIC, GROUP_ID
    );

    let mut stream = consumer.stream();

    while let Some(result) = stream.next().await {
        match result {
            Ok(msg) => {
                let payload = match msg.payload_view::<str>() {
                    Some(Ok(text)) => text,
                    Some(Err(e)) => {
                        warn!("Error decoding Kafka payload: {}", e);
                        continue;
                    }
                    None => {
                        warn!("Empty Kafka message on {}", TOPIC);
                        continue;
                    }
                };

                let event: SubtitleRequestedEvent = match serde_json::from_str(payload) {
                    Ok(event) => event,
                    Err(_) => {
                        continue;
                    }
                };

                handle_subtitle_requested(
                    event,
                    &storage,
                    &kafka,
                    &transcriber,
                    &subtitle_bucket,
                    max_retries,
                )
                .await;
            }
            Err(e) => {
                error!("Kafka consumer error: {}", e);
            }
        }
    }

    Ok(())
}

async fn handle_subtitle_requested(
    event: SubtitleRequestedEvent,
    storage: &Arc<dyn StorageBackend>,
    kafka: &SharedKafkaProducer,
    transcriber: &SharedTranscriber,
    subtitle_bucket: &str,
    max_retries: u32,
) {
    let file_id = match Uuid::parse_str(&event.file_id) {
        Ok(id) => id,
        Err(e) => {
            warn!("Invalid file_id in media.subtitle.requested event: {}", e);
            return;
        }
    };

    info!(
        "Received media.subtitle.requested: file_id={}, source={}/{}",
        event.file_id, event.source_bucket, event.source_object_key
    );

    let storage = storage.clone();
    let kafka = kafka.clone();
    let transcriber = transcriber.clone();
    let subtitle_bucket = subtitle_bucket.to_string();

    tokio::spawn(async move {
        pipeline::run_pipeline(
            file_id,
            event,
            storage,
            kafka,
            transcriber,
            &subtitle_bucket,
            max_retries,
        )
        .await;
    });
}

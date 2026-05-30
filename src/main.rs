mod consumer;
mod kafka;
mod loader_s3;
mod pipeline;
mod storage;
mod subtitle;
mod transcriber;

use std::{sync::Arc, time::Duration};

use tracing::{error, info};
use transcriber::{build_transcriber, SharedTranscriber};
use uuid::Uuid;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let s3_cfg = loader_s3::Config::from_env().expect("S3 config: set S3_* env variables");
    let storage_client = loader_s3::create_client(&s3_cfg)
        .await
        .expect("Failed to create S3 client");
    let storage: Arc<dyn storage::StorageBackend> = Arc::new(storage_client);

    let kafka_brokers = std::env::var("KAFKA_BROKERS")
        .expect("KAFKA_BROKERS is required (example: kafka:9092 inside Docker network)");
    let kafka = kafka::new_producer(&kafka_brokers).expect("Failed to create Kafka producer");

    let subtitle_bucket = std::env::var("SUBTITLE_BUCKET")
        .or_else(|_| std::env::var("S3_BUCKET"))
        .unwrap_or_else(|_| "4c5face5-544c-4bc2-a2e0-57a24d243af3".to_string());
    let subtitle_max_retries = std::env::var("SUBTITLE_MAX_RETRIES")
        .unwrap_or_else(|_| "3".to_string())
        .parse::<u32>()
        .expect("SUBTITLE_MAX_RETRIES must be a positive integer");
    let worker_id = subtitle_worker_id();

    let transcriber_backend =
        std::env::var("TRANSCRIBER_BACKEND").unwrap_or_else(|_| "mock".to_string());
    let whisper_model_path = std::env::var("WHISPER_MODEL_PATH").ok();
    let pyannote_enabled = std::env::var("PYANNOTE_ENABLED")
        .unwrap_or_else(|_| "false".to_string())
        .eq_ignore_ascii_case("true");
    let pyannote_python_bin =
        std::env::var("PYANNOTE_PYTHON_BIN").unwrap_or_else(|_| "python3".to_string());
    let pyannote_script_path =
        std::env::var("PYANNOTE_SCRIPT_PATH").unwrap_or_else(|_| "pyannote/diarize.py".to_string());
    let pyannote_hf_token = std::env::var("PYANNOTE_HF_TOKEN").ok();

    let transcriber: SharedTranscriber = build_transcriber(
        &transcriber_backend,
        whisper_model_path,
        pyannote_enabled,
        pyannote_python_bin,
        pyannote_script_path,
        pyannote_hf_token,
    )
    .expect("Failed to initialize transcriber backend");

    info!(
        "subtitle_worker started (worker_id={}, kafka={}, bucket={})",
        worker_id, kafka_brokers, subtitle_bucket
    );

    loop {
        if let Err(e) = consumer::run_subtitle_consumer(
            &kafka_brokers,
            &worker_id,
            storage.clone(),
            kafka.clone(),
            transcriber.clone(),
            subtitle_bucket.clone(),
            subtitle_max_retries,
        )
        .await
        {
            error!("Subtitle consumer crashed: {}", e);
        } else {
            error!("Subtitle consumer stopped unexpectedly");
        }

        let delay = consumer_restart_delay();
        info!(
            "Restarting subtitle consumer in {} seconds",
            delay.as_secs()
        );
        tokio::time::sleep(delay).await;
    }
}

fn consumer_restart_delay() -> Duration {
    std::env::var("KAFKA_CONSUMER_RESTART_DELAY_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(5))
}

fn subtitle_worker_id() -> String {
    std::env::var("SUBTITLE_WORKER_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("HOSTNAME")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::fs;
use tracing::{info, warn};
use uuid::Uuid;

use crate::kafka::{SharedKafkaProducer, SubtitleRequestedEvent};
use crate::storage::StorageBackend;
use crate::subtitle;
use crate::transcriber::SharedTranscriber;

pub struct SubtitleResult {
    pub bucket: String,
    pub vtt_object_key: String,
    pub srt_object_key: String,
    pub language: String,
    pub segments: usize,
}

pub async fn run_pipeline(
    file_id: Uuid,
    event: SubtitleRequestedEvent,
    storage: Arc<dyn StorageBackend>,
    kafka: SharedKafkaProducer,
    transcriber: SharedTranscriber,
    subtitle_bucket: &str,
    max_retries: u32,
) {
    let mut last_error = String::new();

    for attempt in 1..=max_retries {
        info!(
            "Subtitle pipeline attempt {}/{} for file_id={}",
            attempt, max_retries, file_id
        );

        match execute_pipeline(file_id, &event, &storage, &transcriber, subtitle_bucket).await {
            Ok(result) => {
                if let Err(e) = kafka
                    .send_subtitle_ready(
                        file_id,
                        &result.bucket,
                        &result.vtt_object_key,
                        &result.srt_object_key,
                        &result.language,
                        result.segments,
                    )
                    .await
                {
                    warn!("Failed to publish media.subtitle.ready: {}", e);
                }

                let hls_path = format!("/media/{}/master.m3u8", file_id);
                let subtitle_vtt_path = format!("/{}", result.vtt_object_key);
                let subtitle_srt_path = format!("/{}", result.srt_object_key);

                if let Err(e) = kafka
                    .send_worker_subtitle_ready(
                        file_id,
                        &hls_path,
                        &subtitle_vtt_path,
                        &subtitle_srt_path,
                        &result.language,
                    )
                    .await
                {
                    warn!("Failed to publish media.worker subtitle linkage: {}", e);
                }

                return;
            }
            Err(e) => {
                last_error = format!("{:#}", e);
                warn!(
                    "Subtitle pipeline attempt {}/{} failed for file_id={}: {}",
                    attempt, max_retries, file_id, last_error
                );

                if attempt < max_retries {
                    tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt))).await;
                }
            }
        }
    }

    if let Err(e) = kafka
        .send_subtitle_error(file_id, "transcription", &last_error)
        .await
    {
        warn!("Failed to publish media.subtitle.error: {}", e);
    }
}

async fn execute_pipeline(
    file_id: Uuid,
    event: &SubtitleRequestedEvent,
    storage: &Arc<dyn StorageBackend>,
    transcriber: &SharedTranscriber,
    subtitle_bucket: &str,
) -> Result<SubtitleResult> {
    storage
        .ensure_bucket(subtitle_bucket)
        .await
        .context("ensure subtitle bucket")?;

    let source_bytes = storage
        .get_object(&event.source_bucket, &event.source_object_key)
        .await
        .with_context(|| {
            format!(
                "download source object {}/{}",
                event.source_bucket, event.source_object_key
            )
        })?;

    let temp_audio = if event.source_object_key.ends_with(".m4s") {
        let init_object_key = infer_init_object_key(&event.source_object_key).ok_or_else(|| {
            anyhow::anyhow!(
                "failed to infer init segment key for source object {}",
                event.source_object_key
            )
        })?;
        let variant_prefix = infer_variant_prefix(&event.source_object_key).ok_or_else(|| {
            anyhow::anyhow!(
                "failed to infer variant prefix for source object {}",
                event.source_object_key
            )
        })?;

        let init_bytes = storage
            .get_object(&event.source_bucket, &init_object_key)
            .await
            .with_context(|| {
                format!(
                    "download init segment {}/{}",
                    event.source_bucket, init_object_key
                )
            })?;

        let mut segment_keys = storage
            .list_objects(&event.source_bucket, &variant_prefix)
            .await
            .with_context(|| {
                format!(
                    "list variant segments in {}/{}",
                    event.source_bucket, variant_prefix
                )
            })?
            .into_iter()
            .filter(|key| key.ends_with(".m4s") && !key.ends_with("init.mp4"))
            .collect::<Vec<_>>();
        segment_keys.sort();

        if segment_keys.is_empty() {
            segment_keys.push(event.source_object_key.clone());
        }

        let mut segment_blobs = Vec::with_capacity(segment_keys.len());
        for key in segment_keys {
            let bytes = if key == event.source_object_key {
                source_bytes.clone()
            } else {
                storage
                    .get_object(&event.source_bucket, &key)
                    .await
                    .with_context(|| {
                        format!("download media segment {}/{}", event.source_bucket, key)
                    })?
            };
            segment_blobs.push(bytes);
        }

        write_temp_fragmented_audio(file_id, init_bytes, segment_blobs).await?
    } else {
        write_temp_audio(file_id, &event.source_object_key, source_bytes).await?
    };

    let transcript = transcriber
        .transcribe(&temp_audio, event.language.as_deref(), event.num_speakers)
        .await
        .context("transcribe audio")?;

    let vtt = subtitle::to_webvtt(&transcript);
    let srt = subtitle::to_srt(&transcript);

    let base_prefix = format!("media/{}/", file_id);
    let vtt_key = format!("{}subtitles.vtt", base_prefix);
    let srt_key = format!("{}subtitles.srt", base_prefix);

    storage
        .upload_bytes(subtitle_bucket, &vtt_key, vtt.into_bytes())
        .await
        .context("upload vtt")?;

    storage
        .upload_bytes(subtitle_bucket, &srt_key, srt.into_bytes())
        .await
        .context("upload srt")?;

    let result = SubtitleResult {
        bucket: subtitle_bucket.to_string(),
        vtt_object_key: vtt_key,
        srt_object_key: srt_key,
        language: transcript.language,
        segments: transcript.segments.len(),
    };

    cleanup_temp_file(&temp_audio).await;

    Ok(result)
}

async fn write_temp_audio(
    file_id: Uuid,
    source_object_key: &str,
    bytes: Vec<u8>,
) -> Result<PathBuf> {
    let extension = std::path::Path::new(source_object_key)
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("bin");
    let path = std::env::temp_dir().join(format!("subtitle_input_{}.{}", file_id, extension));
    fs::write(&path, bytes).await?;
    Ok(path)
}

async fn write_temp_fragmented_audio(
    file_id: Uuid,
    init_bytes: Vec<u8>,
    segment_blobs: Vec<Vec<u8>>,
) -> Result<PathBuf> {
    let path = std::env::temp_dir().join(format!("subtitle_input_{}.mp4", file_id));
    let segments_total = segment_blobs.iter().map(|blob| blob.len()).sum::<usize>();
    let mut merged = Vec::with_capacity(init_bytes.len() + segments_total);
    merged.extend_from_slice(&init_bytes);
    for blob in segment_blobs {
        merged.extend_from_slice(&blob);
    }
    fs::write(&path, merged).await?;
    Ok(path)
}

fn infer_init_object_key(source_object_key: &str) -> Option<String> {
    let path = std::path::Path::new(source_object_key);
    let parent = path.parent()?;
    Some(parent.join("init.mp4").to_string_lossy().into_owned())
}

fn infer_variant_prefix(source_object_key: &str) -> Option<String> {
    let path = std::path::Path::new(source_object_key);
    let parent = path.parent()?;
    Some(format!("{}/", parent.to_string_lossy()))
}

async fn cleanup_temp_file(path: &PathBuf) {
    let _ = fs::remove_file(path).await;
}

#[cfg(test)]
mod tests {
    use super::{infer_init_object_key, infer_variant_prefix};

    #[test]
    fn infers_init_object_key_from_segment_key() {
        let key = "media/abc/64k/seg_00000.m4s";
        let init = infer_init_object_key(key);
        assert_eq!(init.as_deref(), Some("media/abc/64k/init.mp4"));
    }

    #[test]
    fn infers_variant_prefix_from_segment_key() {
        let key = "media/abc/64k/seg_00000.m4s";
        let prefix = infer_variant_prefix(key);
        assert_eq!(prefix.as_deref(), Some("media/abc/64k/"));
    }
}

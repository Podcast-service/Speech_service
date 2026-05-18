use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};
use async_trait::async_trait;

#[cfg(feature = "whisper-rs-backend")]
use serde::Deserialize;
#[cfg(feature = "whisper-rs-backend")]
use std::path::PathBuf;
#[cfg(feature = "whisper-rs-backend")]
use std::process::Command;
#[cfg(feature = "whisper-rs-backend")]
use uuid::Uuid;

#[cfg(feature = "whisper-rs-backend")]
use whisper_rs::{FullParams, SamplingStrategy, WhisperContext, WhisperContextParameters};

#[derive(Debug, Clone)]
pub struct TranscriptSegment {
    pub start_ms: u64,
    pub end_ms: u64,
    pub text: String,
    pub speaker: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Transcript {
    pub language: String,
    pub segments: Vec<TranscriptSegment>,
}

#[async_trait]
pub trait Transcriber: Send + Sync {
    async fn transcribe(&self, input_path: &Path, language: Option<&str>) -> Result<Transcript>;
}

pub type SharedTranscriber = Arc<dyn Transcriber>;

pub fn build_transcriber(
    backend: &str,
    whisper_model_path: Option<String>,
    pyannote_enabled: bool,
    pyannote_python_bin: String,
    pyannote_script_path: String,
    pyannote_hf_token: Option<String>,
) -> Result<SharedTranscriber> {
    match backend {
        "mock" => Ok(Arc::new(MockTranscriber)),
        "whisper-rs" => Ok(Arc::new(WhisperRsTranscriber::new(
            whisper_model_path,
            pyannote_enabled,
            pyannote_python_bin,
            pyannote_script_path,
            pyannote_hf_token,
        )?)),
        other => bail!("Unsupported TRANSCRIBER_BACKEND: {other}"),
    }
}

pub struct MockTranscriber;

#[async_trait]
impl Transcriber for MockTranscriber {
    async fn transcribe(&self, _input_path: &Path, language: Option<&str>) -> Result<Transcript> {
        let lang = language.unwrap_or("ru").to_string();

        Ok(Transcript {
            language: lang,
            segments: vec![
                TranscriptSegment {
                    start_ms: 0,
                    end_ms: 2500,
                    text: "Это тестовый сегмент субтитров.".to_string(),
                    speaker: None,
                },
                TranscriptSegment {
                    start_ms: 2500,
                    end_ms: 5000,
                    text: "Замените MockTranscriber на whisper-rs backend.".to_string(),
                    speaker: None,
                },
            ],
        })
    }
}

pub struct WhisperRsTranscriber {
    model_path: String,
    pyannote_enabled: bool,
    pyannote_python_bin: String,
    pyannote_script_path: String,
    pyannote_hf_token: Option<String>,
}

impl WhisperRsTranscriber {
    pub fn new(
        model_path: Option<String>,
        pyannote_enabled: bool,
        pyannote_python_bin: String,
        pyannote_script_path: String,
        pyannote_hf_token: Option<String>,
    ) -> Result<Self> {
        let model_path = model_path.ok_or_else(|| {
            anyhow::anyhow!("WHISPER_MODEL_PATH is required for TRANSCRIBER_BACKEND=whisper-rs")
        })?;

        if !Path::new(&model_path).exists() {
            bail!(
                "Whisper model file not found at '{}'. Mount model into /models and set WHISPER_MODEL_PATH accordingly.",
                model_path
            );
        }

        Ok(Self {
            model_path,
            pyannote_enabled,
            pyannote_python_bin,
            pyannote_script_path,
            pyannote_hf_token,
        })
    }
}

#[async_trait]
impl Transcriber for WhisperRsTranscriber {
    async fn transcribe(&self, input_path: &Path, language: Option<&str>) -> Result<Transcript> {
        #[cfg(feature = "whisper-rs-backend")]
        {
            let model_path = self.model_path.clone();
            let input_path = input_path.to_path_buf();
            let language = language.map(|s| s.to_string());
            let pyannote_enabled = self.pyannote_enabled;
            let pyannote_python_bin = self.pyannote_python_bin.clone();
            let pyannote_script_path = self.pyannote_script_path.clone();
            let pyannote_hf_token = self.pyannote_hf_token.clone();

            let transcript = tokio::task::spawn_blocking(move || {
                transcribe_with_whisper_rs(
                    &model_path,
                    &input_path,
                    language.as_deref(),
                    pyannote_enabled,
                    &pyannote_python_bin,
                    &pyannote_script_path,
                    pyannote_hf_token.as_deref(),
                )
            })
            .await
            .map_err(|e| anyhow::anyhow!("whisper-rs task panicked: {}", e))??;

            Ok(transcript)
        }

        #[cfg(not(feature = "whisper-rs-backend"))]
        {
            let _input_path = input_path;
            let _language = language;
            let _touch = &self.model_path;
            let _py = &self.pyannote_enabled;
            let _py_bin = &self.pyannote_python_bin;
            let _py_script = &self.pyannote_script_path;
            let _py_token = &self.pyannote_hf_token;
            bail!(
                "TRANSCRIBER_BACKEND=whisper-rs requires build with --features whisper-rs-backend"
            )
        }
    }
}

#[cfg(feature = "whisper-rs-backend")]
#[derive(Debug, Clone, Deserialize)]
struct PyannoteResult {
    segments: Vec<DiarizationSegment>,
}

#[cfg(feature = "whisper-rs-backend")]
#[derive(Debug, Clone, Deserialize)]
struct DiarizationSegment {
    speaker: String,
    start_ms: u64,
    end_ms: u64,
}

#[cfg(feature = "whisper-rs-backend")]
fn transcribe_with_whisper_rs(
    model_path: &str,
    input_path: &Path,
    requested_language: Option<&str>,
    pyannote_enabled: bool,
    pyannote_python_bin: &str,
    pyannote_script_path: &str,
    pyannote_hf_token: Option<&str>,
) -> Result<Transcript> {
    let wav_path = decode_to_wav_16khz_mono(input_path)?;
    let samples = read_wav_f32(&wav_path)?;

    let ctx = WhisperContext::new_with_params(model_path, WhisperContextParameters::default())
        .map_err(|e| anyhow::anyhow!("whisper context init failed: {}", e))?;
    let mut state = ctx
        .create_state()
        .map_err(|e| anyhow::anyhow!("whisper state init failed: {}", e))?;

    let whisper_best_of = env_i32("WHISPER_GREEDY_BEST_OF", 8).max(1);
    let whisper_threads = env_i32("WHISPER_THREADS", 4).max(1);
    let mut params = FullParams::new(SamplingStrategy::Greedy {
        best_of: whisper_best_of,
    });
    params.set_n_threads(whisper_threads);
    params.set_print_progress(false);
    params.set_print_realtime(false);
    params.set_print_timestamps(false);
    params.set_translate(false);
    params.set_no_timestamps(false);

    match requested_language {
        Some(lang) if !lang.trim().is_empty() && lang != "auto" => {
            params.set_language(Some(lang));
            params.set_detect_language(false);
        }
        _ => {
            params.set_language(None);
            params.set_detect_language(true);
        }
    }

    state
        .full(params, &samples)
        .map_err(|e| anyhow::anyhow!("whisper full() failed: {}", e))?;

    let mut segments = Vec::new();
    let segment_count = state.full_n_segments();

    for index in 0..segment_count {
        let Some(segment) = state.get_segment(index) else {
            continue;
        };

        let start_ms = (segment.start_timestamp().max(0) as u64) * 10;
        let end_ms = (segment.end_timestamp().max(0) as u64) * 10;
        let text = segment
            .to_str_lossy()
            .map_err(|e| anyhow::anyhow!("segment decode failed: {}", e))?
            .trim()
            .to_string();

        if text.is_empty() {
            continue;
        }

        segments.push(TranscriptSegment {
            start_ms,
            end_ms,
            text,
            speaker: None,
        });
    }

    if pyannote_enabled {
        let diarization_segments = run_pyannote_diarization(
            &wav_path,
            pyannote_python_bin,
            pyannote_script_path,
            pyannote_hf_token,
        )?;
        assign_speakers(&mut segments, &diarization_segments);
    }

    let lang = requested_language
        .filter(|lang| !lang.trim().is_empty() && *lang != "auto")
        .map(|s| s.to_string())
        .or_else(|| {
            let id = state.full_lang_id_from_state();
            whisper_rs::get_lang_str(id).map(|s| s.to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());

    let _ = std::fs::remove_file(&wav_path);

    Ok(Transcript {
        language: lang,
        segments,
    })
}

#[cfg(feature = "whisper-rs-backend")]
fn env_i32(name: &str, default: i32) -> i32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(default)
}

#[cfg(feature = "whisper-rs-backend")]
fn decode_to_wav_16khz_mono(input_path: &Path) -> Result<PathBuf> {
    let output_path = std::env::temp_dir().join(format!("subtitle_whisper_{}.wav", Uuid::new_v4()));
    let input_str = input_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("non utf-8 input path"))?;
    let output_str = output_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("non utf-8 output path"))?;

    let output = Command::new("ffmpeg")
        .args([
            "-i", input_str, "-vn", "-ac", "1", "-ar", "16000", "-f", "wav", "-y", output_str,
        ])
        .output()
        .map_err(|e| anyhow::anyhow!("failed to run ffmpeg for whisper: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!(
            "ffmpeg decode failed: {}",
            if stderr.trim().is_empty() {
                format!("exit code {}", output.status)
            } else {
                stderr.trim().to_string()
            }
        ));
    }

    if !output_path.exists() {
        return Err(anyhow::anyhow!("ffmpeg did not produce wav output"));
    }

    Ok(output_path)
}

#[cfg(feature = "whisper-rs-backend")]
fn read_wav_f32(path: &Path) -> Result<Vec<f32>> {
    let mut reader = hound::WavReader::open(path)
        .map_err(|e| anyhow::anyhow!("failed to open wav {}: {}", path.display(), e))?;
    let spec = reader.spec();

    if spec.channels != 1 {
        return Err(anyhow::anyhow!(
            "expected mono wav, got {} channels",
            spec.channels
        ));
    }

    let samples = match (spec.sample_format, spec.bits_per_sample) {
        (hound::SampleFormat::Float, 32) => reader
            .samples::<f32>()
            .map(|sample| sample.map_err(|e| anyhow::anyhow!("wav sample error: {}", e)))
            .collect::<Result<Vec<_>>>()?,
        (hound::SampleFormat::Int, 16) => reader
            .samples::<i16>()
            .map(|sample| {
                sample
                    .map(|value| value as f32 / i16::MAX as f32)
                    .map_err(|e| anyhow::anyhow!("wav sample error: {}", e))
            })
            .collect::<Result<Vec<_>>>()?,
        (hound::SampleFormat::Int, bits) if (24..=32).contains(&bits) => {
            let max_amplitude = ((1_i64 << (bits - 1)) - 1) as f32;
            reader
                .samples::<i32>()
                .map(|sample| {
                    sample
                        .map(|value| value as f32 / max_amplitude)
                        .map_err(|e| anyhow::anyhow!("wav sample error: {}", e))
                })
                .collect::<Result<Vec<_>>>()?
        }
        _ => {
            return Err(anyhow::anyhow!(
                "unsupported wav format: {:?} {} bits",
                spec.sample_format,
                spec.bits_per_sample
            ));
        }
    };

    if samples.is_empty() {
        return Err(anyhow::anyhow!("wav has no samples"));
    }

    Ok(samples)
}

#[cfg(feature = "whisper-rs-backend")]
fn run_pyannote_diarization(
    wav_path: &Path,
    python_bin: &str,
    script_path: &str,
    hf_token: Option<&str>,
) -> Result<Vec<DiarizationSegment>> {
    let output_json = std::env::temp_dir().join(format!("pyannote_{}.json", Uuid::new_v4()));

    let mut command = Command::new(python_bin);
    command
        .arg(script_path)
        .arg("--input")
        .arg(
            wav_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("non utf-8 wav path"))?,
        )
        .arg("--output")
        .arg(
            output_json
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("non utf-8 pyannote output path"))?,
        );

    if let Some(token) = hf_token {
        command.arg("--hf-token").arg(token);
    }

    let output = command
        .output()
        .map_err(|e| anyhow::anyhow!("failed to run pyannote script: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!(
            "pyannote diarization failed: {}",
            if stderr.trim().is_empty() {
                format!("exit code {}", output.status)
            } else {
                stderr.trim().to_string()
            }
        ));
    }

    let json_text = std::fs::read_to_string(&output_json)
        .map_err(|e| anyhow::anyhow!("failed to read pyannote output json: {}", e))?;
    let parsed: PyannoteResult = serde_json::from_str(&json_text)
        .map_err(|e| anyhow::anyhow!("failed to parse pyannote output json: {}", e))?;

    let _ = std::fs::remove_file(output_json);

    Ok(parsed.segments)
}

#[cfg(feature = "whisper-rs-backend")]
fn assign_speakers(
    asr_segments: &mut [TranscriptSegment],
    diarization_segments: &[DiarizationSegment],
) {
    for asr in asr_segments.iter_mut() {
        let mut best_speaker: Option<String> = None;
        let mut best_overlap: u64 = 0;

        for diar in diarization_segments {
            let overlap_start = asr.start_ms.max(diar.start_ms);
            let overlap_end = asr.end_ms.min(diar.end_ms);
            let overlap = overlap_end.saturating_sub(overlap_start);

            if overlap > best_overlap {
                best_overlap = overlap;
                best_speaker = Some(diar.speaker.clone());
            }
        }

        if best_overlap > 0 {
            asr.speaker = best_speaker;
        }
    }
}

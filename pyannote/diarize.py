#!/usr/bin/env python3
import argparse
import json
import os
import sys
import tempfile
import warnings
from pathlib import Path


# pyannote 3.x on newer torch versions can emit this warning during pooling on
# valid inputs. In our pipeline it is non-actionable noise and can surface as
# the only stderr output when the process exits unsuccessfully for unrelated
# reasons, so we silence it to keep failure reporting meaningful.
warnings.filterwarnings(
    "ignore",
    message=r"std\(\): degrees of freedom is <= 0\..*",
    category=UserWarning,
)
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    module=r"pyannote\.audio\.models\.blocks\.pooling",
)


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default

    try:
        return float(value)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default

    try:
        return int(value)
    except ValueError:
        return default


def _env_optional_int(name: str) -> int | None:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return None

    try:
        parsed = int(value)
    except ValueError:
        return None

    return parsed if parsed > 0 else None


def _pipeline_kwargs() -> dict:
    kwargs = {}
    for env_name, arg_name in (
        ("PYANNOTE_NUM_SPEAKERS", "num_speakers"),
        ("PYANNOTE_MIN_SPEAKERS", "min_speakers"),
        ("PYANNOTE_MAX_SPEAKERS", "max_speakers"),
    ):
        value = _env_optional_int(env_name)
        if value is not None:
            kwargs[arg_name] = value

    return kwargs


def _configured_max_speakers() -> int | None:
    num_speakers = _env_optional_int("PYANNOTE_NUM_SPEAKERS")
    max_speakers = _env_optional_int("PYANNOTE_MAX_SPEAKERS")

    if num_speakers is not None:
        return num_speakers
    return max_speakers


def _speaker_index(label: str) -> int | None:
    if not label.startswith("SPEAKER_"):
        return None

    try:
        return int(label.removeprefix("SPEAKER_"))
    except ValueError:
        return None


def _speaker_sort_key(label: str) -> tuple[int, str]:
    index = _speaker_index(label)
    return (index if index is not None else sys.maxsize, label)


def _build_pipeline(hf_token: str):
    import inspect
    import huggingface_hub
    import soundfile as sf
    import torch
    import torchaudio
    from torch.serialization import add_safe_globals
    from torch.torch_version import TorchVersion

    original_torch_load = torch.load

    torch_threads = max(1, _env_int("PYANNOTE_TORCH_THREADS", 1))
    torch_interop_threads = max(1, _env_int("PYANNOTE_TORCH_INTEROP_THREADS", 1))
    torch.set_num_threads(torch_threads)
    try:
        torch.set_num_interop_threads(torch_interop_threads)
    except RuntimeError:
        # torch only allows setting interop threads once per process.
        pass

    try:
        add_safe_globals([TorchVersion])
    except Exception:
        pass

    def torch_load_compat(*args, **kwargs):
        kwargs["weights_only"] = False
        return original_torch_load(*args, **kwargs)

    torch.load = torch_load_compat

    if not hasattr(torchaudio, "AudioMetaData"):
        class _AudioMetaData:
            sample_rate = 16000
            num_frames = 0
            num_channels = 1
            bits_per_sample = 16
            encoding = "PCM_S"

        torchaudio.AudioMetaData = _AudioMetaData

    if not hasattr(torchaudio, "list_audio_backends"):
        torchaudio.list_audio_backends = lambda: ["soundfile"]
    else:
        try:
            backends = torchaudio.list_audio_backends() or []
        except Exception:
            backends = []

        if not backends:
            torchaudio.list_audio_backends = lambda: ["soundfile"]
            backends = ["soundfile"]

        if hasattr(torchaudio, "set_audio_backend") and "soundfile" in backends:
            try:
                torchaudio.set_audio_backend("soundfile")
            except Exception:
                pass

    original_torchaudio_load = torchaudio.load

    def torchaudio_load_compat(uri, *args, **kwargs):
        try:
            return original_torchaudio_load(uri, *args, **kwargs)
        except ImportError as exc:
            if "TorchCodec" not in str(exc):
                raise

            frame_offset = kwargs.pop("frame_offset", 0)
            num_frames = kwargs.pop("num_frames", -1)
            channels_first = kwargs.pop("channels_first", True)
            kwargs.pop("backend", None)

            if len(args) >= 1:
                frame_offset = args[0]
            if len(args) >= 2:
                num_frames = args[1]
            if len(args) >= 4:
                channels_first = args[3]

            start = max(0, int(frame_offset or 0))
            frames = int(num_frames) if num_frames is not None else -1
            stop = start + frames if frames >= 0 else None

            waveform, sample_rate = sf.read(
                uri,
                dtype="float32",
                always_2d=True,
                start=start,
                stop=stop,
            )
            tensor = torch.from_numpy(waveform.T if channels_first else waveform)
            return tensor, int(sample_rate)

    torchaudio.load = torchaudio_load_compat

    original_torchaudio_info = getattr(torchaudio, "info", None)

    def torchaudio_info_compat(uri, *args, **kwargs):
        if original_torchaudio_info is not None:
            try:
                return original_torchaudio_info(uri, *args, **kwargs)
            except ImportError as exc:
                if "TorchCodec" not in str(exc):
                    raise

        info = sf.info(uri)
        meta = torchaudio.AudioMetaData()
        meta.sample_rate = int(info.samplerate)
        meta.num_frames = int(info.frames)
        meta.num_channels = int(info.channels)
        meta.bits_per_sample = 16
        meta.encoding = "PCM_S"
        return meta

    torchaudio.info = torchaudio_info_compat

    if "use_auth_token" not in inspect.signature(huggingface_hub.hf_hub_download).parameters:
        original_hf_hub_download = huggingface_hub.hf_hub_download

        def hf_hub_download_compat(*args, use_auth_token=None, **kwargs):
            if use_auth_token is not None and "token" not in kwargs:
                kwargs["token"] = use_auth_token
            return original_hf_hub_download(*args, **kwargs)

        huggingface_hub.hf_hub_download = hf_hub_download_compat

    from pyannote.audio import Pipeline

    model_id = os.environ.get("PYANNOTE_PIPELINE", "pyannote/speaker-diarization-3.1")
    return Pipeline.from_pretrained(model_id, use_auth_token=hf_token)


def _annotation_to_segments(annotation, offset_ms: int = 0) -> list[dict]:
    segments = []
    for turn, _, speaker in annotation.itertracks(yield_label=True):
        start_ms = offset_ms + int(round(turn.start * 1000.0))
        end_ms = offset_ms + int(round(turn.end * 1000.0))

        if end_ms <= start_ms:
            continue

        segments.append(
            {
                "speaker": str(speaker),
                "start_ms": start_ms,
                "end_ms": end_ms,
            }
        )

    return segments


def _merge_adjacent_segments(segments: list[dict], max_gap_ms: int = 250) -> list[dict]:
    if not segments:
        return []

    merged = [segments[0].copy()]
    for segment in segments[1:]:
        current = segment.copy()
        previous = merged[-1]

        if (
            current["speaker"] == previous["speaker"]
            and current["start_ms"] <= previous["end_ms"] + max_gap_ms
        ):
            previous["end_ms"] = max(previous["end_ms"], current["end_ms"])
            continue

        merged.append(current)

    return merged


def _existing_speaker_labels(segments: list[dict]) -> list[str]:
    labels = sorted(
        {segment["speaker"] for segment in segments},
        key=_speaker_sort_key,
    )
    return labels


def _nearest_existing_speaker(existing_segments: list[dict], chunk_start_ms: int) -> str | None:
    if not existing_segments:
        return None

    previous_segments = [
        segment
        for segment in existing_segments
        if segment["start_ms"] <= chunk_start_ms
    ]
    candidates = previous_segments if previous_segments else existing_segments

    nearest = min(
        candidates,
        key=lambda segment: min(
            abs(segment["start_ms"] - chunk_start_ms),
            abs(segment["end_ms"] - chunk_start_ms),
        ),
    )
    return nearest["speaker"]


def _fallback_existing_speaker(
    existing_labels: list[str],
    used_labels: set[str],
    preferred_label: str | None,
) -> str | None:
    if preferred_label is not None and preferred_label not in used_labels:
        return preferred_label

    for label in existing_labels:
        if label not in used_labels:
            return label

    return preferred_label or (existing_labels[0] if existing_labels else None)


def _assign_speakers_for_chunk(
    existing_segments: list[dict],
    chunk_segments: list[dict],
    chunk_start_ms: int,
    overlap_ms: int,
    next_speaker_index: int,
    max_speakers: int | None,
) -> tuple[dict[str, str], int]:
    local_labels = sorted({segment["speaker"] for segment in chunk_segments})
    if not local_labels:
        return {}, next_speaker_index

    existing_labels = _existing_speaker_labels(existing_segments)

    if not existing_segments or overlap_ms <= 0:
        mapping = {}
        for label in local_labels:
            if max_speakers is not None and next_speaker_index >= max_speakers:
                if existing_labels:
                    fallback = _fallback_existing_speaker(
                        existing_labels,
                        set(mapping.values()),
                        None,
                    )
                    mapping[label] = fallback or existing_labels[0]
                    continue

            mapping[label] = f"SPEAKER_{next_speaker_index:02d}"
            next_speaker_index += 1
            existing_labels.append(mapping[label])
        return mapping, next_speaker_index

    overlap_start_ms = chunk_start_ms
    overlap_end_ms = chunk_start_ms + overlap_ms

    candidates = []
    for chunk_segment in chunk_segments:
        for existing in existing_segments:
            overlap_start = max(chunk_segment["start_ms"], existing["start_ms"], overlap_start_ms)
            overlap_end = min(chunk_segment["end_ms"], existing["end_ms"], overlap_end_ms)
            overlap = overlap_end - overlap_start
            if overlap > 0:
                candidates.append(
                    (overlap, chunk_segment["speaker"], existing["speaker"])
                )

    mapping: dict[str, str] = {}
    used_global_labels: set[str] = set()
    for _, local_label, global_label in sorted(candidates, reverse=True):
        if local_label in mapping or global_label in used_global_labels:
            continue
        mapping[local_label] = global_label
        used_global_labels.add(global_label)

    for label in local_labels:
        if label in mapping:
            continue

        if max_speakers is not None and next_speaker_index >= max_speakers:
            nearest_speaker = _nearest_existing_speaker(existing_segments, chunk_start_ms)
            fallback = _fallback_existing_speaker(
                existing_labels,
                used_global_labels | set(mapping.values()),
                nearest_speaker,
            )
            if fallback is not None:
                mapping[label] = fallback
                continue

            if existing_labels:
                mapping[label] = existing_labels[0]
                continue

        mapping[label] = f"SPEAKER_{next_speaker_index:02d}"
        next_speaker_index += 1
        existing_labels.append(mapping[label])

    return mapping, next_speaker_index


def _limit_speaker_count(segments: list[dict], max_speakers: int | None) -> list[dict]:
    if max_speakers is None or max_speakers <= 0:
        return segments

    allowed = _existing_speaker_labels(segments)[:max_speakers]
    if len(allowed) < max_speakers:
        return segments

    allowed_set = set(allowed)
    last_allowed_speaker = allowed[0] if allowed else None
    remapped_segments = []

    for segment in segments:
        speaker = segment["speaker"]
        if speaker in allowed_set:
            last_allowed_speaker = speaker
            remapped_segments.append(segment)
            continue

        remapped = segment.copy()
        if last_allowed_speaker is None:
            last_allowed_speaker = allowed[0]
        remapped["speaker"] = last_allowed_speaker
        remapped_segments.append(remapped)

    return _merge_adjacent_segments(remapped_segments)


def _run_chunked_diarization(pipeline, input_path: Path) -> list[dict]:
    import soundfile as sf

    chunk_seconds = max(1.0, _env_float("PYANNOTE_CHUNK_SECONDS", 60.0))
    overlap_seconds = max(0.0, _env_float("PYANNOTE_CHUNK_OVERLAP_SECONDS", 10.0))
    if overlap_seconds >= chunk_seconds:
        overlap_seconds = max(0.0, chunk_seconds / 2.0)

    pipeline_kwargs = _pipeline_kwargs()
    max_speakers = _configured_max_speakers()
    info = sf.info(str(input_path))
    total_duration = float(info.duration)
    if total_duration <= chunk_seconds:
        diarization = pipeline(str(input_path), **pipeline_kwargs)
        segments = _merge_adjacent_segments(_annotation_to_segments(diarization))
        return _limit_speaker_count(segments, max_speakers)

    step_seconds = max(1.0, chunk_seconds - overlap_seconds)
    overlap_ms = int(round(overlap_seconds * 1000.0))

    combined_segments: list[dict] = []
    next_speaker_index = 0

    with tempfile.TemporaryDirectory(prefix="pyannote_chunks_") as temp_dir:
        chunk_start = 0.0
        chunk_index = 0

        while chunk_start < total_duration:
            chunk_end = min(total_duration, chunk_start + chunk_seconds)
            start_frame = int(round(chunk_start * info.samplerate))
            end_frame = int(round(chunk_end * info.samplerate))

            waveform, sample_rate = sf.read(
                str(input_path),
                dtype="float32",
                always_2d=True,
                start=start_frame,
                stop=end_frame,
            )

            chunk_path = Path(temp_dir) / f"chunk_{chunk_index:04d}.wav"
            sf.write(str(chunk_path), waveform, sample_rate)

            diarization = pipeline(str(chunk_path), **pipeline_kwargs)
            raw_segments = _annotation_to_segments(
                diarization,
                offset_ms=int(round(chunk_start * 1000.0)),
            )

            mapping, next_speaker_index = _assign_speakers_for_chunk(
                combined_segments,
                raw_segments,
                int(round(chunk_start * 1000.0)),
                overlap_ms,
                next_speaker_index,
                max_speakers,
            )

            dedupe_before_ms = int(round((chunk_start + overlap_seconds) * 1000.0))
            for segment in raw_segments:
                start_ms = segment["start_ms"]
                end_ms = segment["end_ms"]

                if chunk_index > 0:
                    if end_ms <= dedupe_before_ms:
                        continue
                    if start_ms < dedupe_before_ms:
                        start_ms = dedupe_before_ms

                if end_ms <= start_ms:
                    continue

                combined_segments.append(
                    {
                        "speaker": mapping[segment["speaker"]],
                        "start_ms": start_ms,
                        "end_ms": end_ms,
                    }
                )

            if chunk_end >= total_duration:
                break

            chunk_start += step_seconds
            chunk_index += 1

    combined_segments.sort(key=lambda segment: (segment["start_ms"], segment["end_ms"]))
    combined_segments = _merge_adjacent_segments(combined_segments)
    return _limit_speaker_count(combined_segments, max_speakers)


def main() -> int:
    if sys.version_info >= (3, 12):
        raise RuntimeError(
            "pyannote diarization requires Python 3.11 (3.12+ is not supported in this setup)"
        )

    parser = argparse.ArgumentParser(description="Run pyannote diarization and export JSON segments")
    parser.add_argument("--input", required=True, help="Path to local audio file")
    parser.add_argument("--output", required=True, help="Path to output JSON")
    parser.add_argument("--hf-token", required=False, help="HuggingFace token")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise FileNotFoundError(f"Input audio not found: {input_path}")

    hf_token = args.hf_token or os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_TOKEN")
    if not hf_token:
        raise RuntimeError("HF token is required: pass --hf-token or set HF_TOKEN/HUGGINGFACE_TOKEN")

    pipeline = _build_pipeline(hf_token)
    segments = _run_chunked_diarization(pipeline, input_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps({"segments": segments}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

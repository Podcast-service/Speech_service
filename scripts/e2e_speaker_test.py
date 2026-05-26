#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import boto3


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_MEDIA_API_DIR = SCRIPT_DIR.parent.parent / "Media_upload_service"


def run(
    cmd: list[str],
    *,
    check: bool = True,
    input_text: str | None = None,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        input=input_text.encode("utf-8") if input_text is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=False,
        cwd=str(cwd) if cwd is not None else None,
        check=check,
    )


def ensure_binary(path: str) -> None:
    result = subprocess.run(["zsh", "-lc", f"command -v {path} >/dev/null 2>&1"])
    if result.returncode != 0:
        raise RuntimeError(f"Required binary not found: {path}")


def ensure_docker_available() -> None:
    result = run(["docker", "info"], check=False)
    if result.returncode != 0:
        output = result.stdout.decode("utf-8", errors="ignore").strip()
        hint = "Docker daemon is not available. Start Docker Desktop and retry."
        if output:
            raise RuntimeError(f"{hint}\n\nDocker output:\n{output}")
        raise RuntimeError(hint)


def media_api_dir() -> Path:
    configured = os.getenv("MEDIA_API_DIR")
    if configured:
        path = Path(configured).expanduser().resolve()
    else:
        path = DEFAULT_MEDIA_API_DIR

    if not (path / "docker-compose.yml").exists():
        raise RuntimeError(
            f"Media API compose directory is invalid: {path} (expected docker-compose.yml)"
        )

    return path


def compose_cmd(*args: str) -> tuple[list[str], Path]:
    api_dir = media_api_dir()
    return (["docker", "compose", *args], api_dir)


def ensure_local_services() -> None:
    ensure_docker_available()

    cmd, cwd = compose_cmd("ps", "--services", "--status", "running")
    result = run(cmd, check=False, cwd=cwd)
    if result.returncode != 0:
        output = result.stdout.decode("utf-8", errors="ignore").strip()
        raise RuntimeError(
            "Failed to query Docker Compose services. Make sure compose project is up."
            + (f"\n\nCompose output:\n{output}" if output else "")
        )

    running = set(result.stdout.decode("utf-8", errors="ignore").splitlines())
    required_services = ["kafka", "media-worker", "media-subtitle-worker"]
    missing = [service for service in required_services if service not in running]
    if missing:
        raise RuntimeError(
            "Required compose services are not running: "
            + ", ".join(missing)
            + f" (project dir: {cwd})"
        )


def make_audio(target_wav: Path) -> None:
    run([
        "say",
        "-v",
        "Alex",
        "Привет, это первый спикер. Проверяем распределение ролей.",
        "-o",
        "/tmp/e2e_spk1.aiff",
    ])
    run([
        "say",
        "-v",
        "Victoria",
        "Здравствуйте, это второй спикер. Проверяем diarization.",
        "-o",
        "/tmp/e2e_spk2.aiff",
    ])
    run([
        "ffmpeg",
        "-y",
        "-v",
        "error",
        "-i",
        "/tmp/e2e_spk1.aiff",
        "-i",
        "/tmp/e2e_spk2.aiff",
        "-filter_complex",
        "[0:a][1:a]concat=n=2:v=0:a=1[out]",
        "-map",
        "[out]",
        str(target_wav),
    ])


def prepare_input_audio(source_path: Path, target_wav: Path, max_seconds: int | None) -> None:
    if not source_path.exists():
        raise RuntimeError(f"Input file not found: {source_path}")

    cmd = [
        "ffmpeg",
        "-y",
        "-v",
        "error",
        "-i",
        str(source_path),
    ]

    if max_seconds is not None and max_seconds > 0:
        cmd += ["-t", str(max_seconds)]

    cmd += [
        "-ac",
        "1",
        "-ar",
        "16000",
        str(target_wav),
    ]

    run(cmd)


def upload_source_audio(local_wav: Path, bucket: str, file_id: str) -> tuple[str, str]:
    object_key = f"media/{file_id}/source.wav"
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "https://s3.twcstorage.ru"),
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
        region_name=os.getenv("S3_REGION", "ru-1"),
    )
    s3.upload_file(str(local_wav), bucket, object_key)
    return object_key, f"s3://{bucket}/{object_key}"


def produce(topic: str, payload: dict) -> None:
    cmd, cwd = compose_cmd(
        "exec",
        "-T",
        "kafka",
        "kafka-console-producer",
        "--bootstrap-server",
        "kafka:9092",
        "--topic",
        topic,
    )
    run(
        cmd,
        input_text=json.dumps(payload) + "\n",
        cwd=cwd,
    )


def consume_topic(topic: str, timeout_ms: int = 5000) -> list[dict]:
    cmd, cwd = compose_cmd(
        "exec",
        "-T",
        "kafka",
        "kafka-console-consumer",
        "--bootstrap-server",
        "kafka:9092",
        "--topic",
        topic,
        "--from-beginning",
        "--timeout-ms",
        str(timeout_ms),
    )
    result = run(
        cmd,
        check=False,
        cwd=cwd,
    )
    events: list[dict] = []
    for line in result.stdout.decode("utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            events.append(json.loads(line))
        except Exception:
            continue
    return events


def fetch_text(bucket: str, key: str) -> str:
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "https://s3.twcstorage.ru"),
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
        region_name=os.getenv("S3_REGION", "ru-1"),
    )
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8", errors="ignore")


def list_objects(bucket: str, prefix: str) -> list[str]:
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "https://s3.twcstorage.ru"),
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
        region_name=os.getenv("S3_REGION", "ru-1"),
    )
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item.get("Key")
            if key:
                keys.append(key)
    return keys


def wait_for_event(
    topic: str,
    file_id: str,
    predicate,
    timeout_seconds: int,
    label: str,
    progress_interval_seconds: int = 30,
    error_predicate=None,
) -> dict | None:
    deadline = time.time() + timeout_seconds if timeout_seconds > 0 else None
    started_at = time.time()
    last_progress_at = 0.0

    while True:
        now = time.time()
        if deadline is not None and now >= deadline:
            return None

        if last_progress_at == 0.0 or (now - last_progress_at) >= progress_interval_seconds:
            if deadline is None:
                elapsed = int(now - started_at)
                print(f"waiting {label}... ({elapsed}s elapsed)")
            else:
                remaining = max(0, int(deadline - now))
                print(f"waiting {label}... ({remaining}s left)")
            last_progress_at = now

        events = consume_topic(topic, timeout_ms=4000)
        for event in events:
            if event.get("file_id") != file_id:
                continue
            if predicate(event):
                print(f"received {label}")
                return event

            if error_predicate is not None and error_predicate(event):
                raise RuntimeError(f"{label} failed: {event}")

        time.sleep(1)


def main() -> int:
    parser = argparse.ArgumentParser(description="E2E test: speaker diarization subtitles")
    parser.add_argument("--wait-upload-seconds", type=int, default=2)
    parser.add_argument("--wait-convert-seconds", type=int, default=0)
    parser.add_argument("--wait-subtitle-seconds", type=int, default=0)
    parser.add_argument(
        "--progress-interval-seconds",
        type=int,
        default=30,
        help="How often to print waiting progress (seconds)",
    )
    parser.add_argument("--input-file", type=Path, default=None, help="Path to local podcast/audio file")
    parser.add_argument(
        "--max-seconds",
        type=int,
        default=45,
        help="Trim input audio to N seconds for faster E2E (0 disables trim)",
    )
    args = parser.parse_args()

    if not os.getenv("PYANNOTE_HF_TOKEN"):
        raise RuntimeError("Set PYANNOTE_HF_TOKEN before running this E2E test")
    if not os.getenv("S3_ACCESS_KEY_ID") or not os.getenv("S3_SECRET_ACCESS_KEY"):
        raise RuntimeError("Set S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY before running this E2E test")

    required_binaries = ["docker", "ffmpeg"]
    if args.input_file is None:
        required_binaries.append("say")

    for binary in required_binaries:
        ensure_binary(binary)

    ensure_local_services()

    file_id = str(uuid.uuid4())
    podcast_id = f"podcast_{uuid.uuid4()}"
    wav_path = Path(f"/tmp/e2e_input_{file_id}.wav")

    if args.input_file is not None:
        trim_seconds = args.max_seconds if args.max_seconds > 0 else None
        prepare_input_audio(args.input_file, wav_path, trim_seconds)
        print(
            f"prepared input audio from {args.input_file}"
            + (f" (trim={trim_seconds}s)" if trim_seconds is not None else " (no trim)")
        )
    else:
        make_audio(wav_path)

    storage_bucket = os.getenv("S3_BUCKET", "4c5face5-544c-4bc2-a2e0-57a24d243af3")
    _, audio_url_file = upload_source_audio(wav_path, storage_bucket, file_id)
    print(f"uploaded source audio to {audio_url_file}")

    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    upload_event = {
        "event": "uploaded",
        "file_id": file_id,
        "podcast_id": podcast_id,
        "author_id": "e2e-speaker-test",
        "need_subtitle": True,
        "audio_size_file": wav_path.stat().st_size,
        "original_format": "audio/wav",
        "audio_url_file": audio_url_file,
        "uploaded_at": now,
    }
    produce("media", upload_event)
    print(f"sent media.uploaded file_id={file_id}, podcast_id={podcast_id}")

    time.sleep(args.wait_upload_seconds)

    converted_event = wait_for_event(
        "media.worker",
        file_id,
        lambda e: "path" in e and "converted_at" in e,
        args.wait_convert_seconds,
        "media.worker.converted",
        args.progress_interval_seconds,
        error_predicate=lambda e: e.get("stage") == "conversion",
    )
    if converted_event is None:
        raise RuntimeError("No media.worker.converted event found")

    ready = wait_for_event(
        "media.subtitle",
        file_id,
        lambda e: "vtt_object_key" in e,
        args.wait_subtitle_seconds,
        "media.subtitle.ready",
        args.progress_interval_seconds,
        error_predicate=lambda e: e.get("stage") == "transcription",
    )
    if ready is None:
        raise RuntimeError("No media.subtitle.ready event found")

    vtt = fetch_text(ready["bucket"], ready["vtt_object_key"])
    print("--- subtitles preview ---")
    print("\n".join(vtt.splitlines()[:20]))

    if "SPEAKER_" not in vtt:
        raise RuntimeError("No speaker labels found in VTT output")

    print("OK: speaker labels detected in subtitles")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# Speech_service

`media_subtitle_worker` слушает Kafka topic `media.subtitle.request`, скачивает исходный аудиообъект из S3-compatible storage, строит `VTT` и `SRT`, загружает результат обратно в S3. Публичный результат публикуется в `media.subtitle.ready`, ошибки — в `media.subtitle.error`, связка HLS↔субтитры — в `media.worker.events`, а backend-результат для `podcast_core` — в `media.subtitle`.

## S3

Сервис использует только S3-compatible storage. Обязательные переменные:

```env
S3_ENDPOINT_URL=https://s3.twcstorage.ru
S3_BUCKET=4c5face5-544c-4bc2-a2e0-57a24d243af3
SUBTITLE_BUCKET=4c5face5-544c-4bc2-a2e0-57a24d243af3
S3_REGION=ru-1
S3_ACCESS_KEY_ID=<secret>
S3_SECRET_ACCESS_KEY=<secret>
```

Секреты кладите в локальный `.env` рядом с `compose.yml`; `.env` игнорируется git. В репозитории оставлен только безопасный `.env.example`.

`S3_CREATE_BUCKET=true` можно использовать только если окружение должно создавать бакет автоматически. Для managed S3 бакет обычно уже создан инфраструктурой.

Число одновременно работающих pipeline на один экземпляр сервиса задаётся через
`SUBTITLE_MAX_CONCURRENT_JOBS`. По умолчанию запускается один pipeline; остальные
полученные запросы ожидают свободный слот.

## Запуск

Smoke-режим с `mock` transcriber:

```bash
docker compose up -d --build kafka kafka-init media-subtitle-worker
```

Для реальной транскрибации дополнительно нужны:

```env
TRANSCRIBER_BACKEND=whisper-rs
WHISPER_MODEL_PATH=/models/ggml-medium.bin
PYANNOTE_ENABLED=true
PYANNOTE_HF_TOKEN=<secret>
```

### Запуск с NVIDIA GPU

На сервере должны быть установлены NVIDIA driver и NVIDIA Container Toolkit.
CUDA-сборка включает GPU backend для `whisper-rs`, устанавливает CUDA wheel для
PyTorch и автоматически переносит pyannote pipeline на GPU, если CUDA доступна.

```env
WHISPER_MODELS_DIR=/path/to/models
WHISPER_MODEL_PATH=/models/ggml-medium.bin
PYANNOTE_HF_TOKEN=<secret>
```

```bash
docker compose -f compose.yml -f compose.gpu.yml up -d --build \
  kafka kafka-init media-subtitle-worker
```

`PYANNOTE_DEVICE=auto` используется по умолчанию. Для диагностики можно явно
задать `PYANNOTE_DEVICE=cpu` или `PYANNOTE_DEVICE=cuda`. Выбранное устройство
попадает в лог `pyannote: selected device: ...`. Для Whisper лог
`creating whisper-rs context: use_gpu=..., gpu_device=...` показывает запрос
на использование GPU.

## Kafka Contract

Сервис читает:

- `media.subtitle.request`, consumer group `media-subtitle-worker-service`.

Сервис публикует:

- `media.subtitle.ready` — публичный результат генерации;
- `media.subtitle` — backend-результат для `podcast_core`;
- `media.subtitle.error` — публичная ошибка генерации;
- `media.worker.events` — публичная связь HLS с VTT/SRT.

Полные JSON-контракты: [`docs/kafka-contract.md`](docs/kafka-contract.md).

## E2E

E2E-скрипт использует эти же `S3_*` переменные:

```bash
python scripts/e2e_speaker_test.py \
  --input-file scripts/bfda67ac521afab.mp3 \
  --max-seconds 45
```

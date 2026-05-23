# Media_subtitle_worker

Микросервис-воркер для генерации субтитров. Работает как Kafka consumer: читает событие `media.subtitle`, скачивает исходный аудиофайл из RustFS, строит `VTT` и `SRT`, загружает результат обратно в RustFS и публикует события `media.subtitle.ready`, `media.subtitle.error` и `media.worker`.

Сервис не открывает HTTP-порт. Нормальный запуск в Docker подразумевает, что рядом уже доступны:

- Kafka
- RustFS / S3-compatible storage
- Whisper model file, если используется `whisper-rs`

## Самодостаточность сервиса

Директория `Media_subtitle_worker` содержит всё, что нужно для сборки и запуска самого subtitle worker:

- `Cargo.toml` и `Cargo.lock` - Rust-зависимости сервиса;
- `Dockerfile` - standalone-сборка Rust binary с `whisper-rs-backend` и Python runtime для `pyannote`;
- `compose.yml` - локальный smoke-стенд с Kafka, RustFS и subtitle worker в `mock` backend;
- `pyannote/requirements.txt` и `pyannote/diarize.py` - Python-часть diarization;
- `scripts/e2e_speaker_test.py` - локальная e2e-проверка полного потока;
- `.gitignore` и `.dockerignore` - исключают build artifacts, virtualenv, Python cache, локальные env-файлы, editor files, логи и временные файлы.

Код `Media_api` и `Media_worker` не нужен для сборки subtitle worker. Контракт с остальными сервисами - Kafka JSON и объекты в S3-compatible storage. Для полноценного локального e2e рядом всё равно нужен общий стенд с Kafka, RustFS и upstream audio/HLS producer.

## Что делает сервис

- читает `media.subtitle.requested` из topic `media.subtitle`
- скачивает аудио по `source_bucket + source_object_key`
- транскрибирует аудио через выбранный backend
- сохраняет `subtitles.vtt` и `subtitles.srt`
- публикует результат обратно в Kafka

## Модели, дробление текста и формат результата

Pipeline состоит из двух независимых нейросетевых этапов:

1. ASR / speech-to-text: `whisper-rs` распознаёт речь и возвращает текстовые сегменты с таймкодами.
2. Speaker diarization: `pyannote` определяет интервалы, где говорит каждый голос, и возвращает speaker labels (`SPEAKER_00`, `SPEAKER_01` и т.д.).

После этого сервис сопоставляет оба результата: каждому Whisper-сегменту назначается speaker, чей pyannote-интервал имеет максимальное пересечение по времени. Финальный subtitle cue сейчас равен одному Whisper-сегменту. То есть итоговое дробление текста задаёт Whisper, а pyannote только добавляет speaker label.

Практический вывод:

- объединять соседние финальные subtitle cues одного speaker сейчас нельзя отдельной env-настройкой;
- если один speaker говорит долго, но Whisper разбил речь на 5 сегментов, в `VTT/SRT` будет 5 cues с одинаковым `SPEAKER_XX`;
- `PYANNOTE_CHUNK_SECONDS`, `PYANNOTE_CHUNK_OVERLAP_SECONDS` и `PYANNOTE_NUM_SPEAKERS` влияют на стабильность speaker labels, но не склеивают финальный текст;
- склейку формата "если speaker тот же и пауза меньше N мс, объединить текст" нужно добавлять отдельной постобработкой между `transcribe()` и `to_webvtt()/to_srt()`.

Рекомендуемые будущие настройки для такой постобработки:

- `SUBTITLE_MERGE_SAME_SPEAKER=true|false` - включить склейку соседних cues одного speaker.
- `SUBTITLE_MERGE_MAX_GAP_MS=1000` - максимальная пауза между соседними cues, которую ещё можно склеивать.
- `SUBTITLE_MERGE_MAX_DURATION_MS=8000` - максимальная длительность одного итогового cue после склейки.
- `SUBTITLE_MERGE_MAX_CHARS=160` - максимальный размер текста одного cue, чтобы не делать слишком длинные субтитры.

Пример текущего финального `VTT`:

```vtt
WEBVTT

00:00:01.000 --> 00:00:03.200
SPEAKER_00: Первая фраза.

00:00:03.300 --> 00:00:06.100
SPEAKER_00: Продолжение того же speaker.

00:00:06.500 --> 00:00:08.000
SPEAKER_01: Ответ другого speaker.
```

Пример желаемого результата после будущей склейки same-speaker cues:

```vtt
WEBVTT

00:00:01.000 --> 00:00:06.100
SPEAKER_00: Первая фраза. Продолжение того же speaker.

00:00:06.500 --> 00:00:08.000
SPEAKER_01: Ответ другого speaker.
```

Внутренние структуры данных:

- Whisper-сегмент в Rust: `TranscriptSegment { start_ms, end_ms, text, speaker }`.
- Итоговый transcript: `Transcript { language, segments }`.
- Pyannote JSON, который отдаёт Python-скрипт в Rust:

```json
{
  "segments": [
    {
      "speaker": "SPEAKER_00",
      "start_ms": 1000,
      "end_ms": 6100
    }
  ]
}
```

Сервис наружу не сохраняет отдельный JSON transcript. Сейчас стабильный внешний результат - это только `subtitles.vtt`, `subtitles.srt` и Kafka-события с путями к этим объектам.

## Docker image

В репозитории есть multi-stage [Dockerfile](/Users/drakowkq/work/uprpo/Media_subtitle_worker/Dockerfile), который:

- собирает Rust-бинарь с feature `whisper-rs-backend`
- устанавливает `ffmpeg`
- создаёт отдельный Python venv для `pyannote`

Dockerfile сервиса самодостаточен: для сборки нужны только файлы из директории `Media_subtitle_worker`. Сервис не зависит от кода `Media_api` или `Media_worker`; контракт между сервисами - Kafka JSON и объекты в S3.

`pyannote/requirements.txt` копируется отдельным слоем, поэтому изменение `pyannote/diarize.py` не должно заново скачивать тяжёлые `torch/pyannote` зависимости, пока сам `requirements.txt` не изменился.

Образ по умолчанию стартует со следующими значениями:

- `TRANSCRIBER_BACKEND=whisper-rs`
- `PYANNOTE_ENABLED=true`
- `PYANNOTE_PYTHON_BIN=/opt/pyannote-venv/bin/python`
- `PYANNOTE_SCRIPT_PATH=/app/pyannote/diarize.py`

Это значит, что для запуска "как есть" нужно дополнительно передать рабочие `KAFKA_BROKERS`, `RUSTFS_*`, `WHISPER_MODEL_PATH` и смонтировать файл модели внутрь контейнера.

## Обязательные переменные окружения

- `KAFKA_BROKERS`  
  В коде это обязательная переменная. Пример внутри Docker network: `kafka:9092`.
- `RUSTFS_REGION`
- `RUSTFS_ACCESS_KEY_ID`
- `RUSTFS_SECRET_ACCESS_KEY`
- `RUSTFS_ENDPOINT_URL`

Дополнительно для `TRANSCRIBER_BACKEND=whisper-rs`:

- `WHISPER_MODEL_PATH`  
  Путь внутри контейнера. Пример: `/models/ggml-medium.bin`.

Дополнительно для `PYANNOTE_ENABLED=true`:

- `PYANNOTE_HF_TOKEN`  
  HuggingFace token для `pyannote/speaker-diarization-3.1`.

## Необязательные переменные

- `SUBTITLE_BUCKET`  
  По умолчанию: `audio-hls`.
- `SUBTITLE_MAX_RETRIES`  
  По умолчанию: `3`.
- `TRANSCRIBER_BACKEND`  
  `mock` или `whisper-rs`. По умолчанию в коде: `mock`, но в Docker image переопределён в `whisper-rs`.
- `PYANNOTE_ENABLED`  
  `true` или `false`. По умолчанию в коде: `false`, но в Docker image переопределён в `true`.
- `PYANNOTE_PYTHON_BIN`
- `PYANNOTE_SCRIPT_PATH`
- `WHISPER_THREADS`  
  По умолчанию: `4`. Увеличение может ускорить CPU-инференс, если машине хватает ядер.
- `WHISPER_GREEDY_BEST_OF`  
  По умолчанию: `8`. Большее значение может дать чуть стабильнее распознавание, но работает медленнее.
- `PYANNOTE_CHUNK_SECONDS`  
  По умолчанию: `60`. Более длинные окна дают diarization больше контекста и обычно меньше дробят одного реального спикера на разные `SPEAKER_*`.
- `PYANNOTE_CHUNK_OVERLAP_SECONDS`  
  По умолчанию: `10`. Overlap нужен для склейки speaker labels между окнами.
- `PYANNOTE_NUM_SPEAKERS`, `PYANNOTE_MIN_SPEAKERS`, `PYANNOTE_MAX_SPEAKERS`  
  Подсказки pyannote по количеству спикеров. Если число реальных голосов известно, `PYANNOTE_NUM_SPEAKERS` часто заметно улучшает стабильность labels.
- `SPEAKER_CONTINUITY_MAX_GAP_MS`  
  Сейчас присутствует в compose-конфигурациях, но код `Media_subtitle_worker` эту переменную не читает. Она не влияет на финальное дробление, склейку текста или назначение speaker.

## Сборка образа

Из директории сервиса:

```bash
cd /Users/drakowkq/work/uprpo/Media_subtitle_worker
docker build -t media-subtitle-worker:local .
```

Из корня общего workspace можно собрать тот же образ так:

```bash
docker build -t media-subtitle-worker:local -f Media_subtitle_worker/Dockerfile Media_subtitle_worker
```

## Локальный smoke-стенд сервиса

Если нужно проверить только старт subtitle worker, Kafka consumer и подключение к RustFS, можно поднять локальный compose из директории сервиса. В этом режиме используется `TRANSCRIBER_BACKEND=mock`, поэтому не нужны `WHISPER_MODEL_PATH` и `PYANNOTE_HF_TOKEN`.

```bash
cd /Users/drakowkq/work/uprpo/Media_subtitle_worker
docker compose -f compose.yml up -d --build kafka kafka-init rustfs media-subtitle-worker
docker compose -f compose.yml logs --tail=100 media-subtitle-worker
```

Ожидаемые логи:

- `subtitle_worker started (kafka=..., bucket=...)`
- `Subtitle consumer started: topic='media.subtitle', group='media-subtitle-worker-service'`

## Сценарий 1. Продовый запуск в Docker

Это сценарий, когда сервис стартует как отдельный Docker-контейнер и подключается к уже существующим Kafka и RustFS.

Требования:

- заранее существуют Kafka и RustFS
- в Kafka создан topic `media.subtitle`
- внутрь контейнера смонтирован Whisper model file
- задан `PYANNOTE_HF_TOKEN`, если нужен diarization

Запуск standalone-контейнера:

```bash
docker run --rm \
  --name media-subtitle-worker \
  --network <docker-network> \
  -e KAFKA_BROKERS=kafka:9092 \
  -e SUBTITLE_BUCKET=audio-hls \
  -e SUBTITLE_MAX_RETRIES=3 \
  -e TRANSCRIBER_BACKEND=whisper-rs \
  -e WHISPER_MODEL_PATH=/models/ggml-medium.bin \
  -e PYANNOTE_ENABLED=true \
  -e PYANNOTE_HF_TOKEN=<your_hf_token> \
  -e RUSTFS_REGION=us-east-1 \
  -e RUSTFS_ACCESS_KEY_ID=rustfsadmin \
  -e RUSTFS_SECRET_ACCESS_KEY=rustfsadmin \
  -e RUSTFS_ENDPOINT_URL=http://rustfs:9000 \
  -v /Users/drakowkq/work/uprpo/models:/models:ro \
  media-subtitle-worker:local
```

Если topic ещё не создан:

```bash
docker exec <kafka-container> /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic media.subtitle \
  --partitions 1 \
  --replication-factor 1
```

Проверка старта:

```bash
docker logs --tail=100 media-subtitle-worker
```

Ожидаемые логи:

- `subtitle_worker started (kafka=..., bucket=...)`
- `Subtitle consumer started: topic='media.subtitle', group='media-subtitle-worker-service'`

## Сценарий 2. Тестовый запуск в Docker

Это сценарий для локальной разработки и QA, когда в Docker поднимается весь стек, а входной файл передаётся в e2e-скрипт.

В корне workspace есть общий `docker-compose.yml`, который поднимает Kafka, RustFS, `media-api`, `media-worker` и `media-subtitle-worker`. Это основной способ запускать полный локальный поток. В `Media_api/docker-compose.yml` есть похожий интеграционный compose, но он привязан к соседним директориям.

Требования:

- локально существует модель `../models/ggml-medium.bin`
- экспортирован `PYANNOTE_HF_TOKEN`, если нужен diarization

Подъём тестового стека:

```bash
cd /Users/drakowkq/work/uprpo
export PYANNOTE_HF_TOKEN=<your_hf_token>
docker compose up -d --build kafka kafka-init rustfs media-worker media-subtitle-worker
```

`kafka-init` в этом сценарии создаёт нужные topics, включая `media.subtitle`.

Проверка статуса:

```bash
docker compose ps media-subtitle-worker
docker compose logs --tail=100 media-subtitle-worker
```

Ожидаемые логи при успешном старте:

- `subtitle_worker started (kafka=..., bucket=...)`
- `Subtitle consumer started: topic='media.subtitle', group='media-subtitle-worker-service'`

## Smoke test без Whisper и pyannote

Если нужно быстро проверить, что контейнер стартует, подключается к Kafka и RustFS и не упирается в модель, переопределите backend на `mock`:

```bash
docker run --rm \
  --name media-subtitle-worker-smoke \
  --network media_api_default \
  -e KAFKA_BROKERS=kafka:9092 \
  -e TRANSCRIBER_BACKEND=mock \
  -e PYANNOTE_ENABLED=false \
  -e RUSTFS_REGION=us-east-1 \
  -e RUSTFS_ACCESS_KEY_ID=rustfsadmin \
  -e RUSTFS_SECRET_ACCESS_KEY=rustfsadmin \
  -e RUSTFS_ENDPOINT_URL=http://rustfs:9000 \
  media-subtitle-worker:local
```

Такой запуск не требует `WHISPER_MODEL_PATH` и `PYANNOTE_HF_TOKEN`.

Для проверки потребления можно отправить тестовое событие:

```bash
docker exec <kafka-container> sh -lc "printf '%s\n' '{\"file_id\":\"11111111-1111-4111-8111-111111111111\",\"source_bucket\":\"audio-hls\",\"source_object_key\":\"media/11111111-1111-4111-8111-111111111111/source.wav\",\"language\":\"ru\",\"requested_at\":\"2026-04-12T09:35:00Z\"}' | /opt/bitnami/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic media.subtitle"
```

Если файла в RustFS нет, это нормально для smoke test: в логах должно быть получение события и затем ошибка `NoSuchKey` на этапе download. Это подтверждает, что контейнер стартовал и реально читает Kafka.

## Передача файла в тестовом сценарии

Для e2e не нужно руками копировать файл в контейнер subtitle worker. Передавать нужно путь к локальному файлу в `scripts/e2e_speaker_test.py`, а сам скрипт:

- подготовит WAV из входного файла
- положит его в `media-worker`
- отправит `media.uploaded`
- дождётся `media.worker.converted`
- отправит `media.subtitle.requested`
- проверит итоговый `VTT`

Пример на реальном файле:

```bash
cd /Users/drakowkq/work/uprpo/Media_subtitle_worker
export PYANNOTE_HF_TOKEN=<your_hf_token>
/Users/drakowkq/work/uprpo/.venv/bin/python scripts/e2e_speaker_test.py \
  --input-file scripts/bfda67ac521afab.mp3 \
  --max-seconds 0 \
  --wait-convert-seconds 1800 \
  --wait-subtitle-seconds 3600 \
  --progress-interval-seconds 30
```

## Проверка обработки события

После старта сервиса можно проверить, что он слушает Kafka:

```bash
docker compose logs -f media-subtitle-worker
```

Для полноценной end-to-end проверки используйте [scripts/e2e_speaker_test.py](/Users/drakowkq/work/uprpo/Media_subtitle_worker/scripts/e2e_speaker_test.py). Скрипт ожидает, что уже запущены:

- `kafka`
- `rustfs`
- `media-worker`
- `media-subtitle-worker`

Запуск:

```bash
cd /Users/drakowkq/work/uprpo/Media_subtitle_worker
export PYANNOTE_HF_TOKEN=<your_hf_token>
/Users/drakowkq/work/uprpo/.venv/bin/python scripts/e2e_speaker_test.py
```

### E2E на реальном 19-минутном файле

В репозитории уже есть тестовый входной файл [scripts/bfda67ac521afab.mp3](/Users/drakowkq/work/uprpo/Media_subtitle_worker/scripts/bfda67ac521afab.mp3). Это рекомендуемый способ проверить полный цикл на реальном контенте.

Требования:

- подняты `kafka`, `rustfs`, `media-worker`, `media-subtitle-worker`
- `media-subtitle-worker` запущен с `TRANSCRIBER_BACKEND=whisper-rs`
- `PYANNOTE_ENABLED=true`
- экспортирован рабочий `PYANNOTE_HF_TOKEN`

Для локального CPU-стенда практичнее проверять e2e на `ggml-base.bin`. Конфигурация с `ggml-medium.bin` тоже рабочая, но полный прогон 19-минутного файла занимает существенно дольше.

Запуск полного e2e без обрезки аудио:

```bash
cd /Users/drakowkq/work/uprpo/Media_subtitle_worker
export PYANNOTE_HF_TOKEN=<your_hf_token>
/Users/drakowkq/work/uprpo/.venv/bin/python scripts/e2e_speaker_test.py \
  --input-file scripts/bfda67ac521afab.mp3 \
  --max-seconds 0 \
  --wait-convert-seconds 1800 \
  --wait-subtitle-seconds 3600 \
  --progress-interval-seconds 30
```

Если хотите именно быстрый локальный прогон, временно переопределите модель для `media-subtitle-worker` на:

```bash
WHISPER_MODEL_PATH=/models/ggml-base.bin
```

Что проверяет этот прогон:

- `media-worker` принимает исходный mp3 и публикует `media.worker.converted`
- `media-subtitle-worker` берёт реальный сегмент из RustFS
- `whisper-rs` строит транскрипт
- `pyannote` добавляет speaker diarization
- в итоговом `subtitles.vtt` присутствуют метки `SPEAKER_*`

Если `PYANNOTE_HF_TOKEN` не задан, полный цикл с diarization не выполнится: `pyannote/diarize.py` завершится ошибкой `HF token is required`.

## Event contracts

Входящее событие `media.subtitle`:

```json
{
  "file_id": "uuid",
  "source_bucket": "audio-hls",
  "source_object_key": "media/<uuid>/source.wav",
  "language": "ru",
  "requested_at": "2026-03-22T12:34:56Z"
}
```

Исходящее событие `media.subtitle.ready`:

```json
{
  "file_id": "uuid",
  "bucket": "audio-hls",
  "vtt_object_key": "media/<uuid>/subtitles.vtt",
  "srt_object_key": "media/<uuid>/subtitles.srt",
  "language": "ru",
  "segments": 42,
  "ready_at": "2026-03-22T12:35:56Z"
}
```

Исходящее событие `media.subtitle.error`:

```json
{
  "file_id": "uuid",
  "stage": "download | transcribe | upload",
  "error_message": "...",
  "timestamp": "2026-03-22T12:35:56Z"
}
```

Дополнительное событие `media.worker`:

```json
{
  "file_id": "uuid",
  "hls_path": "/media/<uuid>/master.m3u8",
  "subtitle_vtt_path": "/media/<uuid>/subtitles.vtt",
  "subtitle_srt_path": "/media/<uuid>/subtitles.srt",
  "language": "ru",
  "subtitle_ready_at": "2026-03-22T12:35:56Z"
}
```

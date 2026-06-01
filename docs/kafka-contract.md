# Kafka Contract

`Speech_service` читает запросы генерации субтитров, строит VTT и SRT и
публикует результаты в JSON.

| Направление | Topic | Kafka key | Назначение |
| --- | --- | --- | --- |
| Входящий | `media.subtitle.request` | `file_id` | Запрос генерации субтитров |
| Исходящий | `media.subtitle.ready` | `file_id` | Публичный результат генерации |
| Исходящий | `media.subtitle` | `file_id` | Backend-результат для `podcast_core` |
| Исходящий | `media.subtitle.error` | `file_id` | Публичная ошибка генерации |
| Исходящий | `media.worker.events` | `file_id` | Связь HLS с VTT/SRT |

Consumer group для `media.subtitle.request`: `media-subtitle-worker-service`.

## Topic `media.subtitle.request`

Событие публикует `Media_worker` после успешной HLS-конвертации при
`need_subtitle=true`.

```json
{
  "file_id": "11111111-1111-1111-1111-111111111111",
  "source_bucket": "4c5face5-544c-4bc2-a2e0-57a24d243af3",
  "source_object_key": "media/11111111-1111-1111-1111-111111111111/256k/seg_00000.m4s",
  "language": "ru",
  "num_speakers": 2,
  "requested_at": "2026-04-07T12:01:31Z"
}
```

`language` и `num_speakers` опциональны для consumer. Если `num_speakers`
не передан, transcriber определяет число спикеров без этой подсказки.

## Topic `media.subtitle.ready`

Публичный результат успешной генерации субтитров.

```json
{
  "file_id": "11111111-1111-1111-1111-111111111111",
  "bucket": "4c5face5-544c-4bc2-a2e0-57a24d243af3",
  "vtt_object_key": "media/11111111-1111-1111-1111-111111111111/subtitles.vtt",
  "srt_object_key": "media/11111111-1111-1111-1111-111111111111/subtitles.srt",
  "language": "ru",
  "segments": 42,
  "ready_at": "2026-04-07T12:05:00Z"
}
```

## Topic `media.subtitle`

Backend-результат успешной генерации для `podcast_core`.

```json
{
  "podcast_id": "11111111-1111-1111-1111-111111111111",
  "content": {
    "vtt_object_key": "https://s3.twcstorage.ru/4c5face5-544c-4bc2-a2e0-57a24d243af3/media/11111111-1111-1111-1111-111111111111/subtitles.vtt",
    "srt_object_key": "https://s3.twcstorage.ru/4c5face5-544c-4bc2-a2e0-57a24d243af3/media/11111111-1111-1111-1111-111111111111/subtitles.srt"
  },
  "ready_at": "2026-04-07T12:05:00Z"
}
```

Имена полей `vtt_object_key` и `srt_object_key` сохранены для совместимости,
но значениями являются публичные URL в формате
`https://s3.twcstorage.ru/<bucket>/<object_key>`.
Для доступа без авторизации `SUBTITLE_BUCKET` должен быть публичным.

## Topic `media.subtitle.error`

Публичная ошибка генерации. Сервис публикует ее после исчерпания попыток
транскрибации.

```json
{
  "file_id": "11111111-1111-1111-1111-111111111111",
  "stage": "transcription",
  "error_message": "transcribe audio failed",
  "timestamp": "2026-04-07T12:05:00Z"
}
```

## Topic `media.worker.events`

После успешной генерации speech-сервис дополнительно публикует связь HLS с
созданными файлами субтитров.

```json
{
  "file_id": "11111111-1111-1111-1111-111111111111",
  "hls_path": "/media/11111111-1111-1111-1111-111111111111/master.m3u8",
  "subtitle_vtt_path": "/media/11111111-1111-1111-1111-111111111111/subtitles.vtt",
  "subtitle_srt_path": "/media/11111111-1111-1111-1111-111111111111/subtitles.srt",
  "language": "ru",
  "subtitle_ready_at": "2026-04-07T12:05:00Z"
}
```

Это сообщение не содержит поля `event`: текущий контракт отличается от
событий `converted`, `error` и `deleted`, которые публикует `Media_worker` в
тот же топик.

## Notes

- Все timestamp-поля сериализуются как RFC 3339 UTC.
- `file_id` используется как `podcast_id` в backend-событии `media.subtitle`.

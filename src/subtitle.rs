use crate::transcriber::Transcript;

pub fn to_webvtt(transcript: &Transcript) -> String {
    let mut output = String::from("WEBVTT\n\n");

    for segment in &transcript.segments {
        let text = if let Some(speaker) = &segment.speaker {
            format!("{}: {}", speaker, segment.text)
        } else {
            segment.text.clone()
        };
        output.push_str(&format!(
            "{} --> {}\n{}\n\n",
            format_timestamp_vtt(segment.start_ms),
            format_timestamp_vtt(segment.end_ms),
            text.trim()
        ));
    }

    output
}

pub fn to_srt(transcript: &Transcript) -> String {
    let mut output = String::new();

    for (index, segment) in transcript.segments.iter().enumerate() {
        let text = if let Some(speaker) = &segment.speaker {
            format!("{}: {}", speaker, segment.text)
        } else {
            segment.text.clone()
        };
        output.push_str(&format!(
            "{}\n{} --> {}\n{}\n\n",
            index + 1,
            format_timestamp_srt(segment.start_ms),
            format_timestamp_srt(segment.end_ms),
            text.trim()
        ));
    }

    output
}

fn format_timestamp_vtt(ms: u64) -> String {
    let total_seconds = ms / 1000;
    let millis = ms % 1000;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    format!("{:02}:{:02}:{:02}.{:03}", hours, minutes, seconds, millis)
}

fn format_timestamp_srt(ms: u64) -> String {
    let total_seconds = ms / 1000;
    let millis = ms % 1000;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    format!("{:02}:{:02}:{:02},{:03}", hours, minutes, seconds, millis)
}

#[cfg(test)]
mod tests {
    use crate::transcriber::{Transcript, TranscriptSegment};

    use super::{to_srt, to_webvtt};

    #[test]
    fn renders_vtt_and_srt() {
        let transcript = Transcript {
            language: "ru".to_string(),
            segments: vec![TranscriptSegment {
                start_ms: 1000,
                end_ms: 2200,
                text: "Привет".to_string(),
                speaker: Some("SPEAKER_00".to_string()),
            }],
        };

        let vtt = to_webvtt(&transcript);
        let srt = to_srt(&transcript);

        assert!(vtt.contains("WEBVTT"));
        assert!(vtt.contains("00:00:01.000 --> 00:00:02.200"));
        assert!(vtt.contains("SPEAKER_00: Привет"));

        assert!(srt.contains("1"));
        assert!(srt.contains("00:00:01,000 --> 00:00:02,200"));
        assert!(srt.contains("SPEAKER_00: Привет"));
    }

    #[test]
    fn renders_mixed_segments_with_and_without_speakers() {
        let transcript = Transcript {
            language: "ru".to_string(),
            segments: vec![
                TranscriptSegment {
                    start_ms: 0,
                    end_ms: 1500,
                    text: "Реплика первого".to_string(),
                    speaker: Some("SPEAKER_00".to_string()),
                },
                TranscriptSegment {
                    start_ms: 1500,
                    end_ms: 3000,
                    text: "Реплика без роли".to_string(),
                    speaker: None,
                },
                TranscriptSegment {
                    start_ms: 3000,
                    end_ms: 4500,
                    text: "Реплика второго".to_string(),
                    speaker: Some("SPEAKER_01".to_string()),
                },
            ],
        };

        let vtt = to_webvtt(&transcript);
        let srt = to_srt(&transcript);

        assert!(vtt.contains("SPEAKER_00: Реплика первого"));
        assert!(vtt.contains("\nРеплика без роли\n"));
        assert!(vtt.contains("SPEAKER_01: Реплика второго"));

        assert!(srt.contains("SPEAKER_00: Реплика первого"));
        assert!(srt.contains("\nРеплика без роли\n"));
        assert!(srt.contains("SPEAKER_01: Реплика второго"));
    }
}

//! Application metrics exported to the OTEL collector via the global meter
//! (configured in [`crate::telemetry`]). Instruments are created lazily on
//! first use.
use std::sync::OnceLock;

use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{global, KeyValue};

pub struct Metrics {
    messages_received: Counter<u64>,
    messages_processed: Counter<u64>,
    processing_duration: Histogram<f64>,
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Returns the process-wide metrics, initialising them on first call.
pub fn metrics() -> &'static Metrics {
    METRICS.get_or_init(|| {
        let meter = global::meter("speech_service");
        Metrics {
            messages_received: meter
                .u64_counter("subtitle.messages.received")
                .with_description("Subtitle requests received from Kafka")
                .build(),
            messages_processed: meter
                .u64_counter("subtitle.messages.processed")
                .with_description("Subtitle pipeline runs completed")
                .build(),
            processing_duration: meter
                .f64_histogram("subtitle.processing.duration")
                .with_unit("s")
                .with_description("Subtitle pipeline duration in seconds")
                .build(),
        }
    })
}

impl Metrics {
    /// One Kafka message pulled from the topic (before validation).
    pub fn record_received(&self) {
        self.messages_received.add(1, &[]);
    }

    /// One pipeline run finished, labelled by outcome.
    pub fn record_processed(&self, status: &'static str) {
        self.messages_processed
            .add(1, &[KeyValue::new("status", status)]);
    }

    /// Wall-clock duration of a pipeline run, labelled by outcome.
    pub fn record_duration(&self, seconds: f64, status: &'static str) {
        self.processing_duration
            .record(seconds, &[KeyValue::new("status", status)]);
    }
}

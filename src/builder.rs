use crate::{recorder::Inner, MeterRecorder};
use metrics::{Key, SetRecorderError};
use metrics_util::{Handle, MetricKindMask, Recency, Registry, Tracked};
use opentelemetry::{
    global::{meter_provider, GlobalMeterProvider},
    metrics::MeterProvider,
};
use parking_lot::RwLock;
use quanta::Clock;
use std::{collections::HashMap, sync::Arc, time::Duration};

/// Builder for creating and installing an OpenTelemetry recorder/exporter.
pub struct OtelBuilder<M: MeterProvider> {
    idle_timeout: Option<Duration>,
    mask: MetricKindMask,
    clock: Clock,
    provider: M,
}

impl OtelBuilder<GlobalMeterProvider> {
    pub fn new_with_global() -> Self {
        OtelBuilder {
            idle_timeout: None,
            mask: MetricKindMask::ALL,
            clock: Clock::new(),
            provider: meter_provider(),
        }
    }
}

impl<M: MeterProvider> OtelBuilder<M> {
    pub fn new(meter_provider: M) -> Self {
        OtelBuilder {
            idle_timeout: None,
            mask: MetricKindMask::ALL,
            clock: Clock::new(),
            provider: meter_provider,
        }
    }

    pub fn timeout(mut self, mask: MetricKindMask, timeout: Option<Duration>) -> Self {
        self.idle_timeout = timeout;
        self.mask = if self.idle_timeout.is_none() {
            MetricKindMask::NONE
        } else {
            mask
        };
        self
    }

    pub fn clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    pub fn build(self) -> MeterRecorder {
        let meter = self
            .provider
            .meter("github.com/vibhavp/metrics-rs-opentelemetry", Some("0.1.0"));

        let inner = Arc::new(Inner {
            meter,
            recency: Recency::new(self.clock, self.mask, self.idle_timeout),
            registry: Registry::<Key, Handle, Tracked<Handle>>::tracked(),
            descriptions: RwLock::new(HashMap::new()),
            units: RwLock::new(HashMap::new()),
        });

        MeterRecorder { inner }
    }

    pub fn install(self) -> Result<(), SetRecorderError> {
        let recorder = self.build();
        metrics::set_boxed_recorder(Box::new(recorder))
    }
}

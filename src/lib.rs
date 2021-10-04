use core::slice::Iter;
use metrics::{GaugeValue, Key, Label, Recorder, Unit};
use metrics_util::{Handle, MetricKind, MetricKindMask, Recency, Registry, Tracked};
use opentelemetry::{
    metrics::{BatchObserverResult, Meter, MeterProvider, MetricsError, Unit as OtelUnit},
    KeyValue, Value,
};
use parking_lot::RwLock;
use quanta::Clock;
use std::{borrow::Cow, collections::HashMap, sync::Arc, time::Duration};

fn labels_to_keyvalue(labels: Iter<'_, Label>) -> Vec<KeyValue> {
    let mut kv = Vec::new();

    for label in labels {
        kv.push(KeyValue::new(
            label.key().to_string(),
            Value::String(Cow::Owned(label.value().to_string())),
        ))
    }

    kv
}

struct Inner {
    meter: Meter,
    recency: Recency<Key>,
    registry: Registry<Key, Handle, Tracked<Handle>>,
    descriptions: RwLock<HashMap<String, &'static str>>,
    units: RwLock<HashMap<String, Unit>>,
}

pub struct MeterRecorder {
    inner: Arc<Inner>,
}

impl MeterRecorder {
    pub fn new(
        provider: impl MeterProvider,
        idle_timeout: Option<Duration>,
    ) -> Result<MeterRecorder, MetricsError> {
        let meter = provider.meter("github.com/vibhavp/metrics-opentelemetry", Some("0.0.1"));
        let recorder = MeterRecorder {
            inner: Arc::new(Inner {
                meter,
                recency: Recency::new(Clock::new(), MetricKindMask::ALL, idle_timeout),
                registry: Registry::<Key, Handle, Tracked<Handle>>::tracked(),
                descriptions: RwLock::new(HashMap::new()),
                units: RwLock::new(HashMap::new()),
            }),
        };
        recorder.setup_batch_observer()?;

        Ok(recorder)
    }

    fn add_details_if_missing(
        &self,
        key: &Key,
        description: Option<&'static str>,
        unit: Option<Unit>,
    ) {
        if let Some(description) = description {
            let mut map = self.inner.descriptions.write();
            if !map.contains_key(key.name()) {
                map.insert(key.name().to_string(), description);
            }
        }

        if let Some(unit) = unit {
            let mut map = self.inner.units.write();
            if !map.contains_key(key.name()) {
                map.insert(key.name().to_string(), unit);
            }
        }
    }

    fn setup_batch_observer(&self) -> Result<(), MetricsError> {
        self.inner.meter.build_batch_observer(|batch| {
            let metrics = self.inner.registry.get_handles();
            let mut observations = Vec::with_capacity(metrics.len());

            for ((kind, key), (gen, handle)) in metrics.into_iter() {
                if !self
                    .inner
                    .recency
                    .should_store(kind, &key, gen, &self.inner.registry)
                {
                    continue;
                }

                let units = self.inner.units.read();
                let descriptions = self.inner.descriptions.read();

                match kind {
                    MetricKind::Counter => {
                        let mut builder = batch.u64_sum_observer(key.name());
                        if let Some(unit) = units.get(key.name()) {
                            builder = builder.with_unit(OtelUnit::new(unit.as_str().to_string()));
                        }
                        if let Some(desc) = descriptions.get(key.name()) {
                            builder = builder.with_description(desc.to_string());
                        }

                        observations.push((
                            vec![builder.try_init()?.observation(handle.read_counter())],
                            labels_to_keyvalue(key.labels()),
                        ));
                    }

                    MetricKind::Gauge => {
                        let mut builder = batch.f64_up_down_sum_observer(key.name());
                        if let Some(unit) = units.get(key.name()) {
                            builder = builder.with_unit(OtelUnit::new(unit.as_str().to_string()));
                        }
                        if let Some(desc) = descriptions.get(key.name()) {
                            builder = builder.with_description(desc.to_string());
                        }

                        observations.push((
                            vec![builder.try_init()?.observation(handle.read_gauge())],
                            labels_to_keyvalue(key.labels()),
                        ));
                    }

                    MetricKind::Histogram => {
                        let mut builder = batch.f64_value_observer(key.name());
                        if let Some(unit) = units.get(key.name()) {
                            builder = builder.with_unit(OtelUnit::new(unit.as_str().to_string()));
                        }
                        if let Some(desc) = descriptions.get(key.name()) {
                            builder = builder.with_description(desc.to_string());
                        }

                        let observer = builder.try_init()?;
                        let mut obs = Vec::new();
                        handle.read_histogram_with_clear(|values| {
                            for value in values.iter() {
                                obs.push(observer.observation(*value));
                            }
                        });

                        observations.push((obs, labels_to_keyvalue(key.labels())));
                    }
                }
            }

            Ok(move |result: BatchObserverResult| {
                for observation in observations.iter() {
                    result.observe(&observation.1, &observation.0);
                }
            })
        })
    }
}

impl Recorder for MeterRecorder {
    fn register_counter(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        self.add_details_if_missing(key, description, unit);
        self.inner
            .registry
            .op(MetricKind::Counter, key, |_| {}, Handle::counter);
    }

    fn register_gauge(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        self.add_details_if_missing(key, description, unit);
        self.inner
            .registry
            .op(MetricKind::Gauge, key, |_| {}, Handle::counter);
    }

    fn register_histogram(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        self.add_details_if_missing(key, description, unit);
        self.inner
            .registry
            .op(MetricKind::Histogram, key, |_| {}, Handle::counter);
    }

    fn increment_counter(&self, key: &Key, value: u64) {
        self.inner.registry.op(
            MetricKind::Counter,
            key,
            |h| h.increment_counter(value),
            Handle::counter,
        );
    }

    fn update_gauge(&self, key: &Key, value: GaugeValue) {
        self.inner.registry.op(
            MetricKind::Gauge,
            key,
            |h| h.update_gauge(value),
            Handle::counter,
        );
    }

    fn record_histogram(&self, key: &Key, value: f64) {
        self.inner.registry.op(
            MetricKind::Histogram,
            key,
            |h| h.record_histogram(value),
            Handle::counter,
        );
    }
}

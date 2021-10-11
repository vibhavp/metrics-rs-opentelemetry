use core::slice::Iter;
use metrics::{GaugeValue, Key, Label, Recorder, Unit};
use metrics_util::{Handle, MetricKind, MetricKindMask, Recency, Registry, Tracked};
use opentelemetry::{
    metrics::{Meter, MeterProvider, MetricsError},
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
        let meter = provider.meter("github.com/vibhavp/metrics-rs-opentelemetry", Some("0.1.0"));

        let recorder = MeterRecorder {
            inner: Arc::new(Inner {
                meter,
                recency: Recency::new(Clock::new(), MetricKindMask::ALL, idle_timeout),
                registry: Registry::<Key, Handle, Tracked<Handle>>::tracked(),
                descriptions: RwLock::new(HashMap::new()),
                units: RwLock::new(HashMap::new()),
            }),
        };

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
}

impl Recorder for MeterRecorder {
    fn register_counter(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        self.add_details_if_missing(key, description, unit);
        self.inner
            .registry
            .op(MetricKind::Counter, key, |_| {}, Handle::counter);

        let inner = self.inner.clone();
        let key_1 = key.clone();
        self.inner
            .meter
            .u64_sum_observer(key.name(), move |observer| {
                let handles = inner.registry.get_handles();
                let metric = handles.get(&(MetricKind::Counter, key_1.clone()));

                if let Some(metric) = metric {
                    if inner.recency.should_store(
                        MetricKind::Counter,
                        &key_1,
                        metric.0.clone(),
                        &inner.registry,
                    ) {
                        observer
                            .observe(metric.1.read_counter(), &labels_to_keyvalue(key_1.labels()));
                    }
                }
            })
            .init();
    }

    fn register_gauge(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        self.add_details_if_missing(key, description, unit);
        self.inner
            .registry
            .op(MetricKind::Gauge, key, |_| {}, Handle::counter);

        let inner = self.inner.clone();
        let key_1 = key.clone();
        self.inner
            .meter
            .f64_up_down_sum_observer(key.name(), move |observer| {
                let handles = inner.registry.get_handles();
                let metric = handles.get(&(MetricKind::Gauge, key_1.clone()));

                if let Some(metric) = metric {
                    if inner.recency.should_store(
                        MetricKind::Gauge,
                        &key_1,
                        metric.0.clone(),
                        &inner.registry,
                    ) {
                        observer
                            .observe(metric.1.read_gauge(), &labels_to_keyvalue(key_1.labels()));
                    }
                }
            });
    }

    fn register_histogram(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        self.add_details_if_missing(key, description, unit);
        self.inner
            .registry
            .op(MetricKind::Histogram, key, |_| {}, Handle::counter);

        let inner = self.inner.clone();
        let key_1 = key.clone();
        self.inner
            .meter
            .f64_value_observer(key.name(), move |observer| {
                let handles = inner.registry.get_handles();
                let metric = handles.get(&(MetricKind::Histogram, key_1.clone()));

                if let Some(metric) = metric {
                    if inner.recency.should_store(
                        MetricKind::Histogram,
                        &key_1,
                        metric.0.clone(),
                        &inner.registry,
                    ) {
                        let key_values = &labels_to_keyvalue(key_1.labels());
                        metric.1.read_histogram_with_clear(|values| {
                            for value in values.iter() {
                                observer.observe(*value, key_values);
                            }
                        });
                    }
                }
            });
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

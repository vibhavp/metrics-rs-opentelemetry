use core::slice::Iter;
use metrics::{GaugeValue, Key, Label, Recorder, Unit};
use metrics_util::{Handle, MetricKind, Recency, Registry, Tracked};
use opentelemetry::{metrics::Meter, KeyValue, Value};
use parking_lot::RwLock;
use std::{borrow::Cow, collections::HashMap, sync::Arc};

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

pub(crate) struct Inner {
    pub meter: Meter,
    pub recency: Recency<Key>,
    pub registry: Registry<Key, Handle, Tracked<Handle>>,
    pub descriptions: RwLock<HashMap<String, &'static str>>,
    pub units: RwLock<HashMap<String, Unit>>,
}

pub struct MeterRecorder {
    pub(crate) inner: Arc<Inner>,
}

impl MeterRecorder {
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

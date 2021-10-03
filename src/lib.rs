use core::slice::Iter;
use metrics::{GaugeValue, Key, Label, Recorder, Unit};
use metrics_util::{Handle, MetricKind, MetricKindMask, Recency, Registry, Tracked};
use opentelemetry::{
    metrics::{
        BatchObserverResult, Counter, Meter, MeterProvider, MetricsError, Unit as OtelUnit,
        UpDownCounter, ValueRecorder,
    },
    KeyValue, Value,
};
use quanta::Clock;
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

enum Metric {
    Counter(Counter<u64>),
    Gauge(UpDownCounter<f64>),
    Histogram(ValueRecorder<f64>),
}

pub struct MeterRecorder {
    meter: Meter,
    metrics: RwLock<HashMap<String, Metric>>,
}

impl Recorder for MeterRecorder {
    fn register_counter(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        let mut builder = self.meter.u64_counter(key.name());
        if let Some(unit) = unit {
            builder = builder.with_unit(OtelUnit::new(unit.as_str().to_string()))
        }

        if let Some(desc) = description {
            builder = builder.with_description(desc)
        }

        let mut map = self.metrics.write().unwrap();
        let counter = builder.init();

        counter.bind(&labels_to_keyvalue(key.labels()));
        map.insert(key.name().to_string(), Metric::Counter(counter));
    }

    fn register_gauge(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        let mut builder = self.meter.f64_up_down_counter(key.name());
        if let Some(unit) = unit {
            builder = builder.with_unit(OtelUnit::new(unit.as_str().to_string()))
        }

        if let Some(desc) = description {
            builder = builder.with_description(desc)
        }

        let mut map = self.metrics.write().unwrap();
        let gauge = builder.init();

        gauge.bind(&labels_to_keyvalue(key.labels()));
        map.insert(key.name().to_string(), Metric::Gauge(gauge));
    }

    fn register_histogram(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        let mut builder = self.meter.f64_value_recorder(key.name());
        if let Some(unit) = unit {
            builder = builder.with_unit(OtelUnit::new(unit.as_str().to_string()))
        }

        if let Some(desc) = description {
            builder = builder.with_description(desc)
        }

        let mut map = self.metrics.write().unwrap();
        let hist = builder.init();

        hist.bind(&labels_to_keyvalue(key.labels()));
        map.insert(key.name().to_string(), Metric::Histogram(hist));
    }

    fn increment_counter(&self, key: &Key, value: u64) {
        let map = self.metrics.read().unwrap();
        if let Some(Metric::Counter(counter)) = map.get(key.name()) {
            counter.add(value, &labels_to_keyvalue(key.labels()));
        }
    }

    fn update_gauge(&self, key: &Key, value: GaugeValue) {
        let map = self.metrics.read().unwrap();
        if let Some(Metric::Gauge(gauge)) = map.get(key.name()) {
            match value {
                GaugeValue::Increment(v) => gauge.add(v, &labels_to_keyvalue(key.labels())),
                GaugeValue::Decrement(v) => gauge.add(-v, &labels_to_keyvalue(key.labels())),
                GaugeValue::Absolute(_) => {}
            }
        }
    }

    fn record_histogram(&self, key: &Key, value: f64) {
        let map = self.metrics.read().unwrap();
        if let Some(Metric::Histogram(val_recorder)) = map.get(key.name()) {
            val_recorder.record(value, &labels_to_keyvalue(key.labels()));
        }
    }
}

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

pub struct AsyncMeterRecorder {
    inner: Arc<Inner>,
}

impl AsyncMeterRecorder {
    pub fn new(provider: impl MeterProvider, idle_timeout: Option<Duration>) -> AsyncMeterRecorder {
        let meter = provider.meter("github.com/vibhavp/metrics-opentelemetry", Some("0.0.1"));

        AsyncMeterRecorder {
            inner: Arc::new(Inner {
                meter,
                recency: Recency::new(Clock::new(), MetricKindMask::ALL, idle_timeout),
                registry: Registry::<Key, Handle, Tracked<Handle>>::tracked(),
                descriptions: HashMap::new(),
                units: HashMap::new(),
            }),
        }
    }

    fn setup_batch_observer(&self) -> Result<(), MetricsError> {
        self.inner.meter.build_batch_observer(|batch| {
            let metrics = self.inner.registry.get_handles();
            let observations = Vec::new();

            for ((kind, key), (gen, handle)) in metrics.into_iter() {
                if !self
                    .inner
                    .recency
                    .should_store(kind, &key, gen, &self.inner.registry)
                {
                    continue;
                }

                match kind {
                    MetricKind::Gauge(gauge) => batch.u64_sum_observer(key.name()).with_unit(),
                }
            }

            Ok(move |result| {})
        })
    }
}

impl Recorder for AsyncMeterRecorder {
    fn register_counter(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        self.inner
            .registry
            .op(MetricKind::Counter, key, |_| {}, Handle::counter);
        let inner = self.inner.clone();
        let key_1 = key.clone();

        let mut builder = self
            .inner
            .meter
            .u64_sum_observer(key.name(), move |observer| {
                let inner = inner.clone();
                if let Some(handle) = inner
                    .registry
                    .get_handles()
                    .get(&(MetricKind::Counter, key_1.clone()))
                {
                    if !inner.recency.should_store(
                        MetricKind::Counter,
                        &key_1,
                        handle.0.clone(),
                        &inner.registry,
                    ) {
                        return;
                    }

                    observer.observe(handle.1.read_counter(), &[]);
                }
                // if !inner.recency.should_store(MetricKind::Counter, &key_1, )
            });

        if let Some(unit) = unit {
            builder = builder.with_unit(OtelUnit::new(unit.as_str().to_string()));
        }

        if let Some(desc) = description {
            builder = builder.with_description(desc);
        }

        builder.init();
    }

    fn register_gauge(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        let inner = self.inner.clone();
        let key_1 = key.clone();

        let mut builder = self.inner.meter;
    }

    fn register_histogram(&self, key: &Key, unit: Option<Unit>, description: Option<&'static str>) {
        todo!()
    }

    fn increment_counter(&self, key: &Key, value: u64) {
        todo!()
    }

    fn update_gauge(&self, key: &Key, value: GaugeValue) {
        todo!()
    }

    fn record_histogram(&self, key: &Key, value: f64) {
        todo!()
    }
}

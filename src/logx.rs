use std::{collections::BTreeMap, str::FromStr};

use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, Layer,
};

use crate::{utils, LOG_SPAN_ID, LOG_TRACE_ID};

pub(crate) struct LayerX;

impl<S> Layer<S> for LayerX
where
    S: tracing::Subscriber,
    S: for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let level = utils::get_env("RUST_LOG")
            .and_then(|level| tracing::Level::from_str(level.as_str()).ok())
            .unwrap_or(tracing::Level::INFO);
        if event.metadata().level() > &level {
            return;
        }

        let now = chrono::Utc::now();
        let mut fields = BTreeMap::new();

        let mut visitor = JsonVisitor(&mut fields);
        event.record(&mut visitor);
        let mut output = serde_json::json!({
            "target": event.metadata().target(),
            "span": event.metadata().name(),
            "level": format!("{}", event.metadata().level()),
            "@timestamp": format!("{}", now.format("%Y-%m-%dT%H:%M:%S%.3fZ")),
        });

        match ctx.event_scope(event) {
            Some(scope) => {
                scope.from_root().for_each(|span| {
                    match span.extensions().get::<LayerXFieldStorage>() {
                        Some(storage) => {
                            fields.extend(storage.clone());
                        }
                        None => {}
                    };
                });
            }
            None => {}
        };

        match output.as_object_mut() {
            Some(obj) => {
                obj.extend(fields);
            }
            None => {}
        }

        println!("{}", output);
    }

    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        const INNERT_TRACE_ID: &str = "trace_id";
        const INNERT_SPAN_ID: &str = "span_id";
        ctx.span(id).into_iter().for_each(|span| {
            let mut fields = BTreeMap::new();
            let mut visitor = JsonVisitor(&mut fields);
            attrs.record(&mut visitor);
            let mut storage = LayerXFieldStorage::new();
            fields.get(INNERT_TRACE_ID).iter().for_each(|trace_id| {
                if trace_id.as_str().map(|s| !s.is_empty()).unwrap_or_default() {
                    storage.insert(LOG_TRACE_ID.to_string(), (*trace_id).clone());
                }
            });

            fields.get(INNERT_SPAN_ID).iter().for_each(|span_id| {
                if span_id.as_str().map(|s| !s.is_empty()).unwrap_or_default() {
                    storage.insert(LOG_SPAN_ID.to_string(), (*span_id).clone());
                }
            });
            span.extensions_mut().insert(storage);
        })
    }
}

type LayerXFieldStorage = BTreeMap<String, serde_json::Value>;

struct JsonVisitor<'a>(&'a mut BTreeMap<String, serde_json::Value>);

impl<'a> tracing::field::Visit for JsonVisitor<'a> {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.0.insert(
            field.name().to_string(),
            serde_json::json!(value.to_string()),
        );
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(
            field.name().to_string(),
            serde_json::json!(format!("{:?}", value)),
        );
    }
}

pub fn register_layerx() {
    tracing_subscriber::registry().with(LayerX).init();
}

#[cfg(test)]
mod tests {
    use std::sync::Once;

    use super::register_layerx;

    static ONCE: Once = Once::new();

    fn setup() {
        ONCE.call_once(|| {
            register_layerx();
        });
    }

    #[test]
    fn test_info() {
        setup();
        tracing::info_span!("X-Trace-Id", trace_id = "ssss").in_scope(|| {
            tracing::info!("hello world");
        });

        tracing::info!("hello world {}", "test");

        tracing::info!("hello world {name}", name = "test");
    }

    #[test]
    fn test_debug() {
        setup();
        tracing::info_span!("X-Trace-Id", trace_id = "sss").in_scope(|| {
            tracing::debug!("hello world");
        });

        tracing::debug!("hello world {}", "test");

        tracing::debug!("hello world {name}", name = "test");
    }

    #[test]
    fn test_enter() {
        setup();
        let span = tracing::info_span!("X-Trace-Id", trace_id = "sss");
        let _enter = span.enter();
        tracing::info!("hello world");
        tracing::info!("hello world {}", "test");
    }
}

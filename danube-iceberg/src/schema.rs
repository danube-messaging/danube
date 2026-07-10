//! Schema mapping — converts Danube `StreamMessage` values into Arrow `RecordBatch`.
//!
//! Two modes, matching the WarpStream / AutoMQ pattern:
//!
//! 1. **Registry** — The topic has a registered schema (JSON Schema, Avro, etc.)
//!    in Danube's Schema Registry. Messages carry `schema_id` / `schema_version`
//!    which the [`SchemaResolver`](crate::schema_resolver) uses to fetch the
//!    schema definition and convert it to a typed Arrow schema.
//!
//! 2. **Envelope** (fallback) — No schema is registered. Every message becomes
//!    a row with fixed metadata columns + an opaque binary payload. Analytics
//!    engines can query metadata but need to parse the payload themselves.

use arrow_array::builder::{BinaryBuilder, Int64Builder, StringBuilder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

/// How message payloads are encoded on the wire.
///
/// This determines how `messages_to_registry_batch` decodes each message's
/// payload bytes before feeding them to the Arrow batch builder.
#[derive(Debug, Clone)]
pub enum PayloadFormat {
    /// Payloads are JSON text — can be fed directly to `arrow_json::ReaderBuilder`.
    Json,
    /// Payloads are Avro binary — decoded directly to Arrow via
    /// `arrow_avro::reader::Decoder` using Confluent wire framing.
    ///
    /// Carries the raw Avro schema JSON needed by the Confluent decoder.
    Avro {
        /// Raw Avro schema JSON definition from the Danube Schema Registry.
        schema_json: String,
    },
}

/// The resolved schema mode for a topic.
#[derive(Debug, Clone)]
pub enum SchemaMode {
    /// Fixed envelope schema — message metadata + raw payload.
    Envelope,
    /// Schema from registry — payload fields promoted to typed Arrow columns.
    Registry {
        /// Schema ID from the Danube Schema Registry.
        schema_id: u64,
        /// Schema version from the Danube Schema Registry.
        schema_version: u32,
        /// Arrow schema with metadata columns + registry-defined payload columns.
        schema: Arc<Schema>,
        /// How the payload bytes are encoded (JSON text or Avro binary).
        payload_format: PayloadFormat,
    },
}

/// Build the fixed envelope Arrow schema.
///
/// Every Danube message can be represented as this schema — it's the
/// universal fallback when no schema is registered in the registry.
pub fn envelope_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("offset", DataType::Int64, false),
        Field::new("publish_time", DataType::Int64, false),
        Field::new("producer_name", DataType::Utf8, false),
        Field::new("routing_key", DataType::Utf8, true),
        Field::new("schema_id", DataType::Int64, true),
        Field::new("schema_version", DataType::Int64, true),
        Field::new("payload", DataType::Binary, false),
        Field::new("attributes_json", DataType::Utf8, true),
    ]))
}

// ============================================================================
// RecordBatch builders
// ============================================================================

/// Convert messages to a RecordBatch using the envelope schema.
pub fn messages_to_envelope_batch(
    messages: &[crate::segment_reader::DecodedMessage],
) -> anyhow::Result<RecordBatch> {
    let schema = envelope_schema();

    let mut offsets = Int64Builder::with_capacity(messages.len());
    let mut publish_times = Int64Builder::with_capacity(messages.len());
    let mut producers = StringBuilder::with_capacity(messages.len(), messages.len() * 32);
    let mut routing_keys = StringBuilder::with_capacity(messages.len(), messages.len() * 16);
    let mut schema_ids = Int64Builder::with_capacity(messages.len());
    let mut schema_versions = Int64Builder::with_capacity(messages.len());
    let mut payloads = BinaryBuilder::with_capacity(messages.len(), messages.len() * 256);
    let mut attributes = StringBuilder::with_capacity(messages.len(), messages.len() * 64);

    for dm in messages {
        let msg = &dm.message;
        offsets.append_value(dm.offset as i64);
        publish_times.append_value(msg.publish_time as i64);
        producers.append_value(&msg.producer_name);

        match &msg.routing_key {
            Some(k) => routing_keys.append_value(k),
            None => routing_keys.append_null(),
        }

        match msg.schema_id {
            Some(id) => schema_ids.append_value(id as i64),
            None => schema_ids.append_null(),
        }

        match msg.schema_version {
            Some(v) => schema_versions.append_value(v as i64),
            None => schema_versions.append_null(),
        }

        payloads.append_value(&msg.payload);

        if msg.attributes.is_empty() {
            attributes.append_null();
        } else {
            let json = serde_json::to_string(&msg.attributes)?;
            attributes.append_value(&json);
        }
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(offsets.finish()),
            Arc::new(publish_times.finish()),
            Arc::new(producers.finish()),
            Arc::new(routing_keys.finish()),
            Arc::new(schema_ids.finish()),
            Arc::new(schema_versions.finish()),
            Arc::new(payloads.finish()),
            Arc::new(attributes.finish()),
        ],
    )?;

    Ok(batch)
}

/// Convert messages to a RecordBatch using a registry-defined schema.
///
/// Message metadata (offset, publish_time, producer_name, routing_key) are
/// always included as the first 4 columns. The remaining columns come from
/// the registry schema.
///
/// The `payload_format` determines how payload bytes are decoded:
/// - **JSON**: fed directly to `arrow_json::ReaderBuilder`.
/// - **Avro**: each raw datum is wrapped in a Confluent wire-format frame
///   (`0x00` + 4-byte BE schema_id + Avro body) and decoded directly to Arrow
///   via `arrow_avro::reader::Decoder` — no intermediate JSON conversion.
pub fn messages_to_registry_batch(
    messages: &[crate::segment_reader::DecodedMessage],
    schema: &Arc<Schema>,
    payload_format: &PayloadFormat,
    schema_id: u64,
) -> anyhow::Result<RecordBatch> {
    let n = messages.len();

    // Metadata columns (built manually — not part of the payload)
    let mut offsets = Int64Builder::with_capacity(n);
    let mut publish_times = Int64Builder::with_capacity(n);
    let mut producers = StringBuilder::with_capacity(n, n * 32);
    let mut routing_keys = StringBuilder::with_capacity(n, n * 16);

    for dm in messages {
        let msg = &dm.message;
        offsets.append_value(dm.offset as i64);
        publish_times.append_value(msg.publish_time as i64);
        producers.append_value(&msg.producer_name);
        match &msg.routing_key {
            Some(k) => routing_keys.append_value(k),
            None => routing_keys.append_null(),
        }
    }

    // Build the payload-only schema (skip the 4 metadata columns)
    let payload_fields: Vec<arrow_schema::FieldRef> =
        schema.fields().iter().skip(4).cloned().collect();
    let payload_schema = Arc::new(Schema::new(payload_fields));

    // Build the payload RecordBatch according to the wire format
    let payload_batch = match payload_format {
        PayloadFormat::Json => decode_json_payloads(messages, &payload_schema)?,
        PayloadFormat::Avro { schema_json } => {
            decode_avro_payloads(messages, schema_json, schema_id)?
        }
    };

    // Merge: metadata columns + payload columns → final RecordBatch
    let mut arrays: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(offsets.finish()),
        Arc::new(publish_times.finish()),
        Arc::new(producers.finish()),
        Arc::new(routing_keys.finish()),
    ];

    for i in 0..payload_batch.num_columns() {
        arrays.push(payload_batch.column(i).clone());
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    Ok(batch)
}

/// Decode JSON text payloads into a payload-only RecordBatch.
///
/// Each message's payload is expected to be a JSON object. Invalid JSON
/// payloads emit an empty object `{}` so the row gets null values for all
/// fields rather than failing the entire batch.
fn decode_json_payloads(
    messages: &[crate::segment_reader::DecodedMessage],
    payload_schema: &Arc<Schema>,
) -> anyhow::Result<RecordBatch> {
    let n = messages.len();
    let mut json_lines: Vec<u8> = Vec::with_capacity(n * 256);

    for dm in messages {
        if serde_json::from_slice::<serde_json::Value>(&dm.message.payload).is_ok() {
            json_lines.extend_from_slice(&dm.message.payload);
        } else {
            json_lines.extend_from_slice(b"{}");
        }
        json_lines.push(b'\n');
    }

    let cursor = std::io::Cursor::new(json_lines);
    let mut reader = arrow_json::ReaderBuilder::new(payload_schema.clone())
        .with_batch_size(n)
        .build(cursor)?;

    let batch = reader
        .next()
        .transpose()?
        .unwrap_or_else(|| RecordBatch::new_empty(payload_schema.clone()));

    Ok(batch)
}

/// Decode Avro binary payloads directly to a payload-only RecordBatch.
///
/// Uses `arrow_avro::reader::Decoder` with Confluent wire framing. Each raw
/// Avro datum is prefixed with a 5-byte Confluent header:
/// `[0x00, schema_id_be[0..4]]` before being fed to the decoder.
///
/// This provides native Avro → Arrow type mapping without any intermediate
/// JSON serialization — `arrow-avro` handles all Avro types (records, arrays,
/// maps, unions, logical types) directly.
fn decode_avro_payloads(
    messages: &[crate::segment_reader::DecodedMessage],
    avro_schema_json: &str,
    schema_id: u64,
) -> anyhow::Result<RecordBatch> {

    // Register the Avro schema in a Confluent-style store keyed by numeric ID
    let mut store = arrow_avro::schema::SchemaStore::new_with_type(
        arrow_avro::schema::FingerprintAlgorithm::Id,
    );
    let avro_schema = arrow_avro::schema::AvroSchema::new(avro_schema_json.to_string());
    store.set(
        arrow_avro::schema::Fingerprint::Id(schema_id as u32),
        avro_schema,
    )?;

    // Build a Confluent-aware decoder
    let mut decoder = arrow_avro::reader::ReaderBuilder::new()
        .with_writer_schema_store(store)
        .with_batch_size(messages.len())
        .build_decoder()?;

    // Frame each raw Avro datum as a Confluent message and decode
    let confluent_id = (schema_id as u32).to_be_bytes();
    for dm in messages {
        let payload = &dm.message.payload;
        let mut frame = Vec::with_capacity(5 + payload.len());
        frame.push(0x00); // Confluent magic byte
        frame.extend_from_slice(&confluent_id);
        frame.extend_from_slice(payload);

        match decoder.decode(&frame) {
            Ok(consumed) => {
                if consumed != frame.len() {
                    tracing::warn!(
                        offset = dm.offset,
                        consumed,
                        frame_len = frame.len(),
                        "Avro decoder didn't consume entire frame"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    offset = dm.offset,
                    error = %e,
                    "failed to decode Avro payload — row may be missing"
                );
            }
        }
    }

    // Flush all decoded rows into a single RecordBatch
    let batch = decoder
        .flush()?
        .unwrap_or_else(|| RecordBatch::new_empty(Arc::new(Schema::empty())));

    Ok(batch)
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;

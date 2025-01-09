use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

use danube_core::proto::{schema::TypeSchema as ProtoTypeSchema, Schema as ProtoSchema};

// Define the enum with serde attributes for (de)serialization
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SchemaType {
    Bytes,
    String,
    Int64,
    Json(String), // JSON schema described by a string
}

impl SchemaType {
    pub fn from_str(s: &str) -> Option<SchemaType> {
        // Convert input string to lowercase for case-insensitive comparison
        // therefore allowed input could be: Bytes or bytes, String or string, etc..
        let s = s.to_lowercase();

        match s.as_str() {
            "bytes" => Some(SchemaType::Bytes),
            "string" => Some(SchemaType::String),
            "int64" => Some(SchemaType::Int64),
            "json" => Some(SchemaType::Json(String::new())),
            _ => None, // Return None for unrecognized strings
        }
    }
}

// Implement conversions from SchemaType to ProtoTypeSchema
impl From<SchemaType> for ProtoTypeSchema {
    fn from(schema_type: SchemaType) -> Self {
        match schema_type {
            SchemaType::Bytes => ProtoTypeSchema::Bytes,
            SchemaType::String => ProtoTypeSchema::String,
            SchemaType::Int64 => ProtoTypeSchema::Int64,
            SchemaType::Json(_) => ProtoTypeSchema::Json,
        }
    }
}

// Implement conversions from ProtoTypeSchema to SchemaType
impl From<ProtoTypeSchema> for SchemaType {
    fn from(proto_schema: ProtoTypeSchema) -> Self {
        match proto_schema {
            ProtoTypeSchema::Bytes => SchemaType::Bytes,
            ProtoTypeSchema::String => SchemaType::String,
            ProtoTypeSchema::Int64 => SchemaType::Int64,
            ProtoTypeSchema::Json => SchemaType::Json(String::new()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    name: String,
    schema_data: Option<Vec<u8>>,
    type_schema: SchemaType,
}

impl Schema {
    #[allow(dead_code)]
    pub fn new(name: String, type_schema: SchemaType) -> Self {
        let schema_data = match &type_schema {
            SchemaType::Json(schema) => Some(schema.as_bytes().to_vec()),
            _ => None,
        };
        Self {
            name,
            schema_data,
            type_schema,
        }
    }
}

// Implement a conversion method from ProtoSchema to Schema
impl From<ProtoSchema> for Schema {
    fn from(proto_schema: ProtoSchema) -> Self {
        let type_schema =
            ProtoTypeSchema::try_from(proto_schema.type_schema).expect("Invalid type schema");
        Schema {
            name: proto_schema.name,
            schema_data: Some(proto_schema.schema_data),
            type_schema: type_schema.into(),
        }
    }
}

// Implement a conversion method from Schema to ProtoSchema
impl From<Schema> for ProtoSchema {
    fn from(schema: Schema) -> Self {
        ProtoSchema {
            name: schema.name,
            schema_data: schema.schema_data.unwrap_or_default(),
            type_schema: ProtoTypeSchema::from(schema.type_schema).into(),
        }
    }
}

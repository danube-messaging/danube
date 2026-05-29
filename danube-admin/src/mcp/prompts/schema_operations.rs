//! Schema operational workflow prompts
//!
//! Contains the schema evolution prompt — a guided workflow for safely
//! evolving schemas with compatibility validation. Encodes the critical
//! distinction between subject-level compatibility and topic-level enforcement.

use rmcp::model::{
    GetPromptRequestParams, GetPromptResult, Prompt, PromptArgument, PromptMessage,
    PromptMessageRole,
};

/// Get schema operational prompt definitions
pub fn prompts() -> Vec<Prompt> {
    vec![Prompt::new(
        "manage_schema_evolution",
        Some("Guided workflow for safely evolving schemas with compatibility validation and subject/topic policy awareness"),
        Some(vec![
            PromptArgument::new("subject")
                .with_description("Schema subject name to evolve")
                .with_required(true),
            PromptArgument::new("schema_type")
                .with_description("Schema type: json_schema, avro, protobuf, string, or bytes (optional)")
                .with_required(false),
        ]),
    )
    .with_title("Manage Schema Evolution")]
}

/// Try to get a schema operational prompt by name
pub fn get_prompt(params: &GetPromptRequestParams) -> Option<GetPromptResult> {
    let args = params.arguments.as_ref();

    match params.name.as_str() {
        "manage_schema_evolution" => {
            let subject = args
                .and_then(|a| a.get("subject"))
                .and_then(|v| v.as_str())
                .unwrap_or("<SUBJECT>");
            let schema_type = args
                .and_then(|a| a.get("schema_type"))
                .and_then(|v| v.as_str());

            Some(
                GetPromptResult::new(vec![PromptMessage::new_text(
                    PromptMessageRole::User,
                    build_schema_evolution_prompt(subject, schema_type),
                )])
                .with_description(format!("Manage schema evolution for {}", subject)),
            )
        }
        _ => None,
    }
}

fn build_schema_evolution_prompt(subject: &str, schema_type: Option<&str>) -> String {
    let schema_type_guidance = schema_type
        .map(|t| format!(" (type: {})", t))
        .unwrap_or_default();

    let st = schema_type.unwrap_or("json_schema");

    format!(
        r#"I need to evolve the schema for subject "{subject}"{schema_type_guidance}.

Please guide me through this workflow:

## Important: Subject vs Topic Policies

Before starting, understand two separate policy layers:

- **Compatibility mode** is set at the **subject** level. It governs schema evolution — what changes are allowed between versions. Applies to ALL topics using this subject.
- **Validation policy** is set at the **topic** level (NONE / WARN / ENFORCE). It controls whether the broker validates incoming messages. Different topics can use the same subject with different enforcement levels.

## Step 1: Discover Existing Schemas
Use `list_subjects` to see all registered subjects and quickly check if "{subject}" already exists.

## Step 2: Review Current Schema
Use `get_schema` with subject="{subject}" to see the current schema definition, version, and type.

## Step 3: Check Version History
Use `list_schema_versions` with subject="{subject}" to see the full evolution history.

## Step 4: Check Compatibility Mode
Use `get_schema_compatibility_mode` with subject="{subject}" to see the current mode:

| Mode | Rule | Safe Changes |
|------|------|-------------|
| BACKWARD | New schema can read old data | Add optional fields, widen types |
| FORWARD | Old schema can read new data | Remove optional fields, narrow types |
| FULL | Both directions must hold | Only additive changes with defaults |
| NONE | No checks | Any change (development only) |

## Step 5: Check Compatibility BEFORE Registering
Use `check_schema_compatibility` to validate your new schema:
- subject: {subject}
- schema_definition: <new schema definition>
- schema_type: {st}

This validates both syntax and compatibility rules. Returns `is_compatible: true/false` with detailed error messages if incompatible.

**Do NOT skip this step** — it prevents rejected registrations.

## Step 6: Register New Version
If the compatibility check passes, use `register_schema` with:
- subject: {subject}
- schema_definition: <new schema definition>
- schema_type: {st}
- description: <what changed and why>

Note: The registry uses fingerprint deduplication — if you register a schema identical to an existing version, it returns the existing ID without creating a duplicate.

## Step 7: Verify Evolution
Use `get_schema` with subject="{subject}" to confirm:
- Version number incremented
- Schema definition stored correctly
- Schema ID assigned

## If Compatibility Check Fails

1. **Adjust the schema** to be compatible (preferred):
   - BACKWARD: add fields with defaults, make required fields optional
   - FORWARD: remove optional fields only
   - FULL: only additive changes with defaults
2. **Change compatibility mode** (admin-only, risky):
   - Use `set_schema_compatibility_mode` — but coordinate with ALL teams using this subject first
3. **Create a new subject** (for breaking changes):
   - Register under a new name (e.g., "{subject}-v2")
   - Update topic schema bindings with `configure_topic_schema`

## Safety Reminders
- **Always check compatibility before registering** — saves time and prevents surprises
- **Rollback**: use `delete_schema_version` if needed (may break consumers on that version)
- **Test first**: evolve schemas in dev/staging before production
- **Document**: use the description field to explain what changed in each version"#,
    )
}

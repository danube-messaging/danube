//! Schema operational workflow prompts
//!
//! Contains prompts for schema evolution and management.

use rmcp::model::{
    GetPromptRequestParams, GetPromptResult, Prompt, PromptArgument, PromptMessage,
    PromptMessageContent, PromptMessageRole,
};

/// Get schema operational prompt definitions
pub fn prompts() -> Vec<Prompt> {
    vec![Prompt {
        name: "manage_schema_evolution".to_string(),
        title: Some("Manage Schema Evolution".to_string()),
        description: Some(
            "Guided workflow for safely evolving schemas with compatibility validation"
                .to_string(),
        ),
        arguments: Some(vec![
            PromptArgument {
                name: "subject".to_string(),
                title: None,
                description: Some("Schema subject name to evolve".to_string()),
                required: Some(true),
            },
            PromptArgument {
                name: "schema_type".to_string(),
                title: None,
                description: Some(
                    "Schema type: json_schema, avro, protobuf, string, number, or bytes (optional)"
                        .to_string(),
                ),
                required: Some(false),
            },
        ]),
        icons: None,
        meta: None,
    }]
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

            Some(GetPromptResult {
                description: Some(format!("Manage schema evolution for {}", subject)),
                messages: vec![PromptMessage {
                    role: PromptMessageRole::User,
                    content: PromptMessageContent::Text {
                        text: build_schema_evolution_prompt(subject, schema_type),
                    },
                }],
            })
        }
        _ => None,
    }
}

fn build_schema_evolution_prompt(subject: &str, schema_type: Option<&str>) -> String {
    let schema_type_guidance = schema_type
        .map(|t| format!(" (type: {})", t))
        .unwrap_or_default();

    format!(
        r#"I need to evolve the schema for subject "{}"{}.

Please guide me through this workflow:

## Step 1: Review Current Schema
Use `get_schema` with subject="{}" to see the current schema definition and version.

## Step 2: List Existing Versions
Use `list_schema_versions` with subject="{}" to see the evolution history.

## Step 3: Check Compatibility Mode
Use `get_subject_compatibility` with subject="{}" to understand:
- BACKWARD: New schema can read old data (consumers upgrade first, add optional fields)
- FORWARD: Old schema can read new data (producers upgrade first, remove optional fields)
- FULL: Both BACKWARD and FORWARD (safest for critical schemas)
- NONE: No compatibility checks (development/testing only)

Note: Compatibility mode is set at **subject level** (applies to all versions).
Only administrators can change it with `set_subject_compatibility`.

## Step 4: Check Compatibility
Use `check_compatibility` to validate your new schema:
- subject: {}
- schema_definition: <new schema definition>
- schema_type: {}

This validates:
1. **Syntax**: Schema is well-formed (JSON/Avro/Protobuf)
2. **Compatibility**: New schema follows the subject's compatibility rules

Returns `is_compatible: true/false` with detailed error messages if incompatible.

## Step 5: Register New Version
If compatibility check passes:
Use `create_schema` with:
- subject: {}
- schema_definition: <new schema definition>
- schema_type: {}
- description: <optional change description>

The broker automatically checks compatibility again during registration.

## Step 6: Verify Evolution
Use `get_schema` with subject="{}" to confirm:
- New version registered successfully
- Version number incremented
- Schema definition stored correctly
- Compatibility mode unchanged

## Important Notes

**Subject-Level vs Topic-Level:**
- **Compatibility mode**: Subject-level (applies to all topics using this subject)
- **Validation policy**: Topic-level (NONE/WARN/ENFORCE, set by admin per topic)

**If Compatibility Check Fails:**
1. **Adjust schema** to be compatible (preferred):
   - BACKWARD: Add fields with defaults, make required fields optional
   - FORWARD: Remove optional fields only
   - FULL: Only additive changes with defaults
2. **Change compatibility mode** (admin-only, risky - coordinate with all teams)
3. **Create new subject** (e.g., "user-events-v2-value" for migration)

**Safety:**
- **Always check compatibility before registering** to avoid rejected schemas
- **Rollback**: Use `delete_schema_version` if absolutely needed (breaks consumers using that version)
- **Testing**: Test schema changes in dev/staging topics first
- **Documentation**: Add description field to explain what changed in each version"#,
        subject,
        schema_type_guidance,
        subject,
        subject,
        subject,
        subject,
        schema_type.unwrap_or("json_schema"),
        subject,
        subject,
        schema_type.unwrap_or("json_schema")
    )
}

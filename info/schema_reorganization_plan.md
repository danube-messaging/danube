# Schema Module Reorganization - Implementation Plan

## Overview

Reorganize the schema module from a feature-based structure (formats/, compatibility.rs, validator.rs) to a **schema-type-based structure** where each schema type (Avro, JSON, Protobuf) has its own folder containing all related functionality.

## Current Structure
```
danube-broker/src/schema/
├── compatibility.rs        (all compatibility logic)
├── validator.rs           (all validator logic)
├── formats/
│   ├── avro.rs           (Avro parsing/fingerprint)
│   ├── json_schema.rs    (JSON parsing/fingerprint)
│   └── mod.rs
├── metadata.rs
├── registry.rs
├── types.rs
└── mod.rs
```

## Target Structure
```
danube-broker/src/schema/
├── avro/
│   ├── mod.rs                    (public exports)
│   ├── avro_handler.rs           (parsing/fingerprint - from formats/avro.rs)
│   ├── avro_compatibility.rs     (backward/forward checks)
│   └── avro_validator.rs         (payload validation)
├── json/
│   ├── mod.rs                    (public exports)
│   ├── json_handler.rs           (parsing/fingerprint - from formats/json_schema.rs)
│   ├── json_compatibility.rs     (backward/forward checks)
│   └── json_validator.rs         (payload validation)
├── protobuf/
│   ├── mod.rs                    (public exports)
│   ├── protobuf_handler.rs       (parsing/fingerprint - stub)
│   ├── protobuf_compatibility.rs (backward/forward checks - stub)
│   └── protobuf_validator.rs     (payload validation - stub)
├── compatibility.rs              (orchestration only)
├── validator.rs                  (factory only)
├── metadata.rs                   (unchanged)
├── registry.rs                   (unchanged)
├── types.rs                      (unchanged)
└── mod.rs                        (update module declarations)
```

---

## Implementation Steps

### Phase 1: Create New Folder Structure

**Step 1.1**: Create new directories
- Create `danube-broker/src/schema/avro/`
- Create `danube-broker/src/schema/json/`
- Create `danube-broker/src/schema/protobuf/`

**Step 1.2**: Create module files
- Create `avro/mod.rs`
- Create `json/mod.rs`
- Create `protobuf/mod.rs`

---

### Phase 2: Move and Split Avro Implementation

**Step 2.1**: Move Avro handler
- Copy `formats/avro.rs` → `avro/avro_handler.rs`
- Rename struct from `AvroSchemaHandler` to `AvroHandler` (or keep name for consistency)
- Update imports in the file

**Step 2.2**: Extract Avro compatibility
- Create `avro/avro_compatibility.rs`
- Extract from `compatibility.rs`:
  - `check_avro_compatibility()` method → make it a standalone function or struct method
  - `check_avro_backward()` method
  - `check_avro_forward()` method
  - `check_schema_compatibility()` helper method
  - All Avro-related unit tests
- Create `AvroCompatibilityChecker` struct with these methods
- Update imports and function signatures

**Step 2.3**: Extract Avro validator
- Create `avro/avro_validator.rs`
- Move `AvroValidator` struct and implementation from `validator.rs`
- Move related unit tests
- Update imports

**Step 2.4**: Configure Avro module
- In `avro/mod.rs`:
  - Declare submodules (avro_handler, avro_compatibility, avro_validator)
  - Re-export public types: `pub use avro_handler::AvroHandler;`
  - Re-export `pub use avro_compatibility::AvroCompatibilityChecker;`
  - Re-export `pub use avro_validator::AvroValidator;`

---

### Phase 3: Move and Split JSON Implementation

**Step 3.1**: Move JSON handler
- Copy `formats/json_schema.rs` → `json/json_handler.rs`
- Rename struct from `JsonSchemaHandler` to `JsonHandler` (or keep for consistency)
- Update imports

**Step 3.2**: Extract JSON compatibility
- Create `json/json_compatibility.rs`
- Extract from `compatibility.rs`:
  - `check_json_compatibility()` method
  - `check_json_backward()` method
  - `check_json_forward()` method
  - JSON-related unit tests (currently none, but placeholder for future)
- Create `JsonCompatibilityChecker` struct
- **This is where new JSON compatibility logic will be implemented**

**Step 3.3**: Extract JSON validator
- Create `json/json_validator.rs`
- Move `JsonSchemaValidator` struct and implementation from `validator.rs`
- Move related unit tests
- Update imports

**Step 3.4**: Configure JSON module
- In `json/mod.rs`:
  - Declare submodules
  - Re-export public types
  - Document module purpose

---

### Phase 4: Create Protobuf Structure (Stubs)

**Step 4.1**: Create Protobuf handler stub
- Create `protobuf/protobuf_handler.rs`
- Add basic struct and parsing method (stub/unimplemented)
- Add fingerprint computation

**Step 4.2**: Create Protobuf compatibility stub
- Create `protobuf/protobuf_compatibility.rs`
- Create `ProtobufCompatibilityChecker` struct
- Add stub methods for backward/forward checking

**Step 4.3**: Create Protobuf validator
- Create `protobuf/protobuf_validator.rs`
- Move `ProtobufValidator` from `validator.rs`
- Keep as stub with unimplemented!() for now

**Step 4.4**: Configure Protobuf module
- In `protobuf/mod.rs`:
  - Declare submodules
  - Re-export public types
  - Add TODO comments for future implementation

---

### Phase 5: Refactor Core Files

**Step 5.1**: Refactor `compatibility.rs`
- Remove all format-specific implementation code
- Keep only:
  - `CompatibilityChecker` struct (orchestrator)
  - Main `check()` method that delegates to format-specific checkers
  - High-level logic for determining which checker to use
- Import format-specific checkers:
  - `use crate::schema::avro::AvroCompatibilityChecker;`
  - `use crate::schema::json::JsonCompatibilityChecker;`
  - `use crate::schema::protobuf::ProtobufCompatibilityChecker;`
- Update `check()` method to instantiate and delegate to format-specific checkers
- Remove format-specific unit tests (now in submodules)

**Step 5.2**: Refactor `validator.rs`
- Remove all format-specific validator implementations
- Keep only:
  - `PayloadValidator` trait
  - Simple validators (BytesValidator, StringValidator, NumberValidator)
  - `ValidatorFactory` struct
- Update factory to import and use format-specific validators:
  - `use crate::schema::avro::AvroValidator;`
  - `use crate::schema::json::JsonValidator;`
  - `use crate::schema::protobuf::ProtobufValidator;`
- Remove format-specific unit tests

**Step 5.3**: Update `registry.rs`
- Update imports to use new module structure:
  - `use crate::schema::avro::AvroHandler;`
  - `use crate::schema::json::JsonHandler;`
- Update `parse_schema()` method to use renamed handlers
- No logic changes needed

---

### Phase 6: Update Module Declarations

**Step 6.1**: Update `schema/mod.rs`
- Remove `pub mod formats;` declaration
- Add new module declarations:
  ```rust
  pub mod avro;
  pub mod json;
  pub mod protobuf;
  ```
- Update comments to reflect new organization
- Verify re-exports are still correct

**Step 6.2**: Delete old structure
- Delete `formats/` directory entirely
- Verify no remaining references to `formats` module

---

### Phase 7: Update Tests

**Step 7.1**: Update integration tests
- Update imports in `danube-broker/tests/schema_compatibility.rs`
- Change any direct imports from old structure to new structure
- Ensure all tests still compile

**Step 7.2**: Run unit tests
- Run `cargo test --package danube-broker --lib schema::avro` to verify Avro tests
- Run `cargo test --package danube-broker --lib schema::json` to verify JSON tests
- Run `cargo test --package danube-broker --lib schema` to verify all schema tests

**Step 7.3**: Fix any broken imports
- Search for remaining references to old module structure
- Update imports across the codebase

---

### Phase 8: Documentation and Cleanup

**Step 8.1**: Add module documentation
- Add comprehensive module-level docs to each folder's `mod.rs`
- Document the purpose and responsibilities of each submodule
- Add examples if appropriate

**Step 8.2**: Update README/architecture docs
- Document the new module organization
- Explain the rationale (schema-type-based organization)
- Update any diagrams if they exist

**Step 8.3**: Code review and cleanup
- Verify no code duplication
- Ensure consistent naming conventions
- Check that all imports are optimal (no unnecessary re-exports)
- Run `cargo clippy` and fix warnings

---

## Benefits of This Reorganization

1. **Locality**: All code for a schema type is in one place
2. **Scalability**: Easy to add new schema types (just create a new folder)
3. **Maintainability**: Clear ownership - JSON team works in json/, Avro team in avro/
4. **Testability**: Unit tests live with their implementation
5. **Future-ready**: When implementing JSON compatibility, all work is in json/ folder

## Risks and Considerations

1. **Breaking changes**: Any external code importing from old paths will break
2. **Import complexity**: Need to carefully manage re-exports in mod.rs files
3. **Git history**: File moves may complicate blame/history tracking (use `git log --follow`)
4. **Merge conflicts**: If there are active branches, this will cause conflicts

## Next Steps After Reorganization

Once the reorganization is complete:
1. Implement JSON Schema compatibility logic in `json/json_compatibility.rs`
2. Add comprehensive unit tests for JSON compatibility
3. Enable the ignored integration tests
4. Document the JSON compatibility rules

---

## Validation Checklist

- [x] All cargo tests pass
- [x] No clippy warnings
- [x] Integration tests still work
- [x] Module exports are correct
- [x] Documentation is updated
- [x] No dead code from old structure
- [x] Imports are clean and minimal

## Implementation Progress (Jan 3, 2026)

### Phase 1-7: Schema Reorganization ✅ COMPLETED
- All steps completed successfully
- 28 unit tests passing
- Build successful with no errors

### Phase 8: JSON Schema Compatibility Implementation ✅ COMPLETED
- **Status**: Implementation complete, tested, and bug fixed
- **What was implemented**:
  - `check_backward()` - Full backward compatibility checking
  - `check_forward()` - Full forward compatibility checking
  - Helper methods: `extract_required()`, `types_compatible()`
  - 9 comprehensive unit tests added
- **Test Results**: 15/15 JSON module tests passing
- **Bug Fixed**: Registry now properly rejects incompatible schemas
- **Ready for**: Final integration test run

---

## Summary of Implementation

### JSON Schema Compatibility Rules Implemented

**Backward Compatibility** (new schema reads old data):
- ✅ Can add optional fields
- ❌ Cannot remove required fields  
- ❌ Cannot make optional fields required
- ❌ Cannot change field types

**Forward Compatibility** (old schema reads new data):
- ✅ Can remove optional fields
- ❌ Cannot add new required fields
- ❌ Cannot change field types

**Full Compatibility** (both directions):
- ✅ Can only add optional fields safely

### Files Modified
- `danube-broker/src/schema/json/json_compatibility.rs` - 160+ lines of implementation

### Integration Tests Fixed ✅
Updated `danube-broker/tests/schema_compatibility.rs`:
1. ✅ **Test 2** - `backward_compatibility_remove_required_field_fails` - Removed `#[ignore]` attribute
2. ✅ **Test 4** - `forward_compatibility_add_required_field_fails` - Removed `#[ignore]` attribute
3. ✅ **Test 5** - `full_compatibility_strict_evolution` - Uncommented assertion (lines 235-238)
4. ✅ **Test 8** - `check_compatibility_before_registration` - Uncommented assertion (lines 386-389)

**All 9 integration tests are now enabled and ready to run!**

### Critical Bug Fixed 🐛
**File**: `danube-broker/src/schema/registry.rs` (line 68-70)

**Issue**: The `register_schema()` function was calling `check_compatibility_internal()` but only checking for errors in the Result wrapper, not verifying if the compatibility check itself passed. This meant incompatible schemas were being accepted.

**Fix**: Added explicit check for `compat_result.is_compatible` and return an error with details if the schema is incompatible:
```rust
let compat_result = self.check_compatibility_internal(&metadata, &schema_def, compatibility_mode).await?;
if !compat_result.is_compatible {
    return Err(anyhow!(
        "Schema is not compatible with subject '{}' (mode: {:?}): {}",
        subject, compatibility_mode, compat_result.errors.join("; ")
    ));
}
```

---

## 🎯 Test Results - ALL PASSING ✅

**Integration Tests (schema_compatibility):**
```bash
cargo test --test schema_compatibility
```

**Results: 9/9 PASSING ✅**
- ✅ backward_compatibility_add_optional_field
- ✅ backward_compatibility_remove_required_field_fails
- ✅ forward_compatibility_remove_optional_field
- ✅ forward_compatibility_add_required_field_fails
- ✅ full_compatibility_strict_evolution
- ✅ compatibility_none_allows_breaking_changes
- ✅ avro_backward_compatibility
- ✅ check_compatibility_before_registration
- ✅ multiple_versions_evolution

---

## Schema Type Support Status

### ✅ Fully Implemented & Tested

**Avro Schemas:**
- ✅ Parsing and fingerprinting
- ✅ Backward/Forward/Full compatibility checking
- ✅ Payload validation
- ✅ Integration tests passing

**JSON Schema:**
- ✅ Parsing and fingerprinting
- ✅ Backward/Forward/Full compatibility checking
- ✅ Payload validation
- ✅ Integration tests passing

**Simple Types (Bytes, String, Number):**
- ✅ Registration and versioning
- ✅ Compatibility checking (always compatible with same type)
- ✅ Type change prevention (cannot change String→Number, etc.)
- ✅ Payload validation
- ✅ Tested in `schema_registry_basic.rs`

### 🚧 Stub Implementation (Future Work)

**Protobuf:**
- ⚠️ Stub handler (parse returns placeholder)
- ⚠️ Stub compatibility (always returns compatible)
- ⚠️ Stub validator (unimplemented!)
- 📝 Ready for implementation when needed

---

## ✅ Project Complete!

### What Was Accomplished

1. **Schema Module Reorganized** (Phase 1-7)
   - Restructured from feature-based to schema-type-based organization
   - Each schema type (Avro, JSON, Protobuf) has its own folder
   - All related code (handler, compatibility, validator) co-located
   - 28 unit tests passing

2. **JSON Schema Compatibility Implemented** (Phase 8)
   - Full backward compatibility checking
   - Full forward compatibility checking
   - Full (both directions) compatibility checking
   - 9 new unit tests added
   - 15 total JSON module tests passing

3. **Critical Bug Fixed**
   - Registry now properly enforces compatibility rules
   - Incompatible schemas are rejected with detailed error messages
   - All 9 integration tests now passing

### Simple Schema Types Work Correctly

**Bytes, String, Number are fully functional:**
- ✅ Can be registered and versioned
- ✅ Always compatible with themselves (correct behavior for primitives)
- ✅ Cannot change between types (String→Number is blocked)
- ✅ Validated in existing integration tests

**Why they're "always compatible":**
- These are primitive types without structure
- No schema evolution rules needed (it's just raw data)
- Compatibility checker prevents type changes (line 30-34 in compatibility.rs)

### Nothing Left To Do! 🎉

All objectives completed:
- ✅ Schema module reorganized
- ✅ JSON Schema compatibility implemented and tested
- ✅ All integration tests passing (9/9)
- ✅ All unit tests passing (15/15 JSON, 28/28 total)
- ✅ Bug fixed in registry
- ✅ Documentation updated
- ✅ Simple types working correctly

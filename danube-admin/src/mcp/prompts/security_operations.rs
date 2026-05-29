//! Security operational workflow prompts
//!
//! Contains the cluster security setup prompt — a guided workflow
//! for enabling TLS, creating JWT tokens, configuring RBAC roles
//! and bindings from scratch. Derived from the securing-a-cluster docs.

use rmcp::model::{
    GetPromptRequestParams, GetPromptResult, Prompt, PromptArgument, PromptMessage,
    PromptMessageRole,
};

/// Get security operational prompt definitions
pub fn prompts() -> Vec<Prompt> {
    vec![Prompt::new(
        "secure_cluster_setup",
        Some("End-to-end guided workflow for securing a Danube cluster: TLS, JWT tokens, RBAC roles, and bindings"),
        Some(vec![
            PromptArgument::new("environment")
                .with_description("Target environment: 'development' (skip TLS) or 'production' (full security). Default: production")
                .with_required(false),
        ]),
    )
    .with_title("Secure Cluster Setup")]
}

/// Try to get a security operational prompt by name
pub fn get_prompt(params: &GetPromptRequestParams) -> Option<GetPromptResult> {
    let args = params.arguments.as_ref();

    match params.name.as_str() {
        "secure_cluster_setup" => {
            let environment = args
                .and_then(|a| a.get("environment"))
                .and_then(|v| v.as_str())
                .unwrap_or("production");

            Some(
                GetPromptResult::new(vec![PromptMessage::new_text(
                    PromptMessageRole::User,
                    build_secure_cluster_prompt(environment),
                )])
                .with_description("Secure cluster setup workflow"),
            )
        }
        _ => None,
    }
}

fn build_secure_cluster_prompt(environment: &str) -> String {
    let is_dev = environment == "development";

    let tls_section = if is_dev {
        r#"## Step 1: TLS Configuration (Skipped for Development)
In development mode (`auth.mode: none`), TLS is not required.
All connections are unencrypted and unauthenticated.

⚠️ **Never use `mode: none` in production.** All data flows in plaintext and any client has full access.

Skip to Step 3 if you only need to understand the RBAC model for future use."#
    } else {
        r#"## Step 1: TLS Certificate Setup
Before enabling security, the broker needs TLS certificates.

**Option A**: Use the convenience script (development/testing):
```bash
cd cert/
bash gen_certs.sh
```
This generates `ca-cert.pem`, `server-cert.pem`, `server-key.pem`, and client certs.

**Option B**: Use certificates from your organization's PKI (production).

For multi-node clusters, all brokers must share the same CA certificate.
Each broker can use its own server certificate signed by the shared CA.

## Step 2: Broker Configuration
Configure the broker with TLS and JWT settings in `danube_broker.yml`:

```yaml
auth:
  mode: tls
  tls:
    cert_file: "./cert/server-cert.pem"
    key_file: "./cert/server-key.pem"
    ca_file: "./cert/ca-cert.pem"
  jwt:
    secret_key: "your-secret-key"      # HMAC-SHA256 signing key
    issuer: "danube-auth"
    expiration_time: 3600
  super_admins:
    - "admin"
admin_tls: false   # Set to true if admin API is accessed over untrusted networks
```

**Important**: Use a strong, randomly generated `secret_key` and store it securely."#
    };

    let token_section = if is_dev {
        r#"## Step 3: Understanding JWT Tokens (For Reference)
Tokens are created offline — no broker connection needed.
Use `create_token` with:
- subject: the identity name (e.g., "my-app")
- secret_key: must match the broker's `jwt.secret_key`
- ttl: token lifetime (e.g., "24h", "365d", "8760h" for 1 year)

Use `validate_token` to inspect a token's claims and expiration."#
    } else {
        r#"## Step 3: Create Bootstrap Admin Token
The super-admin token is needed to set up roles and bindings.
This is an **offline** operation — no broker connection required.

Use `create_token` with:
- subject: "admin" (must match a `super_admins` entry)
- secret_key: the same key from the broker config
- ttl: "24h" (short-lived for bootstrap, create a longer one later)

Save the token — you'll need it for subsequent steps.

## Step 4: Create Service Account Tokens
Create a JWT token for each application or service that will connect:

Use `create_token` for each service:
- subject: service identity name (e.g., "payments-service", "analytics-reader")
- secret_key: same key as broker config
- ttl: "8760h" (1 year) or shorter based on rotation policy
- principal_type: "service_account" (default)"#
    };

    let rbac_section = if is_dev {
        r#"## Step 4: Understanding RBAC (For Reference)
Danube uses Role-Based Access Control with **default-deny** semantics.
If no policy explicitly grants access, the request is denied.

The model has three building blocks:
- **Permissions**: Lookup, Produce, Consume, ManageNamespace, ManageTopic, ManageSchema, ManageBroker, ManageCluster
- **Roles**: named bundles of permissions
- **Bindings**: attach a role to a principal at a scope (cluster / namespace / topic)

Scopes are checked from most specific to broadest:
1. Topic-level bindings
2. Namespace-level bindings
3. Cluster-level bindings

If any matching binding grants the required permission, access is allowed."#
    } else {
        r#"## Step 5: Create Roles
Define roles that match your access patterns.

Use `create_role` for each role:

**Minimum recommended roles:**
- **producer-role**: permissions = "Produce,Lookup"
- **consumer-role**: permissions = "Consume,Lookup"
- **operator-role**: permissions = "ManageNamespace,ManageTopic,ManageSchema,Lookup"

**Optional broader roles:**
- **cluster-admin-role**: permissions = "ManageCluster,ManageBroker,ManageNamespace,ManageTopic,ManageSchema,Lookup,Produce,Consume"

Use `list_roles` to verify all roles were created.

## Step 6: Create Bindings
Grant roles to service accounts at the appropriate scope.

Use `create_binding` for each binding:
- id: unique binding identifier (e.g., "bind-payments-producer")
- principal_type: "service_account"
- principal_name: matches the token's subject (e.g., "payments-service")
- roles: comma-separated role names (e.g., "producer-role")
- scope: "cluster", "namespace", or "topic"
- resource: required for namespace/topic scopes (e.g., "/payments", "/payments/orders")

**Scope guidance:**
| Scope | Use When |
|-------|---------|
| cluster | Service needs access to all resources (admin tools) |
| namespace | Service works within one namespace (most common) |
| topic | Service should only access specific topics (most restrictive) |

Prefer the most restrictive scope possible.

## Step 7: Verify Access
Use `list_bindings` with scope="cluster" (and scope="namespace" for each namespace) to audit:
- Every service account has the correct roles
- No overly broad cluster-scoped bindings that should be namespace-scoped
- No missing bindings (service accounts without roles will be denied)

## Step 8: Client Connection
Clients connect with their JWT token:
```
DanubeClient::builder()
    .service_url("https://broker:6650")
    .with_tls("./cert/ca-cert.pem")
    .with_token(&token)
    .build()
```

Unauthorized operations return `PermissionDenied` — check roles and bindings if this happens."#
    };

    let remove_superadmin = if is_dev {
        ""
    } else {
        r#"

## Step 9: Remove Super-Admin (Optional)
Once RBAC policies are verified and working, consider removing the `super_admins`
entry from the broker config for tighter security. Keep the admin token available
for emergency access.

⚠️ Super-admin access bypasses ALL authorization checks. Use only for bootstrap and emergencies."#
    };

    format!(
        r#"I need to set up security for a Danube cluster ({environment} environment).

{tls_section}

{token_section}

{rbac_section}{remove_superadmin}

## Security Model Summary

| Layer | What It Does | Required In Production |
|-------|-------------|----------------------|
| TLS | Encrypts all network traffic | Yes |
| JWT | Authenticates caller identity | Yes |
| RBAC | Controls what each identity can do | Yes |

**Default-deny**: if no binding grants access, the request is denied.
**Scoped bindings**: permissions cascade from topic → namespace → cluster.
**Super-admins**: bypass all checks (broker config only, not RBAC)."#,
    )
}

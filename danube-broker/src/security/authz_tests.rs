use crate::metadata_storage::MetadataStorage;
use crate::resources::SecurityResources;
use crate::security::authn::{AuthenticationMethod, Principal, SecurityContext};
use crate::security::authz::{enforce_authorization, Binding, Permission, Resource, Role};
use danube_core::metadata::MemoryStore;

async fn setup_security() -> SecurityResources {
    let mem = MemoryStore::new()
        .await
        .expect("Failed to init MemoryStore");
    let store = MetadataStorage::InMemory(mem);

    // We pass an empty list of super_admins for typical tests, and configure
    // super_admins explicitly in the super_admin test.
    let security = SecurityResources::new(store, vec![]);
    security
}

/// Tests that `BrokerInternal` principals automatically bypass all RBAC checks.
///
/// **Objective:** Ensure cluster-internal components (like replicators or background tasks)
/// authenticated via mTLS internal mapping (`BrokerInternal`) are never blocked by tenant RBAC policies.
///
/// **Expected Outcome:** The request allows access `Ok(())` unconditionally, despite no
/// roles or bindings existing in the empty store.
#[tokio::test]
async fn test_authorize_broker_internal() {
    let security = setup_security().await;

    let context = SecurityContext::authenticated(
        Principal::BrokerInternal {
            name: "broker/0".into(),
        },
        AuthenticationMethod::MutualTls,
    );

    // BrokerInternal is completely unrestricted, even if no roles exist.
    let result = enforce_authorization(
        &context,
        &Resource::Cluster,
        Permission::ManageCluster,
        &security,
    )
    .await;

    assert!(
        result.is_ok(),
        "BrokerInternal should be allowed unconditionally"
    );
}

/// Tests that explicit config-defined Super Admins bypass all RBAC checks.
///
/// **Objective:** Validate the break-glass/bootstrap mechanism. A user whose username is
/// explicitly listed in `auth.super_admins` configuration must be allowed to perform any action.
///
/// **Expected Outcome:** The request allows access `Ok(())` unconditionally, even though
/// no matching topic bindings or roles exist for `admin`.
#[tokio::test]
async fn test_authorize_super_admin() {
    let mem = MemoryStore::new()
        .await
        .expect("Failed to init MemoryStore");
    let store = MetadataStorage::InMemory(mem);

    // Configure 'admin' as a super admin
    let security = SecurityResources::new(store, vec!["admin".into()]);

    let context = SecurityContext::authenticated(
        Principal::User {
            name: "admin".into(),
        },
        AuthenticationMethod::Jwt,
    );

    // Super admin bypasses all RBAC, even if no bindings exist
    let result = enforce_authorization(
        &context,
        &Resource::Cluster,
        Permission::ManageCluster,
        &security,
    )
    .await;

    assert!(
        result.is_ok(),
        "Super admin should be allowed unconditionally"
    );
}

/// Tests that `Anonymous` principals (auth mode: none) are allowed through
/// without RBAC enforcement.
///
/// **Objective:** When the broker runs with `auth.mode: none`, the interceptor
/// creates an `Anonymous` security context. Authorization must allow these
/// requests unconditionally so that unauthenticated dev/test clients work.
///
/// **Expected Outcome:** The request returns `Ok(())`.
#[tokio::test]
async fn test_authorize_anonymous() {
    let security = setup_security().await;

    let context = SecurityContext::anonymous();

    let result = enforce_authorization(
        &context,
        &Resource::Topic("/default/topic1".into()),
        Permission::Produce,
        &security,
    )
    .await;

    assert!(result.is_ok(), "Anonymous (auth disabled) should be allowed");
}

/// Tests the "Default Deny" behavior of the authorization engine.
///
/// **Objective:** Ensure that a valid, authenticated user (e.g., via JWT or API Key)
/// is securely rejected from accessing resources if no explicit RBAC bindings permit them.
///
/// **Expected Outcome:** The request returns `Err(PermissionDenied)`.
#[tokio::test]
async fn test_authorize_default_deny() {
    let security = setup_security().await;

    let context = SecurityContext::authenticated(
        Principal::User {
            name: "alice".into(),
        },
        AuthenticationMethod::Jwt,
    );

    // No roles or bindings exist, default deny should apply.
    let result = enforce_authorization(
        &context,
        &Resource::Topic("/default/topic1".into()),
        Permission::Produce,
        &security,
    )
    .await;

    assert!(
        result.is_err(),
        "Access should be default-denied when no bindings exist"
    );
}

/// Tests a successful exact-match authorization via RBAC bindings.
///
/// **Objective:** Verify the standard happy-path authorization flow. A user (`alice`)
/// requests `Produce` access on a topic. They have a `topic` scoped binding tying them
/// to a role that explicitly contains `Permission::Produce`.
///
/// **Expected Outcome:** The request resolves to `Ok(())`.
#[tokio::test]
async fn test_authorize_allow_matching_binding() {
    let security = setup_security().await;

    // Create the required Role
    let role = Role {
        name: "producer-role".into(),
        permissions: vec![Permission::Produce, Permission::Lookup],
        system: false,
    };
    security
        .put_role(&role)
        .await
        .expect("failed to create role");

    // Create a Binding for Alice on the topic
    let binding = Binding {
        id: "alice-producer".into(),
        principal_type: "user".into(),
        principal_name: "alice".into(),
        role_names: vec!["producer-role".into()],
        scope: "topic".into(),
        resource_name: "/default/topic1".into(),
    };
    security
        .put_binding(&binding)
        .await
        .expect("failed to create binding");

    let context = SecurityContext::authenticated(
        Principal::User {
            name: "alice".into(),
        },
        AuthenticationMethod::Jwt,
    );

    let result = enforce_authorization(
        &context,
        &Resource::Topic("/default/topic1".into()),
        Permission::Produce,
        &security,
    )
    .await;

    assert!(
        result.is_ok(),
        "Alice should be allowed to produce based on binding"
    );
}

/// Tests that having a valid binding is insufficient if the associated role lacks
/// the specific permission requested.
///
/// **Objective:** Validate fine-grained role enforcement. The user (`bob`) has a binding
/// on the requested topic, but his role only grants `Consume` and `Lookup`.
/// He attempts to `Produce`.
///
/// **Expected Outcome:** The request returns `Err(PermissionDenied)`.
#[tokio::test]
async fn test_authorize_deny_insufficient_binding() {
    let security = setup_security().await;

    // Create a Role that ONLY allows Consume
    let role = Role {
        name: "consumer-role".into(),
        permissions: vec![Permission::Consume, Permission::Lookup],
        system: false,
    };
    security
        .put_role(&role)
        .await
        .expect("failed to create role");

    // Create a Binding for Bob on the topic
    let binding = Binding {
        id: "bob-consumer".into(),
        principal_type: "user".into(),
        principal_name: "bob".into(),
        role_names: vec!["consumer-role".into()],
        scope: "topic".into(),
        resource_name: "/default/topic1".into(),
    };
    security
        .put_binding(&binding)
        .await
        .expect("failed to create binding");

    let context = SecurityContext::authenticated(
        Principal::User { name: "bob".into() },
        AuthenticationMethod::Jwt,
    );

    // Bob tries to Produce, but only has Consume permission
    let result = enforce_authorization(
        &context,
        &Resource::Topic("/default/topic1".into()),
        Permission::Produce,
        &security,
    )
    .await;

    assert!(result.is_err(), "Bob should be denied produce access");
}

/// Tests the hierarchical scope resolution (Namespace-level binding overriding Topic-level checks).
///
/// **Objective:** Verify that bindings at a broader scope implicitly grant access to
/// all child resources. The user (`carol`) holds a `namespace` scoped binding for `/default`.
/// She requests access to a specific topic (`/default/topic1`) inside that namespace.
///
/// **Expected Outcome:** The request resolves to `Ok(())` by inheriting the namespace grant.
#[tokio::test]
async fn test_authorize_hierarchical_scope() {
    let security = setup_security().await;

    let role = Role {
        name: "namespace-admin-role".into(),
        permissions: vec![
            Permission::ManageTopic,
            Permission::Produce,
            Permission::Consume,
            Permission::Lookup,
        ],
        system: false,
    };
    security
        .put_role(&role)
        .await
        .expect("failed to create role");

    // Create a Binding for Carol at the namespace level
    let binding = Binding {
        id: "carol-ns-admin".into(),
        principal_type: "user".into(),
        principal_name: "carol".into(),
        role_names: vec!["namespace-admin-role".into()],
        scope: "namespace".into(),
        resource_name: "default".into(), // match extract_namespace output
    };
    security
        .put_binding(&binding)
        .await
        .expect("failed to create binding");

    let context = SecurityContext::authenticated(
        Principal::User {
            name: "carol".into(),
        },
        AuthenticationMethod::Jwt,
    );

    // Carol tries to Produce on a specific topic within the /default namespace.
    // The binding is at the namespace scope, so she should inherit permission for the topic.
    let result = enforce_authorization(
        &context,
        &Resource::Topic("/default/topic1".into()),
        Permission::Produce,
        &security,
    )
    .await;

    assert!(
        result.is_ok(),
        "Carol should be allowed via namespace-level binding override"
    );
}

use crate::admin::DanubeAdminImpl;
use crate::security::authz::{enforce_authorization, Binding, Permission, Resource, Role};
use crate::security::authn::get_security_context;
use danube_core::admin_proto::{
    security_admin_server::SecurityAdmin, BindingDefinition, CreateBindingRequest,
    CreateBindingResponse, CreateRoleRequest, CreateRoleResponse, DeleteBindingRequest,
    DeleteBindingResponse, DeleteRoleRequest, DeleteRoleResponse, Empty, GetBindingRequest,
    GetBindingResponse, GetRoleRequest, GetRoleResponse, IssueTokenRequest, IssueTokenResponse,
    ListBindingsRequest, ListBindingsResponse, ListRolesResponse, RevokeTokenRequest,
    RevokeTokenResponse, RoleDefinition,
};
use danube_core::jwt::{create_token, Claims};
use tonic::{Request, Response, Status};
use tracing::{info, Level};

// ── Conversion helpers ─────────────────────────────────────────────────────

fn permission_from_str(s: &str) -> Result<Permission, Status> {
    match s {
        "Lookup" => Ok(Permission::Lookup),
        "Produce" => Ok(Permission::Produce),
        "Consume" => Ok(Permission::Consume),
        "Replicate" => Ok(Permission::Replicate),
        "ReadPayload" => Ok(Permission::ReadPayload),
        "ManageNamespace" => Ok(Permission::ManageNamespace),
        "ManageTopic" => Ok(Permission::ManageTopic),
        "ManageSchema" => Ok(Permission::ManageSchema),
        "ManageBroker" => Ok(Permission::ManageBroker),
        "ManageCluster" => Ok(Permission::ManageCluster),
        other => Err(Status::invalid_argument(format!(
            "unknown permission: {other}"
        ))),
    }
}

fn permission_to_str(p: &Permission) -> String {
    match p {
        Permission::Lookup => "Lookup".into(),
        Permission::Produce => "Produce".into(),
        Permission::Consume => "Consume".into(),
        Permission::Replicate => "Replicate".into(),
        Permission::ReadPayload => "ReadPayload".into(),
        Permission::ManageNamespace => "ManageNamespace".into(),
        Permission::ManageTopic => "ManageTopic".into(),
        Permission::ManageSchema => "ManageSchema".into(),
        Permission::ManageBroker => "ManageBroker".into(),
        Permission::ManageCluster => "ManageCluster".into(),
    }
}

fn role_to_proto(role: &Role) -> RoleDefinition {
    RoleDefinition {
        name: role.name.clone(),
        permissions: role.permissions.iter().map(permission_to_str).collect(),
        system: role.system,
    }
}

fn binding_to_proto(b: &Binding) -> BindingDefinition {
    BindingDefinition {
        id: b.id.clone(),
        principal_type: b.principal_type.clone(),
        principal_name: b.principal_name.clone(),
        role_names: b.role_names.clone(),
        scope: b.scope.clone(),
        resource_name: b.resource_name.clone(),
    }
}

fn validate_scope(scope: &str) -> Result<(), Status> {
    match scope {
        "cluster" | "namespace" | "topic" => Ok(()),
        other => Err(Status::invalid_argument(format!(
            "invalid scope: {other} (expected cluster, namespace, or topic)"
        ))),
    }
}

// ── SecurityAdmin implementation ───────────────────────────────────────────

#[tonic::async_trait]
impl SecurityAdmin for DanubeAdminImpl {
    // ── Roles ──────────────────────────────────────────────────────────

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn create_role(
        &self,
        request: Request<CreateRoleRequest>,
    ) -> Result<Response<CreateRoleResponse>, Status> {
        let security_context = get_security_context(&request)?;
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let req = request.into_inner();
        let def = req
            .role
            .ok_or_else(|| Status::invalid_argument("role is required"))?;

        if def.name.is_empty() {
            return Err(Status::invalid_argument("role name is required"));
        }

        let mut permissions = Vec::with_capacity(def.permissions.len());
        for p in &def.permissions {
            permissions.push(permission_from_str(p)?);
        }

        let role = Role {
            name: def.name.clone(),
            permissions,
            system: def.system,
        };

        self.resources
            .security
            .put_role(&role)
            .await
            .map_err(|e| Status::internal(format!("failed to store role: {e}")))?;

        info!(role = %def.name, "created role");
        Ok(Response::new(CreateRoleResponse { success: true }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_role(
        &self,
        request: Request<GetRoleRequest>,
    ) -> Result<Response<GetRoleResponse>, Status> {
        let security_context = get_security_context(&request)?;
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let req = request.into_inner();
        let role = self
            .resources
            .security
            .get_role(&req.name)
            .await
            .map_err(|e| Status::internal(format!("failed to read role: {e}")))?
            .ok_or_else(|| Status::not_found(format!("role '{}' not found", req.name)))?;

        Ok(Response::new(GetRoleResponse {
            role: Some(role_to_proto(&role)),
        }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_roles(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<ListRolesResponse>, Status> {
        let security_context = get_security_context(&request)?;
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let names = self
            .resources
            .security
            .list_role_names()
            .await
            .map_err(|e| Status::internal(format!("failed to list roles: {e}")))?;

        let mut roles = Vec::with_capacity(names.len());
        for name in names {
            if let Ok(Some(role)) = self.resources.security.get_role(&name).await {
                roles.push(role_to_proto(&role));
            }
        }

        Ok(Response::new(ListRolesResponse { roles }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn delete_role(
        &self,
        request: Request<DeleteRoleRequest>,
    ) -> Result<Response<DeleteRoleResponse>, Status> {
        let security_context = get_security_context(&request)?;
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let req = request.into_inner();

        // Verify the role exists before deleting
        let role = self
            .resources
            .security
            .get_role(&req.name)
            .await
            .map_err(|e| Status::internal(format!("failed to read role: {e}")))?
            .ok_or_else(|| Status::not_found(format!("role '{}' not found", req.name)))?;

        if role.system {
            return Err(Status::failed_precondition(format!(
                "cannot delete system role '{}'",
                req.name
            )));
        }

        if self
            .resources
            .security
            .would_remove_last_admin(Some(&req.name), None, None)
            .await
        {
            return Err(Status::failed_precondition(format!(
                "cannot delete role '{}': would remove the last cluster admin",
                req.name
            )));
        }

        self.resources
            .security
            .delete_role(&req.name)
            .await
            .map_err(|e| Status::internal(format!("failed to delete role: {e}")))?;

        info!(role = %req.name, "deleted role");
        Ok(Response::new(DeleteRoleResponse { success: true }))
    }

    // ── Bindings ───────────────────────────────────────────────────────

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn create_binding(
        &self,
        request: Request<CreateBindingRequest>,
    ) -> Result<Response<CreateBindingResponse>, Status> {
        let security_context = get_security_context(&request)?;
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let req = request.into_inner();
        let def = req
            .binding
            .ok_or_else(|| Status::invalid_argument("binding is required"))?;

        if def.id.is_empty() {
            return Err(Status::invalid_argument("binding id is required"));
        }
        if def.principal_type.is_empty() || def.principal_name.is_empty() {
            return Err(Status::invalid_argument(
                "principal_type and principal_name are required",
            ));
        }
        if def.role_names.is_empty() {
            return Err(Status::invalid_argument(
                "at least one role_name is required",
            ));
        }
        validate_scope(&def.scope)?;

        let binding = Binding {
            id: def.id.clone(),
            principal_type: def.principal_type,
            principal_name: def.principal_name,
            role_names: def.role_names,
            scope: def.scope,
            resource_name: def.resource_name,
        };

        self.resources
            .security
            .put_binding(&binding)
            .await
            .map_err(|e| Status::internal(format!("failed to store binding: {e}")))?;

        info!(binding_id = %def.id, "created binding");
        Ok(Response::new(CreateBindingResponse { success: true }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_binding(
        &self,
        request: Request<GetBindingRequest>,
    ) -> Result<Response<GetBindingResponse>, Status> {
        let security_context = get_security_context(&request)?;
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let req = request.into_inner();
        validate_scope(&req.scope)?;

        let binding = self
            .resources
            .security
            .get_binding(&req.scope, &req.resource_name, &req.binding_id)
            .await
            .map_err(|e| Status::internal(format!("failed to read binding: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!("binding '{}' not found", req.binding_id))
            })?;

        Ok(Response::new(GetBindingResponse {
            binding: Some(binding_to_proto(&binding)),
        }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_bindings(
        &self,
        request: Request<ListBindingsRequest>,
    ) -> Result<Response<ListBindingsResponse>, Status> {
        let security_context = get_security_context(&request)?;
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let req = request.into_inner();
        validate_scope(&req.scope)?;

        let bindings = self
            .resources
            .security
            .list_bindings(&req.scope, &req.resource_name)
            .await
            .map_err(|e| Status::internal(format!("failed to list bindings: {e}")))?;

        Ok(Response::new(ListBindingsResponse {
            bindings: bindings.iter().map(binding_to_proto).collect(),
        }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_all_bindings(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<ListBindingsResponse>, Status> {
        let security_context = get_security_context(&request)?;
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let bindings = self
            .resources
            .security
            .list_all_bindings()
            .await
            .map_err(|e| Status::internal(format!("failed to list all bindings: {e}")))?;

        Ok(Response::new(ListBindingsResponse {
            bindings: bindings.iter().map(binding_to_proto).collect(),
        }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn delete_binding(
        &self,
        request: Request<DeleteBindingRequest>,
    ) -> Result<Response<DeleteBindingResponse>, Status> {
        let security_context = get_security_context(&request)?;
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let req = request.into_inner();
        validate_scope(&req.scope)?;

        // Verify binding exists
        self.resources
            .security
            .get_binding(&req.scope, &req.resource_name, &req.binding_id)
            .await
            .map_err(|e| Status::internal(format!("failed to read binding: {e}")))?
            .ok_or_else(|| {
                Status::not_found(format!("binding '{}' not found", req.binding_id))
            })?;

        if self
            .resources
            .security
            .would_remove_last_admin(None, Some((&req.scope, &req.resource_name, &req.binding_id)), None)
            .await
        {
            return Err(Status::failed_precondition(format!(
                "cannot delete binding '{}': would remove the last cluster admin",
                req.binding_id
            )));
        }

        self.resources
            .security
            .delete_binding(&req.scope, &req.resource_name, &req.binding_id)
            .await
            .map_err(|e| Status::internal(format!("failed to delete binding: {e}")))?;

        info!(binding_id = %req.binding_id, "deleted binding");
        Ok(Response::new(DeleteBindingResponse { success: true }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn issue_token(
        &self,
        request: Request<IssueTokenRequest>,
    ) -> Result<Response<IssueTokenResponse>, Status> {
        let security_context = get_security_context(&request)?;
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let req = request.into_inner();
        let subject = req.subject.trim();
        if subject.is_empty() {
            return Err(Status::invalid_argument("subject cannot be empty"));
        }
        let principal_type = if req.principal_type.trim().is_empty() {
            "service_account".to_string()
        } else {
            match req.principal_type.trim() {
                "user" | "service_account" => req.principal_type.trim().to_string(),
                other => {
                    return Err(Status::invalid_argument(format!(
                        "invalid principal_type '{}': expected 'user' or 'service_account'",
                        other
                    )));
                }
            }
        };

        let jwt_cfg = self.auth.jwt_config().ok_or_else(|| {
            Status::failed_precondition("JWT authentication is not configured on this broker")
        })?;

        let ttl = if req.ttl_seconds == 0 {
            jwt_cfg.expiration_time.max(60)
        } else {
            req.ttl_seconds
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let expires_at = now + ttl;

        let claims = Claims {
            iss: jwt_cfg.issuer.clone(),
            exp: expires_at,
            sub: subject.to_string(),
            principal_type,
            principal_name: subject.to_string(),
        };
        let token = create_token(&claims, &jwt_cfg.secret_key)
            .map_err(|e| Status::internal(format!("failed to sign JWT token: {e}")))?;

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "issue_token",
            subject,
            serde_json::json!({ "expires_at": expires_at }),
        )
        .await;

        info!(subject = %subject, expires_at, "issued JWT token");
        Ok(Response::new(IssueTokenResponse { token, expires_at }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn revoke_token(
        &self,
        request: Request<RevokeTokenRequest>,
    ) -> Result<Response<RevokeTokenResponse>, Status> {
        let security_context = get_security_context(&request)?;
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let req = request.into_inner();
        let subject = req.subject.trim();
        if subject.is_empty() {
            return Err(Status::invalid_argument("subject cannot be empty"));
        }

        if self
            .resources
            .security
            .would_remove_last_admin(None, None, Some(subject))
            .await
        {
            return Err(Status::failed_precondition(format!(
                "cannot revoke subject '{}': would remove the last cluster admin",
                subject
            )));
        }

        self.resources
            .security
            .revoke_subject(subject)
            .await
            .map_err(|e| Status::internal(format!("failed to revoke subject: {e}")))?;

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "revoke_token",
            subject,
            serde_json::json!({}),
        )
        .await;

        info!(subject = %subject, "revoked JWT subject");
        Ok(Response::new(RevokeTokenResponse {
            success: true,
            message: format!("revoked subject '{subject}'"),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_storage::MetadataStorage;
    use danube_core::metadata::MemoryStore;

    #[tokio::test]
    async fn test_last_admin_lockout_protection() {
        let mem = MemoryStore::new().await.unwrap();
        let store = MetadataStorage::InMemory(mem);
        let sec = crate::resources::SecurityResources::new(store, vec![]);

        let admin_role = Role {
            name: "cluster-admin".into(),
            permissions: vec![Permission::ManageCluster],
            system: false,
        };
        sec.put_role(&admin_role).await.unwrap();

        let binding1 = Binding {
            id: "b1".into(),
            principal_type: "user".into(),
            principal_name: "alice".into(),
            role_names: vec!["cluster-admin".into()],
            scope: "cluster".into(),
            resource_name: String::new(),
        };
        sec.put_binding(&binding1).await.unwrap();

        // Deleting the sole admin role, sole admin binding, or revoking alice must be blocked.
        assert!(sec.would_remove_last_admin(Some("cluster-admin"), None, None).await);
        assert!(
            sec.would_remove_last_admin(None, Some(("cluster", "", "b1")), None)
                .await
        );
        assert!(sec.would_remove_last_admin(None, None, Some("alice")).await);

        // Adding a second admin unlocks removing the first one.
        let binding2 = Binding {
            id: "b2".into(),
            principal_type: "user".into(),
            principal_name: "bob".into(),
            role_names: vec!["cluster-admin".into()],
            scope: "cluster".into(),
            resource_name: String::new(),
        };
        sec.put_binding(&binding2).await.unwrap();

        assert!(
            !sec.would_remove_last_admin(None, Some(("cluster", "", "b1")), None)
                .await
        );
        assert!(!sec.would_remove_last_admin(None, None, Some("alice")).await);
        // Deleting the shared role itself would still remove both admins!
        assert!(sec.would_remove_last_admin(Some("cluster-admin"), None, None).await);
    }

    #[tokio::test]
    async fn test_revoke_subject_rejects_authenticated_cache() {
        use crate::security::authn::{authenticate_request, JwtValidationCache};
        use crate::security::config::{AuthConfig, AuthMode, JwtConfig};

        let mem = MemoryStore::new().await.unwrap();
        let sec = crate::resources::SecurityResources::new(MetadataStorage::InMemory(mem), vec![]);

        let auth = AuthConfig {
            mode: AuthMode::Tls,
            tls: None,
            jwt: Some(JwtConfig {
                secret_key: "test-secret".into(),
                issuer: "danube".into(),
                expiration_time: 3600,
            }),
            super_admins: vec![],
        };
        let cache = JwtValidationCache::with_revoked(sec.revoked_subjects());

        let token = create_token(
            &Claims {
                iss: "danube".into(),
                exp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 600,
                sub: "agent-1".into(),
                principal_type: "service_account".into(),
                principal_name: "agent-1".into(),
            },
            "test-secret",
        )
        .unwrap();

        let mut req1 = Request::new(());
        req1.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}").parse().unwrap(),
        );
        assert!(authenticate_request(req1, &auth, &cache).is_ok());

        // Revoke agent-1; cached token must fail immediately on the next request.
        sec.revoke_subject("agent-1").await.unwrap();

        let mut req2 = Request::new(());
        req2.metadata_mut().insert(
            "authorization",
            format!("Bearer {token}").parse().unwrap(),
        );
        let err = authenticate_request(req2, &auth, &cache).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }
}

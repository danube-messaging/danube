use crate::admin::DanubeAdminImpl;
use crate::security::authz::{enforce_authorization, Binding, Permission, Resource, Role};
use crate::security::authn::get_security_context;
use danube_core::admin_proto::{
    security_admin_server::SecurityAdmin, BindingDefinition, CreateBindingRequest,
    CreateBindingResponse, CreateRoleRequest, CreateRoleResponse, DeleteBindingRequest,
    DeleteBindingResponse, DeleteRoleRequest, DeleteRoleResponse, Empty, GetBindingRequest,
    GetBindingResponse, GetRoleRequest, GetRoleResponse, ListBindingsRequest,
    ListBindingsResponse, ListRolesResponse, RoleDefinition,
};
use tonic::{Request, Response, Status};
use tracing::{info, Level};

// ── Conversion helpers ─────────────────────────────────────────────────────

fn permission_from_str(s: &str) -> Result<Permission, Status> {
    match s {
        "Lookup" => Ok(Permission::Lookup),
        "Produce" => Ok(Permission::Produce),
        "Consume" => Ok(Permission::Consume),
        "Replicate" => Ok(Permission::Replicate),
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

        self.resources
            .security
            .delete_binding(&req.scope, &req.resource_name, &req.binding_id)
            .await
            .map_err(|e| Status::internal(format!("failed to delete binding: {e}")))?;

        info!(binding_id = %req.binding_id, "deleted binding");
        Ok(Response::new(DeleteBindingResponse { success: true }))
    }
}

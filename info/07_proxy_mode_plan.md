# Proxy-Mode Connectivity Plan (Danube)

## Objective
Implement proxy-style connectivity (inspired by Pulsar multiple advertised listeners)
without backward compatibility. The lookup response returns two URLs:
- `connect_url`: where the client opens the gRPC channel (proxy/ingress or direct)
- `broker_url`: the actual broker that owns the topic (internal identity)

The client uses `connect_url` for transport and keeps `broker_url` for routing metadata.

## Scope
- **Proto**: replace `brokerServiceUrl` with `connect_url`, `broker_url`, `proxy`.
- **Broker config**: add optional `advertised_listeners` to `BrokerConfig`.
- **Broker registration**: store both URLs in metadata store.
- **Discovery handler**: return both URLs in lookup response.
- **Cluster lookup chain**: propagate both URLs through redirect path.
- **Client**: consume both URLs; use `connect_url` for transport, `broker_url` for identity.
- **Proxy routing**: client attaches metadata header when `proxy=true`.

## Non-Goals
- Backward compatibility with `brokerServiceUrl`.
- Building a custom proxy binary (use existing nginx/envoy/haproxy).

---

## 1) Proto Changes
File: `danube-core/proto/DanubeApi.proto`

### Current (lines 155-164):
```proto
message TopicLookupResponse {
  enum LookupType {
    Redirect = 0;
    Connect  = 1;
    Failed   = 2;
  }
  uint64 request_id = 3;
  LookupType response_type = 4;
  string brokerServiceUrl = 5;
}
```

### New:
```proto
message TopicLookupResponse {
  enum LookupType {
    Redirect = 0;
    Connect  = 1;
    Failed   = 2;
  }
  uint64 request_id = 3;
  LookupType response_type = 4;
  string connect_url = 5;       // where client should connect (proxy or direct)
  string broker_url = 6;        // the broker that owns the topic
  bool proxy = 7;               // true when connect_url != broker_url
}
```

Notes:
- Field number 5 is reused (no backward compat needed).
- Regenerate stubs via `cargo build` (triggers `danube-core/build.rs`).
- All code referencing `broker_service_url` must be updated.

---

## 2) Broker Config & ServiceConfiguration

### 2a) Config file (`config/danube_broker.yml`)
`broker.host` + `broker.ports` **must stay** — they define the **bind address** where
the broker listens (e.g., `0.0.0.0:6650`). Advertised listeners define what
clients see — a separate concern.

Add optional `advertised_listeners` under `broker:`:
```yaml
broker:
  host: "0.0.0.0"
  ports:
    client: 6650
    admin: 50051
    prometheus: 9040
  # Optional: advertised addresses for proxy/k8s mode
  # If omitted, bind address is used for both broker_url and connect_url
  advertised_listeners:
    # Internal: address reachable inside the cluster (broker identity)
    # Used for inter-broker communication and topic ownership
    broker_url: "broker-0.danube-core-broker-headless:6650"
    # External: address reachable by external clients via gRPC proxy
    connect_url: "danube-proxy.example.com:6650"
```

When `advertised_listeners` is absent → direct mode (no proxy), both URLs = bind address.
When present → `broker_url` = internal identity, `connect_url` = external client address.

Note on addresses:
- All connections are **gRPC**. The scheme (`http://` or `https://`) is added by
  the broker based on TLS config — not specified in the YAML.
- Ports are typically `6650` for both, but the proxy can map to a different external
  port if needed (e.g., NodePort `30650`).

### 2b) `BrokerConfig` struct (`danube-broker/src/service_configuration.rs`, line 69)
Current:
```rust
pub(crate) struct BrokerConfig {
    pub(crate) host: String,
    pub(crate) ports: BrokerPorts,
}
```
Add:
```rust
/// Optional advertised addresses for proxy/k8s mode
#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct AdvertisedListeners {
    /// Internal identity: reachable inside the cluster (inter-broker, topic ownership)
    pub(crate) broker_url: String,
    /// External: where clients connect (proxy/ingress address)
    pub(crate) connect_url: String,
}

pub(crate) struct BrokerConfig {
    pub(crate) host: String,
    pub(crate) ports: BrokerPorts,
    #[serde(default)]
    pub(crate) advertised_listeners: Option<AdvertisedListeners>,
}
```

### 2c) `ServiceConfiguration` struct (line 40)
Current fields:
```rust
pub(crate) broker_addr: std::net::SocketAddr,       // bind address
pub(crate) advertised_addr: Option<String>,          // CLI --advertised-addr
```
Replace with:
```rust
pub(crate) broker_addr: std::net::SocketAddr,        // bind address (unchanged)
pub(crate) broker_url: String,                       // internal broker identity
pub(crate) connect_url: String,                      // external connect address
pub(crate) proxy_enabled: bool,                      // connect_url != broker_url
```

### 2d) `TryFrom<LoadConfiguration>` (line 97)
Derive `broker_url` and `connect_url` from config:
- If `advertised_listeners` is set:
  - `broker_url` = `advertised_listeners.broker_url`
  - `connect_url` = `advertised_listeners.connect_url`
- If `advertised_listeners` is absent:
  - `broker_url` = `broker_addr.to_string()` (bind address)
  - `connect_url` = same as `broker_url`
- CLI `--advertised-addr` can still override `broker_url` for simple K8s use
  (sets both `broker_url` and `connect_url` to the same value — no proxy)

### 2e) `Args` struct (`danube-broker/src/args_parse.rs`, line 4)
Current: `--advertised-addr` sets `advertised_addr: Option<String>`.
Keep this flag — it overrides `broker_url` for simple cases (no full listener map).
No new CLI flags needed; advanced config goes in YAML.

### 2f) `danube_service.rs` (line 139)
Currently:
```rust
let advertised_addr = if let Some(advertised_addr) = &self.service_config.advertised_addr {
    advertised_addr.to_string()
} else {
    self.service_config.broker_addr.clone().to_string()
};
```
Replace with:
```rust
let broker_url = &self.service_config.broker_url;
let connect_url = &self.service_config.connect_url;
```

---

## 3) Broker Registration (Metadata Store)
File: `danube-broker/src/danube_service/broker_register.rs`

### Current payload (line 29):
```json
{
  "broker_addr": "http://0.0.0.0:6650",
  "advertised_addr": "0.0.0.0:6650",
  "admin_addr": "http://0.0.0.0:50051"
}
```

### New payload:
```json
{
  "broker_url": "http://broker-0.danube-core-broker-headless:6650",
  "connect_url": "http://danube-proxy.example.com:6650",
  "admin_addr": "http://broker-0:50051",
  "prom_exporter": "..."
}
```
Scheme (`http://` or `https://`) is prepended by the broker based on TLS config.

### Function signature change:
```rust
pub(crate) async fn register_broker(
    store: MetadataStorage,
    broker_id: &str,
    broker_url: &str,       // was: broker_addr
    connect_url: &str,      // new
    admin_addr: &str,
    metrics_addr: Option<&str>,
    ttl: i64,
    is_secure: bool,
) -> Result<()>
```

Both URLs get the scheme prefix (`http://` or `https://` based on `is_secure`).

---

## 4) Cluster Lookup Chain (Redirect Path)

When a broker redirects to another broker, the chain is:
1. `discovery_handler.topic_lookup` → calls `broker_service.lookup_topic`
2. `broker_service.lookup_topic` → calls `topic_cluster.find_serving_broker`
3. `topic_cluster.find_serving_broker` → calls `cluster.get_broker_addr`
4. `cluster.get_broker_addr` → reads `"broker_addr"` from JSON in local cache

### Changes needed:

**`resources/cluster.rs` (line 160):**
Add a new method (or update existing) to return both URLs:
```rust
pub(crate) fn get_broker_urls(&self, broker_id: &str) -> Option<(String, String)> {
    // Returns (broker_url, connect_url) from registration JSON
}
```

**`topic_cluster.rs` (line 340):**
Update `find_serving_broker` return type:
```rust
pub(crate) async fn find_serving_broker(&self, topic_name: &str) -> Option<(String, String)> {
    // Returns (broker_url, connect_url)
}
```

**`broker_service.rs` (line 251):**
Update `lookup_topic` return type:
```rust
pub(crate) async fn lookup_topic(&self, topic_name: &str)
    -> Option<(bool, String, String)>
    // Returns (is_local, broker_url, connect_url)
```

---

## 5) Discovery Handler
File: `danube-broker/src/broker_server/discovery_handler.rs`

### Current (line 34):
Returns single `broker_service_url`.

### New:
`DanubeServerImpl` needs access to both local URLs. Currently holds `broker_addr: SocketAddr`.

**`broker_server.rs` (line 29):**
Add fields:
```rust
pub(crate) struct DanubeServerImpl {
    service: Arc<BrokerService>,
    schema_registry: Arc<SchemaRegistryService>,
    broker_addr: SocketAddr,          // bind address (keep for server.serve())
    broker_url: String,               // internal advertised address
    connect_url: String,              // external advertised address
    proxy_enabled: bool,
    auth: AuthConfig,
    valid_api_keys: Vec<String>,
}
```

**`discovery_handler.rs` topic_lookup:**
```rust
let result = match service.lookup_topic(&req.topic).await {
    Some((true, _, _)) => {
        // Local: use own addresses
        (self.broker_url.clone(), self.connect_url.clone(), self.proxy_enabled)
    }
    Some((false, broker_url, connect_url)) => {
        // Redirect: use other broker's addresses
        let proxy = broker_url != connect_url;
        (broker_url, connect_url, proxy)
    }
    None => return Err(Status::not_found(...)),
};

let response = TopicLookupResponse {
    request_id: req.request_id,
    response_type: LookupType::Connect.into(), // or Redirect
    connect_url: result.1,
    broker_url: result.0,
    proxy: result.2,
};
```

**Note on existing inconsistency:**
- Local case currently returns `SocketAddr.to_string()` → `0.0.0.0:6650` (no scheme)
- Redirect case returns from JSON → `http://0.0.0.0:6650` (with scheme)
- This will be fixed: both paths return consistent scheme-prefixed URLs.

---

## 6) Client Changes

### 6a) `LookupResult` (`lookup_service.rs`, line 17)
```rust
pub struct LookupResult {
    pub response_type: i32,
    pub broker_url: Uri,        // was: addr
    pub connect_url: Uri,       // new
    pub proxy: bool,            // new
}
```

### 6b) `lookup_topic` method (line 58)
Parse both fields from response:
```rust
let lookup_resp = resp.into_inner();
lookup_result.broker_url = lookup_resp.broker_url.parse::<Uri>()?;
lookup_result.connect_url = lookup_resp.connect_url.parse::<Uri>()?;
lookup_result.proxy = lookup_resp.proxy;
```

### 6c) `handle_lookup` (line 144)
Currently returns `Uri`. Change to return `BrokerAddress`:
```rust
pub(crate) async fn handle_lookup(&self, addr: &Uri, topic: &str) -> Result<BrokerAddress> {
    match self.lookup_topic(addr, topic).await {
        Ok(lookup_result) => match lookup_type_from_i32(lookup_result.response_type) {
            Some(LookupType::Redirect) => Ok(BrokerAddress {
                connect_url: lookup_result.connect_url,
                broker_url: lookup_result.broker_url,
                proxy: lookup_result.proxy,
            }),
            Some(LookupType::Connect) => Ok(BrokerAddress {
                connect_url: lookup_result.connect_url,
                broker_url: lookup_result.broker_url,
                proxy: lookup_result.proxy,
            }),
            ...
        },
        ...
    }
}
```

### 6d) Callers of `handle_lookup`
Files: `client.rs`, `producer.rs`, `consumer.rs`
All callers currently expect a `Uri`. Update them to receive `BrokerAddress` and
store both `broker_url` and `connect_url`.

### 6e) `topic_producer.rs` / `topic_consumer.rs` connect methods
Currently:
```rust
self.client.cnx_manager.get_connection(&self.broker_addr, &self.broker_addr).await?;
```
Change to:
```rust
self.client.cnx_manager.get_connection(&self.broker_url, &self.connect_url).await?;
```
Both `broker_url` and `connect_url` stored as fields.

### 6f) `MessageID.broker_addr` (`danube-core/src/message.rs`, line 21)
Keep using `broker_url` (the real owner), not `connect_url`. This ensures acks
are routed to the correct broker identity.

### 6g) `ConnectionManager` (`connection_manager.rs`)
**No changes needed** — already supports `connect_url != broker_url` and proxy flag.

---

## 7) Proxy Routing Header
When `proxy=true`, the client should attach a gRPC metadata header on every RPC:
```
x-danube-broker-url: <broker_url>
```

The proxy reads this header to route to the correct backend broker.

### Client changes:
Add header insertion in `topic_producer.rs` and `topic_consumer.rs` when proxy
is enabled. This can be done alongside the existing auth token insertion.

### Proxy options:
1. **Envoy** — native gRPC support, header-based routing via `route_config`
2. **Nginx** — `grpc_pass` with `$http_x_danube_broker_url` variable routing
3. **HAProxy** — `use-server` with header inspection

No custom proxy binary needed. Provide example configs for at least one option.

---

## 8) Helm/Docs Updates
- Helm `values.yaml`: add `broker.advertisedListeners.internal` and `.external`
- Broker StatefulSet: pass listeners via config or env vars
- Docs: update Kubernetes external access guide with proxy setup example

---

## 9) Testing Plan
1. **Unit tests**
   - Config parsing with/without `advertised_listeners`
   - `ServiceConfiguration` derives correct `broker_url`/`connect_url`/`proxy_enabled`
   - Proto serialization/deserialization of new fields

2. **Integration tests (local)**
   - Broker registers with both URLs
   - Lookup returns both URLs correctly (local and redirect cases)
   - Client connects via `connect_url`, uses `broker_url` for identity
   - Full producer→consumer flow with proxy=false (direct mode)

3. **K8s smoke test**
   - Deploy with headless svc (internal) + proxy (external)
   - External client connects via proxy, produces/consumes, acks work

---

## Rollout Order
1. Proto update + regenerate stubs
2. Broker config (`BrokerConfig` + `ServiceConfiguration`)
3. Broker registration (`register_broker` + `danube_service.rs`)
4. Cluster lookup chain (`cluster.rs` → `topic_cluster.rs` → `broker_service.rs`)
5. Discovery handler (`discovery_handler.rs` + `broker_server.rs`)
6. Client lookup + connection (`lookup_service.rs` + callers)
7. Proxy routing header + proxy config examples
8. Helm + docs updates

---

## Files Changed (summary)
| File | Change |
|------|--------|
| `danube-core/proto/DanubeApi.proto` | Replace `brokerServiceUrl` with `connect_url` + `broker_url` + `proxy` |
| `config/danube_broker.yml` | Add `advertised_listeners` section |
| `danube-broker/src/service_configuration.rs` | Add listener fields to `BrokerConfig`, replace `advertised_addr` with `broker_url`/`connect_url`/`proxy_enabled` in `ServiceConfiguration` |
| `danube-broker/src/args_parse.rs` | Keep `--advertised-addr` (overrides `broker_url`) |
| `danube-broker/src/danube_service/broker_register.rs` | Store `broker_url` + `connect_url` in registration JSON |
| `danube-broker/src/danube_service.rs` | Pass both URLs to registration and server |
| `danube-broker/src/resources/cluster.rs` | Add `get_broker_urls()` returning `(broker_url, connect_url)` |
| `danube-broker/src/topic_cluster.rs` | `find_serving_broker` returns `(broker_url, connect_url)` |
| `danube-broker/src/broker_service.rs` | `lookup_topic` returns `(is_local, broker_url, connect_url)` |
| `danube-broker/src/broker_server.rs` | Add `broker_url`/`connect_url`/`proxy_enabled` to `DanubeServerImpl` |
| `danube-broker/src/broker_server/discovery_handler.rs` | Return both URLs in `TopicLookupResponse` |
| `danube-client/src/lookup_service.rs` | `LookupResult` has both URLs; `handle_lookup` returns `BrokerAddress` |
| `danube-client/src/topic_producer.rs` | Store + use both URLs |
| `danube-client/src/topic_consumer.rs` | Store + use both URLs |
| `danube-client/src/client.rs` | Propagate `BrokerAddress` from lookup |
| `danube-client/src/producer.rs` | Propagate `BrokerAddress` from lookup |
| `danube-client/src/consumer.rs` | Propagate `BrokerAddress` from lookup |

---

## Resolved Decisions
- **Proxy**: envoy or nginx with gRPC passthrough (`grpc_pass`). All connections are gRPC.
- **Routing header**: `x-danube-broker-url` (fixed name, not configurable).
- **Config keys**: `advertised_listeners.broker_url` and `advertised_listeners.connect_url`
  (direct naming, no `internal`/`external` indirection, no `internal_listener` field).
- **Ports**: typically `6650` for both broker_url and connect_url (proxy maps same port
  unless NodePort/LoadBalancer uses a different external port).
- **`broker_addr` vs `broker_url` vs `connect_url`**: all three are needed for K8s.
  For local/docker-compose they collapse to the same value automatically.

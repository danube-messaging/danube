# Danube Load Manager

Distributed broker load balancing and cluster orchestration for Danube messaging.

## Overview

The LoadManager is responsible for:

- **Load Distribution**: Monitors broker resource usage and distributes topic assignments
- **Dynamic Rebalancing**: Redistributes topics when brokers join or leave the cluster
- **Failover Management**: Handles broker failures by reallocating topics to healthy brokers
- **Resource Optimization**: Uses ranking algorithms to optimize broker utilization

## Architecture

The LoadManager operates as a centralized coordinator that:

1. Watches broker load metrics via metadata store events
2. Calculates broker rankings based on resource utilization
3. Assigns new topics to the least loaded brokers
4. Handles broker failures by cleaning up assignments and marking topics for reassignment

## Current Features (Phase 0)

### Load Reporting
- `LoadReport` structure containing broker resource usage
- Mock system resource monitoring (CPU, memory)
- Topic count tracking per broker

### Ranking Algorithms
- **Simple**: Topic count-based ranking
- **Composite**: Multi-factor ranking (topics, CPU, memory)

### Topic Assignment
- Leader-only assignment to prevent conflicts
- Least-loaded broker selection
- Exclusion support for broker unload operations

### Failover Handling
- Automatic topic reassignment on broker failure
- Cleanup of broker assignments
- Preservation of topic and namespace metadata

## Planned Enhancements

### Phase 1: Real Resource Monitoring (2-3 weeks)
- Cross-platform resource monitoring (Linux, Windows, macOS)
- Container-aware monitoring (Docker, Kubernetes)
- Real CPU, memory, disk I/O, and network metrics

### Phase 2: Intelligent Ranking (3-4 weeks)
- Per-topic metrics (throughput, latency, connections)
- Weighted composite ranking
- Configurable ranking weights

### Phase 3: Automated Rebalancing (4-5 weeks)
- Automated cluster rebalancing
- Imbalance detection and prevention
- Safety mechanisms (rate limiting, cooldown, blacklist)
- Admin CLI for manual control

### Additional Enhancements
- Observability (Prometheus metrics, Grafana dashboards)
- Smart topic placement (affinity rules, resource hints)
- Historical tracking and predictive analytics
- Performance optimizations

## Usage

### Integration with Broker

```rust
use danube_load_manager::{LoadManager, LeaderStateProvider};

// Create load manager
let mut load_manager = LoadManager::new(broker_id, meta_store);

// Bootstrap and start
let watch_stream = load_manager.bootstrap(broker_id).await?;
load_manager.start(watch_stream, broker_id, leader_election).await;

// Generate load reports periodically
let load_report = generate_load_report(topics_len, topic_list);
```

### Implementing LeaderStateProvider

```rust
#[async_trait::async_trait]
impl LeaderStateProvider for LeaderElection {
    async fn get_state(&self) -> danube_load_manager::LeaderState {
        // Map your leader election state to LoadManager's LeaderState
        match self.internal_state {
            InternalState::Leading => LeaderState::Leading,
            _ => LeaderState::Following,
        }
    }
}
```

## Development

### Building

```bash
cargo build -p danube-load-manager
```

### Testing

```bash
cargo test -p danube-load-manager
```

### Documentation

```bash
cargo doc -p danube-load-manager --open
```

## Migration from danube-broker

The LoadManager was previously part of the `danube-broker` crate. It has been extracted into a separate crate for:

- **Better organization**: Clear separation of concerns
- **Easier testing**: Independent testing without broker dependencies
- **Future extensibility**: Room for growth without bloating the broker crate
- **Reusability**: Potential for use in other Danube components

See `PHASE0_MIGRATION.md` for migration details.

## License

Apache-2.0

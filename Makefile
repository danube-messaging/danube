.DEFAULT_GOAL := no_target_specified

# Base ports for broker_addr, admin_addr, raft_addr, and prom_exporter
BASE_BROKER_PORT := 6650
BASE_ADMIN_PORT := 50051
BASE_RAFT_PORT := 7650
BASE_PROM_PORT := 9040
PROM_PORT := 9090
PROM_NAME := prometheus-danube
PROM_CONFIG := $(PWD)/scripts/prometheus.yml

# Number of broker instances
NUM_BROKERS := 3

# Path to configuration file
CONFIG_FILE := ./config/danube_broker_no_auth.yml

# Base data directory (each broker gets its own sub-directory)
BASE_DATA_DIR := ./danube-data

.PHONY: all brokers brokers-clean cluster-status data-clean admin admin-clean prom prom-clean

no_target_specified:
	@echo "Please specify a target to build. Available targets:"
	@echo "all            -- build and start $(NUM_BROKERS) broker instances (auto-clusters)"
	@echo "brokers        -- compile and start $(NUM_BROKERS) broker instances"
	@echo "brokers-clean  -- stop all broker instances"
	@echo "cluster-status -- show Raft cluster status"
	@echo "data-clean     -- remove all Raft data directories"
	@echo "admin          -- start the admin HTTP server on port 8080"
	@echo "admin-clean    -- stop the admin server"
	@echo "prom           -- start Prometheus on port $(PROM_PORT)"
	@echo "prom-clean     -- stop Prometheus"

all: brokers

# Set log level based on RUST_LOG value (if provided)
LOG_LEVEL = $(if $(RUST_LOG),$(RUST_LOG),info)

# Build the seed_nodes string: "0.0.0.0:7650,0.0.0.0:7651,0.0.0.0:7652"
SEED_NODES := $(shell for i in $$(seq 0 $$(echo $(NUM_BROKERS) - 1 | bc)); do \
	port=$$(($(BASE_RAFT_PORT) + i)); \
	if [ $$i -gt 0 ]; then printf ","; fi; \
	printf "0.0.0.0:$$port"; \
done)

brokers:
	@echo "Building Danube brokers..."
	@mkdir -p temp
	@RUST_LOG=$(LOG_LEVEL) RUST_BACKTRACE=1 cargo build --release --package danube-broker --bin danube-broker
	@echo "Seed nodes: $(SEED_NODES)"
	@for i in $(shell seq 0 $(shell echo $(NUM_BROKERS) - 1 | bc)); do \
		broker_port=$$(($(BASE_BROKER_PORT) + i)); \
		admin_port=$$(($(BASE_ADMIN_PORT) + i)); \
		raft_port=$$(($(BASE_RAFT_PORT) + i)); \
		prom_port=$$(($(BASE_PROM_PORT) + i)); \
		data_dir="$(BASE_DATA_DIR)/broker-$$i/raft"; \
		log_file="broker_$$broker_port.log"; \
		mkdir -p "$$data_dir"; \
		echo "Starting broker $$i: client=$$broker_port admin=$$admin_port raft=$$raft_port prom=$$prom_port"; \
		RUST_LOG=$(LOG_LEVEL) RUST_BACKTRACE=1 \
		./target/release/danube-broker \
		    --config-file $(CONFIG_FILE) \
		    --broker-addr "0.0.0.0:$$broker_port" \
		    --admin-addr "0.0.0.0:$$admin_port" \
		    --raft-addr "0.0.0.0:$$raft_port" \
		    --prom-exporter "0.0.0.0:$$prom_port" \
		    --data-dir "$$data_dir" \
		    --seed-nodes "$(SEED_NODES)" \
		    > temp/$$log_file 2>&1 & \
		sleep 2; \
	done
	@echo ""
	@echo "$(NUM_BROKERS) broker(s) started — cluster auto-bootstraps via seed nodes."

cluster-status:
	@cargo build --release --package danube-admin --bin danube-admin 2>/dev/null
	@RUST_LOG=warn ./target/release/danube-admin cluster status

brokers-clean:
	@echo "Cleaning up Broker instances..."
	@pids=$$(ps aux | grep '[d]anube-broker --config-file' | awk '{print $$2}'); \
	if [ -n "$$pids" ]; then \
		echo "$$pids" | xargs -r kill; \
		echo "Danube brokers stopped."; \
	else \
		echo "No Danube broker instances found."; \
	fi

data-clean:
	@echo "Removing Raft data directories..."
	rm -rf $(BASE_DATA_DIR)/broker-*/raft
	@echo "Raft data removed. Cluster will need re-initialization."

admin:
	@echo "Building Danube admin server..."
	@RUST_LOG=$(LOG_LEVEL) RUST_BACKTRACE=1 cargo build --release --package danube-admin --bin danube-admin
	RUST_LOG=$(LOG_LEVEL) RUST_BACKTRACE=1 ./target/release/danube-admin serve \
	    --listen-addr 0.0.0.0:8080 \
	    --broker-endpoint 0.0.0.0:$(BASE_ADMIN_PORT) \
	    > temp/admin_server_8080.log 2>&1 & \
	sleep 2
	@echo "Danube admin server started on 0.0.0.0:8080"

admin-clean:
	@echo "Cleaning up Admin server instances..."
	@pids=$$(ps aux | grep '[d]anube-admin serve' | awk '{print $$2}'); \
	if [ -n "$$pids" ]; then \
		echo "$$pids" | xargs -r kill; \
		echo "Danube admin server stopped."; \
	else \
		echo "No Danube admin server instances found."; \
	fi

prom:
	@echo "Starting Prometheus..."
	docker run -d --name $(PROM_NAME) \
	    -p $(PROM_PORT):9090 \
	    --add-host=host.docker.internal:host-gateway \
	    -v $(PROM_CONFIG):/etc/prometheus/prometheus.yml:ro \
	    prom/prometheus:latest
	@echo "Prometheus started on port: $(PROM_PORT) with config $(PROM_CONFIG)"

prom-clean:
	@echo "Cleaning up Prometheus container..."
	docker rm -f $(PROM_NAME)
	@echo "Prometheus container removed"


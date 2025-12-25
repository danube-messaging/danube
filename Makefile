.DEFAULT_GOAL := no_target_specified

# Base ports for broker_addr, admin_addr, and prom_exporter
BASE_BROKER_PORT := 6650
BASE_ADMIN_PORT := 50051
BASE_PROM_PORT := 9040
PROM_PORT := 9090
PROM_NAME := prometheus-danube
PROM_CONFIG := $(PWD)/scripts/prometheus.yml

# Number of broker instances
NUM_BROKERS := 3

# Path to configuration file
CONFIG_FILE := ./config/danube_broker.yml

# ETCD configuration
ETCD_NAME := etcd-danube
ETCD_DATA_DIR := ./etcd-data
ETCD_PORT := 2379

# HAProxy configuration
HAPROXY_CONFIG := haproxy.cfg
HAPROXY_PORT := 50051

.PHONY: all brokers etcd haproxy etcd-clean brokers-clean haproxy-clean admin admin-clean prometheus prometheus-clean

no_target_specified:
	@echo "Please specify a target to build. Available targets:"
	@echo "all           -- create the stack with etcd, brokers & haproxy"
	@echo "etcd          -- start ETCD instance in a docker container"
	@echo "etcd-clean    -- remove the ETCD instance"
	@echo "brokers       -- compile the danube-broker and listen on 6650 6651 6652 ports"
	@echo "brokers-clean -- remove the broker instances"
	@echo "admin         -- compile the danube-admin-gateway and listen on 8080 port"
	@echo "admin-clean   -- remove the admin gateway instance"
	@echo "prometheus    -- start a Prometheus server on port 9090 using scripts/prometheus.yml"
	@echo "prometheus-clean -- remove the Prometheus container"
#	@echo "haproxy       -- start an HAProxy instance with the haproxy.cfg config"
#	@echo "haproxy-clean -- remove the HAProxy instance"


all: etcd brokers haproxy 

etcd:
	@echo "Starting ETCD..."
	docker run -d --name $(ETCD_NAME) -p $(ETCD_PORT):$(ETCD_PORT) \
	    -v $(PWD)/$(ETCD_DATA_DIR):/etcd-data \
	    quay.io/coreos/etcd:v3.5.22 \
	    /usr/local/bin/etcd \
	    --name $(ETCD_NAME) \
	    --data-dir /etcd-data \
	    --advertise-client-urls http://0.0.0.0:$(ETCD_PORT) \
	    --listen-client-urls http://0.0.0.0:$(ETCD_PORT)
	@echo "ETCD instance started on port: $(ETCD_PORT)"

etcd-clean:
	@echo "Cleaning up ETCD instance..."
	docker rm -f $(ETCD_NAME)
	sudo rm -rf $(ETCD_DATA_DIR)
	@echo "ETCD instance and data removed"

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
	
# Set log level based on RUST_LOG value (if provided)
LOG_LEVEL = $(if $(RUST_LOG),$(RUST_LOG),info)

brokers:
	@echo "Building Danube brokers..."
	@for i in $(shell seq 0 $(shell echo $(NUM_BROKERS) - 1 | bc)); do \
		broker_port=$$(($(BASE_BROKER_PORT) + i)); \
		admin_port=$$(($(BASE_ADMIN_PORT) + i)); \
		prom_port=$$(($(BASE_PROM_PORT) + i)); \
		log_file="broker_$$broker_port.log"; \
		echo "Starting broker on broker port $$broker_port, admin port $$admin_port, prometheus port $$prom_port, logging to $$log_file"; \
		RUST_LOG=$(LOG_LEVEL) RUST_BACKTRACE=1 cargo build --release --package danube-broker --bin danube-broker && \
		RUST_LOG=$(LOG_LEVEL) RUST_BACKTRACE=1 ./target/release/danube-broker \
		    --config-file $(CONFIG_FILE) \
		    --broker-addr "0.0.0.0:$$broker_port" \
		    --admin-addr "0.0.0.0:$$admin_port" \
		    --prom-exporter "0.0.0.0:$$prom_port" \
		    > temp/$$log_file 2>&1 & \
		sleep 2; \
	done
	@echo "Danube brokers started on ports starting from $(BASE_BROKER_PORT)"

brokers-clean:
	@echo "Cleaning up Brokers instances..."
	@pids=$$(ps aux | grep '[d]anube-broker --config-file'); \
	if [ -n "$$pids" ]; then \
		echo "$$pids" | awk '{print $$2}' | xargs -r kill; \
		echo "Danube brokers cleaned up."; \
	else \
		echo "No Danube broker instances found."; \
	fi

admin:
	@echo "Building Danube admin gateway..."
	RUST_LOG=$(LOG_LEVEL) RUST_BACKTRACE=1 cargo build --release --package danube-admin-gateway --bin danube-admin-gateway && \
	RUST_LOG=$(LOG_LEVEL) RUST_BACKTRACE=1 ./target/release/danube-admin-gateway \
	    --broker-endpoint 0.0.0.0:$(BASE_ADMIN_PORT) \
	    --listen-addr 0.0.0.0:8080 \
	    --request-timeout-ms 800 \
	    --per-endpoint-cache-ms 3000 \
	    --prometheus-base-url http://localhost:$(PROM_PORT) \
	    > temp/admin_gateway_8080.log 2>&1 & \
	sleep 2
	@echo "Danube admin gateway started on 0.0.0.0:8080 with metrics on $(BASE_PROM_PORT)"

admin-clean:
	@echo "Cleaning up Admin Gateway instances..."
	@pids=$$(ps aux | grep '[d]anube-admin-gateway --broker-endpoint'); \
	if [ -n "$$pids" ]; then \
		echo "$$pids" | awk '{print $$2}' | xargs -r kill; \
		echo "Danube admin gateway cleaned up."; \
	else \
		echo "No Danube admin gateway instances found."; \
	fi

# haproxy:
# 	@echo "Starting HAProxy..."
# 	@haproxy -f $(HAPROXY_CONFIG) -D
# 	@echo "HAProxy listening on port: $(HAPROXY_PORT)"

# haproxy-clean:
# 	@echo "Stopping HAProxy..."
# 	@pkill haproxy
# 	@echo "Cleaning up..."






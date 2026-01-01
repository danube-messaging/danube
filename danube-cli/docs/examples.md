# Real-World Examples

Complete end-to-end examples and common use cases! 🎯

## Table of Contents
- [E-Commerce Order Processing](#e-commerce-order-processing)
- [Real-Time Analytics Pipeline](#real-time-analytics-pipeline)
- [Microservices Event-Driven Architecture](#microservices-event-driven-architecture)
- [IoT Data Collection](#iot-data-collection)
- [Log Aggregation System](#log-aggregation-system)
- [Notification Service](#notification-service)

## E-Commerce Order Processing

A complete order processing system with schema validation and reliable delivery.

### Scenario

Process customer orders through multiple stages: creation, payment, fulfillment, and shipping.

### Step 1: Define Order Schema

```bash
cat > order-schema.json << 'EOF'
{
  "type": "object",
  "properties": {
    "order_id": {"type": "string"},
    "customer_id": {"type": "string"},
    "items": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "sku": {"type": "string"},
          "quantity": {"type": "integer", "minimum": 1},
          "price": {"type": "number", "minimum": 0}
        },
        "required": ["sku", "quantity", "price"]
      }
    },
    "total": {"type": "number", "minimum": 0},
    "status": {"type": "string", "enum": ["pending", "paid", "fulfilled", "shipped"]},
    "timestamp": {"type": "string", "format": "date-time"}
  },
  "required": ["order_id", "customer_id", "items", "total", "status", "timestamp"]
}
EOF
```

### Step 2: Register Schema

```bash
danube-cli schema register orders \
  --type json_schema \
  --file order-schema.json
```

### Step 3: Order Creation Service

```bash
#!/bin/bash
# order-creation-service.sh

# Simulate order creation
create_order() {
  local order_id="ord_$(date +%s)_$RANDOM"
  local timestamp=$(date -Iseconds)
  
  danube-cli produce \
    -s http://localhost:6650 \
    -t /production/orders \
    --producer-name order-creation-service \
    --schema-subject orders \
    --reliable \
    -m "{
      \"order_id\": \"$order_id\",
      \"customer_id\": \"cust_$(( RANDOM % 1000 ))\",
      \"items\": [
        {\"sku\": \"WIDGET-001\", \"quantity\": 2, \"price\": 29.99},
        {\"sku\": \"GADGET-002\", \"quantity\": 1, \"price\": 49.99}
      ],
      \"total\": 109.97,
      \"status\": \"pending\",
      \"timestamp\": \"$timestamp\"
    }" \
    -a "service:order-creation,priority:high"
  
  echo "Order created: $order_id"
}

# Create orders every 5 seconds
while true; do
  create_order
  sleep 5
done
```

### Step 4: Payment Processing Service

```bash
#!/bin/bash
# payment-processor.sh

danube-cli consume \
  -s http://localhost:6650 \
  -t /production/orders \
  -n payment-processor \
  -m payment-processing \
  --sub-type shared | \
while IFS= read -r line; do
  if echo "$line" | grep -q "\"status\": \"pending\""; then
    # Extract order_id (simplified)
    echo "Processing payment for order..."
    
    # Simulate payment processing
    sleep 2
    
    # Update order status to 'paid'
    # (In real system, would update and republish)
    echo "Payment processed successfully"
  fi
done
```

### Step 5: Fulfillment Service

```bash
#!/bin/bash
# fulfillment-service.sh

danube-cli consume \
  -s http://localhost:6650 \
  -t /production/orders \
  -n fulfillment-processor \
  -m fulfillment-processing \
  --sub-type shared | \
while IFS= read -r line; do
  if echo "$line" | grep -q "\"status\": \"paid\""; then
    echo "Fulfilling order..."
    sleep 3
    echo "Order fulfilled and ready to ship"
  fi
done
```

### Step 6: Run the System

```bash
# Terminal 1: Start order creation
./order-creation-service.sh

# Terminal 2: Start payment processor
./payment-processor.sh

# Terminal 3: Start fulfillment service
./fulfillment-service.sh
```

## Real-Time Analytics Pipeline

Process and aggregate events in real-time.

### Scenario

Collect user events, aggregate metrics, and generate insights.

### Step 1: Event Schema

```bash
cat > event-schema.json << 'EOF'
{
  "type": "object",
  "properties": {
    "event_id": {"type": "string"},
    "event_type": {"type": "string"},
    "user_id": {"type": "string"},
    "timestamp": {"type": "string", "format": "date-time"},
    "properties": {"type": "object"}
  },
  "required": ["event_id", "event_type", "user_id", "timestamp"]
}
EOF

danube-cli schema register user-events \
  --type json_schema \
  --file event-schema.json
```

### Step 2: Event Collection

```bash
#!/bin/bash
# event-collector.sh

# Collect various user events
events=("page_view" "button_click" "form_submit" "purchase" "logout")

while true; do
  event_type=${events[$RANDOM % ${#events[@]}]}
  user_id="user_$(( RANDOM % 100 ))"
  timestamp=$(date -Iseconds)
  
  danube-cli produce \
    -s http://localhost:6650 \
    -t /analytics/raw-events \
    --producer-name event-collector \
    --schema-subject user-events \
    --partitions 8 \
    -m "{
      \"event_id\": \"evt_$(uuidgen)\",
      \"event_type\": \"$event_type\",
      \"user_id\": \"$user_id\",
      \"timestamp\": \"$timestamp\",
      \"properties\": {
        \"page\": \"/home\",
        \"duration\": $(( RANDOM % 300 ))
      }
    }" \
    -a "source:web,environment:production"
  
  sleep 0.5
done
```

### Step 3: Real-Time Aggregation

```bash
#!/bin/bash
# aggregator.sh

declare -A event_counts
declare -A user_counts

danube-cli consume \
  -s http://localhost:6650 \
  -t /analytics/raw-events \
  -n aggregator-1 \
  -m analytics-aggregation \
  --sub-type shared | \
while IFS= read -r line; do
  # Extract event type and user (simplified)
  if echo "$line" | grep -q "event_type"; then
    event_type=$(echo "$line" | grep -o '"event_type": "[^"]*"' | cut -d'"' -f4)
    user_id=$(echo "$line" | grep -o '"user_id": "[^"]*"' | cut -d'"' -f4)
    
    # Update counts
    ((event_counts[$event_type]++))
    ((user_counts[$user_id]++))
    
    # Print dashboard every 10 events
    total=0
    for count in "${event_counts[@]}"; do
      ((total += count))
    done
    
    if (( total % 10 == 0 )); then
      echo "=== Real-Time Dashboard ==="
      echo "Total Events: $total"
      echo "Event Types:"
      for event in "${!event_counts[@]}"; do
        echo "  $event: ${event_counts[$event]}"
      done
      echo "Active Users: ${#user_counts[@]}"
      echo "=========================="
    fi
  fi
done
```

### Step 4: Publish Aggregated Metrics

```bash
#!/bin/bash
# metrics-publisher.sh

# Publish aggregated metrics every minute
while true; do
  timestamp=$(date -Iseconds)
  
  danube-cli produce \
    -s http://localhost:6650 \
    -t /analytics/metrics \
    --producer-name metrics-publisher \
    -m "{
      \"timestamp\": \"$timestamp\",
      \"metrics\": {
        \"total_events\": $(get_total_events),
        \"active_users\": $(get_active_users),
        \"events_per_second\": $(get_rate)
      }
    }"
  
  sleep 60
done
```

## Microservices Event-Driven Architecture

Coordinate multiple microservices using events.

### Scenario

User registration triggers email, analytics, and CRM updates.

### Step 1: Registration Event Schema

```bash
cat > registration-schema.json << 'EOF'
{
  "type": "object",
  "properties": {
    "event_type": {"type": "string", "enum": ["user.registered"]},
    "user_id": {"type": "string"},
    "email": {"type": "string", "format": "email"},
    "name": {"type": "string"},
    "timestamp": {"type": "string", "format": "date-time"}
  },
  "required": ["event_type", "user_id", "email", "name", "timestamp"]
}
EOF

danube-cli schema register user-events \
  --type json_schema \
  --file registration-schema.json
```

### Step 2: User Service (Publisher)

```bash
#!/bin/bash
# user-service.sh

register_user() {
  local user_id="user_$(uuidgen)"
  local timestamp=$(date -Iseconds)
  
  echo "Registering user: $user_id"
  
  danube-cli produce \
    -s http://localhost:6650 \
    -t /events/users \
    --producer-name user-service \
    --schema-subject user-events \
    --reliable \
    -m "{
      \"event_type\": \"user.registered\",
      \"user_id\": \"$user_id\",
      \"email\": \"user@example.com\",
      \"name\": \"John Doe\",
      \"timestamp\": \"$timestamp\"
    }" \
    -a "service:user-service,event:registration"
  
  echo "Registration event published"
}

# Simulate registrations
while true; do
  register_user
  sleep 10
done
```

### Step 3: Email Service (Subscriber)

```bash
#!/bin/bash
# email-service.sh

send_welcome_email() {
  local email=$1
  local name=$2
  echo "📧 Sending welcome email to $email (Name: $name)"
  # Email sending logic here
}

danube-cli consume \
  -s http://localhost:6650 \
  -t /events/users \
  -n email-service \
  -m email-service-subscription \
  --sub-type shared | \
while IFS= read -r line; do
  if echo "$line" | grep -q "user.registered"; then
    # Extract email and name (simplified)
    email=$(echo "$line" | grep -o '"email": "[^"]*"' | cut -d'"' -f4)
    name=$(echo "$line" | grep -o '"name": "[^"]*"' | cut -d'"' -f4)
    send_welcome_email "$email" "$name"
  fi
done
```

### Step 4: Analytics Service (Subscriber)

```bash
#!/bin/bash
# analytics-service.sh

track_registration() {
  local user_id=$1
  echo "📊 Tracking registration: $user_id"
  # Analytics tracking logic
}

danube-cli consume \
  -s http://localhost:6650 \
  -t /events/users \
  -n analytics-service \
  -m analytics-service-subscription \
  --sub-type shared | \
while IFS= read -r line; do
  if echo "$line" | grep -q "user.registered"; then
    user_id=$(echo "$line" | grep -o '"user_id": "[^"]*"' | cut -d'"' -f4)
    track_registration "$user_id"
  fi
done
```

### Step 5: CRM Service (Subscriber)

```bash
#!/bin/bash
# crm-service.sh

add_to_crm() {
  local user_id=$1
  local email=$2
  echo "👤 Adding to CRM: $user_id ($email)"
  # CRM integration logic
}

danube-cli consume \
  -s http://localhost:6650 \
  -t /events/users \
  -n crm-service \
  -m crm-service-subscription \
  --sub-type shared | \
while IFS= read -r line; do
  if echo "$line" | grep -q "user.registered"; then
    user_id=$(echo "$line" | grep -o '"user_id": "[^"]*"' | cut -d'"' -f4)
    email=$(echo "$line" | grep -o '"email": "[^"]*"' | cut -d'"' -f4)
    add_to_crm "$user_id" "$email"
  fi
done
```

### Run All Services

```bash
# Terminal 1: User Service
./user-service.sh

# Terminal 2: Email Service
./email-service.sh

# Terminal 3: Analytics Service
./analytics-service.sh

# Terminal 4: CRM Service
./crm-service.sh
```

## IoT Data Collection

Collect and process sensor data from IoT devices.

### Scenario

Collect temperature readings from multiple sensors, aggregate, and trigger alerts.

### Step 1: Sensor Data Schema

```bash
cat > sensor-schema.json << 'EOF'
{
  "type": "object",
  "properties": {
    "sensor_id": {"type": "string"},
    "temperature": {"type": "number"},
    "humidity": {"type": "number", "minimum": 0, "maximum": 100},
    "timestamp": {"type": "string", "format": "date-time"},
    "location": {"type": "string"}
  },
  "required": ["sensor_id", "temperature", "timestamp"]
}
EOF

danube-cli schema register sensor-readings \
  --type json_schema \
  --file sensor-schema.json
```

### Step 2: IoT Device Simulator

```bash
#!/bin/bash
# iot-simulator.sh

# Simulate 10 sensors
for sensor_num in {1..10}; do
  (
    while true; do
      temp=$(awk -v min=18 -v max=28 'BEGIN{srand(); print min+rand()*(max-min)}')
      humidity=$(awk -v min=30 -v max=70 'BEGIN{srand(); print int(min+rand()*(max-min))}')
      timestamp=$(date -Iseconds)
      
      danube-cli produce \
        -s http://localhost:6650 \
        -t /iot/sensor-readings \
        --producer-name "sensor-$sensor_num" \
        --schema-subject sensor-readings \
        --partitions 4 \
        -m "{
          \"sensor_id\": \"sensor_$sensor_num\",
          \"temperature\": $temp,
          \"humidity\": $humidity,
          \"timestamp\": \"$timestamp\",
          \"location\": \"floor_$(( sensor_num / 3 + 1 ))\"
        }" \
        -a "device:sensor,floor:$(( sensor_num / 3 + 1 ))"
      
      sleep 5
    done
  ) &
done

wait
```

### Step 3: Monitoring & Alerting

```bash
#!/bin/bash
# monitor.sh

TEMP_THRESHOLD=25.0

danube-cli consume \
  -s http://localhost:6650 \
  -t /iot/sensor-readings \
  -n temperature-monitor \
  -m monitoring-subscription \
  --sub-type shared | \
while IFS= read -r line; do
  if echo "$line" | grep -q "temperature"; then
    # Extract temperature and sensor_id (simplified)
    temp=$(echo "$line" | grep -o '"temperature": [0-9.]*' | awk '{print $2}')
    sensor=$(echo "$line" | grep -o '"sensor_id": "[^"]*"' | cut -d'"' -f4)
    
    # Check threshold
    if (( $(echo "$temp > $TEMP_THRESHOLD" | bc -l) )); then
      echo "🚨 ALERT: $sensor temperature $temp°C exceeds threshold!"
      
      # Send alert
      danube-cli produce \
        -s http://localhost:6650 \
        -t /alerts/temperature \
        --reliable \
        -m "{\"sensor\":\"$sensor\",\"temperature\":$temp,\"alert\":\"HIGH_TEMP\"}" \
        -a "severity:high,type:temperature"
    fi
  fi
done
```

### Step 4: Data Aggregation

```bash
#!/bin/bash
# aggregator.sh

declare -A sensor_readings
declare -A sensor_counts

danube-cli consume \
  -s http://localhost:6650 \
  -t /iot/sensor-readings \
  -n data-aggregator \
  -m aggregation-subscription \
  --sub-type shared | \
while IFS= read -r line; do
  if echo "$line" | grep -q "sensor_id"; then
    sensor=$(echo "$line" | grep -o '"sensor_id": "[^"]*"' | cut -d'"' -f4)
    temp=$(echo "$line" | grep -o '"temperature": [0-9.]*' | awk '{print $2}')
    
    # Accumulate readings
    sensor_readings[$sensor]=$(echo "${sensor_readings[$sensor]:-0} + $temp" | bc)
    ((sensor_counts[$sensor]++))
    
    # Calculate and publish averages every 100 readings
    total_count=0
    for count in "${sensor_counts[@]}"; do
      ((total_count += count))
    done
    
    if (( total_count % 100 == 0 )); then
      echo "=== Sensor Averages ==="
      for sensor in "${!sensor_readings[@]}"; do
        avg=$(echo "scale=2; ${sensor_readings[$sensor]} / ${sensor_counts[$sensor]}" | bc)
        echo "$sensor: ${avg}°C"
      done
      echo "======================="
    fi
  fi
done
```

## Log Aggregation System

Centralized log collection and analysis.

### Scenario

Collect logs from multiple services and analyze in real-time.

### Step 1: Log Schema

```bash
cat > log-schema.json << 'EOF'
{
  "type": "object",
  "properties": {
    "level": {"type": "string", "enum": ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]},
    "service": {"type": "string"},
    "message": {"type": "string"},
    "timestamp": {"type": "string", "format": "date-time"},
    "trace_id": {"type": "string"},
    "context": {"type": "object"}
  },
  "required": ["level", "service", "message", "timestamp"]
}
EOF

danube-cli schema register application-logs \
  --type json_schema \
  --file log-schema.json
```

### Step 2: Service Log Producers

```bash
#!/bin/bash
# service-logger.sh

SERVICE_NAME=$1
LOG_LEVELS=("INFO" "WARN" "ERROR")

while true; do
  level=${LOG_LEVELS[$RANDOM % ${#LOG_LEVELS[@]}]}
  timestamp=$(date -Iseconds)
  trace_id=$(uuidgen)
  
  danube-cli produce \
    -s http://localhost:6650 \
    -t /logs/applications \
    --producer-name "$SERVICE_NAME" \
    --schema-subject application-logs \
    --partitions 8 \
    -m "{
      \"level\": \"$level\",
      \"service\": \"$SERVICE_NAME\",
      \"message\": \"Sample log message from $SERVICE_NAME\",
      \"timestamp\": \"$timestamp\",
      \"trace_id\": \"$trace_id\",
      \"context\": {\"host\": \"$(hostname)\", \"pid\": $$}
    }" \
    -a "level:$level,service:$SERVICE_NAME"
  
  sleep $(( RANDOM % 5 + 1 ))
done
```

```bash
# Start multiple services
./service-logger.sh "api-service" &
./service-logger.sh "auth-service" &
./service-logger.sh "payment-service" &
./service-logger.sh "notification-service" &
```

### Step 3: Log Analyzer

```bash
#!/bin/bash
# log-analyzer.sh

declare -A error_counts
declare -A service_counts

danube-cli consume \
  -s http://localhost:6650 \
  -t /logs/applications \
  -n log-analyzer \
  -m log-analysis \
  --sub-type shared | \
while IFS= read -r line; do
  if echo "$line" | grep -q "\"level\""; then
    level=$(echo "$line" | grep -o '"level": "[^"]*"' | cut -d'"' -f4)
    service=$(echo "$line" | grep -o '"service": "[^"]*"' | cut -d'"' -f4)
    
    ((service_counts[$service]++))
    
    if [ "$level" == "ERROR" ] || [ "$level" == "FATAL" ]; then
      ((error_counts[$service]++))
      echo "🔴 ERROR in $service"
      
      # Alert if error rate is high
      error_rate=$((error_counts[$service] * 100 / service_counts[$service]))
      if [ $error_rate -gt 10 ]; then
        echo "🚨 HIGH ERROR RATE in $service: $error_rate%"
      fi
    fi
  fi
done
```

## Notification Service

Multi-channel notification system.

### Complete Example

```bash
# Schema
cat > notification-schema.json << 'EOF'
{
  "type": "object",
  "properties": {
    "notification_id": {"type": "string"},
    "type": {"type": "string", "enum": ["email", "sms", "push"]},
    "recipient": {"type": "string"},
    "subject": {"type": "string"},
    "body": {"type": "string"},
    "priority": {"type": "string", "enum": ["low", "normal", "high", "urgent"]},
    "timestamp": {"type": "string", "format": "date-time"}
  },
  "required": ["notification_id", "type", "recipient", "body", "priority", "timestamp"]
}
EOF

danube-cli schema register notifications \
  --type json_schema \
  --file notification-schema.json

# Producer: Notification Queue
#!/bin/bash
while true; do
  danube-cli produce \
    -s http://localhost:6650 \
    -t /notifications/queue \
    --schema-subject notifications \
    --reliable \
    -m "{
      \"notification_id\": \"notif_$(uuidgen)\",
      \"type\": \"email\",
      \"recipient\": \"user@example.com\",
      \"subject\": \"Alert\",
      \"body\": \"Important notification\",
      \"priority\": \"high\",
      \"timestamp\": \"$(date -Iseconds)\"
    }" \
    -a "priority:high,channel:email"
  sleep 10
done &

# Consumer: Email Handler
danube-cli consume \
  -s http://localhost:6650 \
  -t /notifications/queue \
  -n email-handler \
  -m email-processors \
  --sub-type shared | \
while IFS= read -r line; do
  if echo "$line" | grep -q "\"type\": \"email\""; then
    echo "📧 Sending email notification..."
  fi
done
```

## Summary

These examples demonstrate:
- ✅ Schema definition and registration
- ✅ Producer patterns for different use cases
- ✅ Consumer patterns with message processing
- ✅ Error handling and monitoring
- ✅ Multi-service coordination
- ✅ Real-time data processing

**Try these patterns in your own applications!** 🚀

---

For more details, see:
- **[Producer Basics](./producer-basics.md)**
- **[Consumer Basics](./consumer-basics.md)**
- **[Schema Registry](./schema-registry.md)**

# danube-cli

Welcome to the **Danube CLI** - command-line companion for interacting with Danube messaging system! 🚀

## What is Danube CLI?

Danube CLI is a powerful, easy-to-use command-line tool that lets you:
- 📤 **Produce** messages to topics with schema validation
- 📥 **Consume** messages from topics with automatic schema detection
- 📋 **Manage schemas** in the schema registry
- 🔄 **Test** your Danube deployment end-to-end
- 🛠️ **Develop** and debug messaging workflows

## Core Concepts

### Topics
Topics are logical channels where messages are published and consumed. Topic names follow a hierarchical structure:

```
/namespace/topic-name
```
Example: `/default/user-events`, `/production/orders`

### Producers
Producers send messages to topics. They can:
- Send messages with or without schemas
- Configure partitioning for scalability
- Enable reliable delivery for critical messages

### Consumers
Consumers receive messages from topics via subscriptions. They support:
- Multiple subscription types (Exclusive, Shared, Failover)
- Automatic schema validation
- Message acknowledgment

### Schema Registry
The schema registry provides:
- Centralized schema management
- Schema evolution with compatibility checking
- Automatic validation for producers and consumers

Whether you're testing a new deployment, debugging message flows, or building automation scripts, Danube CLI has you covered!

## Quick Start

Download the latest release for your system from [Danube Releases](https://github.com/danube-messaging/danube/releases):

```bash
# Linux
wget https://github.com/danube-messaging/danube/releases/download/v0.6.0/danube-cli-linux
chmod +x danube-cli-linux

# macOS (Apple Silicon)
wget https://github.com/danube-messaging/danube/releases/download/v0.6.0/danube-cli-macos
chmod +x danube-cli-macos

# Windows
# Download danube-cli-windows.exe from the releases page
```

### Add to PATH (Optional)

For easier access, add the binary to your PATH:

```bash
# Option 1: Copy to a directory in your PATH
sudo cp danube-cli-linux /usr/local/bin/danube-cli

```

### Verify Installation

```bash
danube-cli --version
danube-cli --help
```

You should see the CLI version and help information!

Let's send and receive a simple message!

## Your First Message

### Step 1: Produce Messages

First, let's produce some messages (this also creates the topic):

```bash
danube-cli produce \
  --service-addr http://localhost:6650 \
  --topic /default/getting-started \
  --message "Hello from Danube CLI!" \
  --count 5
```

You should see:
```
✅ Producer 'test_producer' created successfully
📤 Message 1/5 sent successfully (ID: ...)
📤 Message 2/5 sent successfully (ID: ...)
📤 Message 3/5 sent successfully (ID: ...)
📤 Message 4/5 sent successfully (ID: ...)
📤 Message 5/5 sent successfully (ID: ...)
📊 Summary:
   ✅ Success: 5
```

### Step 2: Start a Consumer

Now open a **new terminal** and start consuming the messages:

```bash
danube-cli consume \
  --service-addr http://localhost:6650 \
  --topic /default/getting-started \
  --subscription my-first-subscription
```

You should see:
```
🔍 Checking for schema associated with topic...
ℹ️  Topic has no schema - consuming raw bytes
Received message: Hello from Danube CLI!
Size: 24 bytes, Total received: 24 bytes
Received message: Hello from Danube CLI!
Size: 24 bytes, Total received: 48 bytes
...
```

## Example Workflows

### Add a Schema

Schemas ensure your messages have the right structure:

```bash
# 1. Create a simple schema file
cat > /tmp/user-schema.json << 'EOF'
{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"},
    "action": {"type": "string"}
  },
  "required": ["user_id", "action"]
}
EOF

# 2. Register the schema
danube-cli schema register user-events \
  --schema-type json_schema \
  --file /tmp/user-schema.json

# 3. Produce with schema validation
danube-cli produce \
  -s http://localhost:6650 \
  -t /default/user-events \
  --schema-subject user-events \
  -m '{"user_id":"user_123","action":"login"}' \
  --count 5

# 4. Consume with automatic validation
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/user-events \
  -m user-subscription
```

The consumer will automatically validate messages against the schema!

### Send Multiple Messages

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -t /default/test \
  -m "Message" \
  --count 20 \
  --interval 500
```

This sends 20 messages with a 500ms delay between each.

### Use Different Subscription Types

```bash
# Exclusive: Only one consumer at a time
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/test \
  -m my-exclusive \
  --sub-type exclusive

# Shared: Multiple consumers share messages
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/test \
  -m my-shared \
  --sub-type shared
```

## Common Patterns

### Pattern 1: Quick Test Message

```bash
# Shortest way to send a message
danube-cli produce -s http://localhost:6650 -m "test"
```

### Pattern 2: Binary Files

```bash
# Send a file as a message
danube-cli produce \
  -s http://localhost:6650 \
  --file /path/to/data.bin
```

### Pattern 3: Messages with Metadata

```bash
# Add attributes for routing/filtering
danube-cli produce \
  -s http://localhost:6650 \
  -m "Alert!" \
  --attributes "priority:high,region:us-west"
```

### Pattern 4: Partitioned Topics

```bash
# Create a topic with partitions
danube-cli produce \
  -s http://localhost:6650 \
  -t /default/events \
  --partitions 4 \
  -m "Partitioned message"
```

### Pattern 5: Reliable Delivery

```bash
# Guarantee message delivery
danube-cli produce \
  -s http://localhost:6650 \
  -m "Important message" \
  --reliable
```

## Tips for Success

### 1. Check Examples in Help

Every command has examples built-in:

```bash
danube-cli produce --help     # See producer examples
danube-cli consume --help     # See consumer examples
danube-cli schema --help      # See schema examples
```

### 2. JSON Output for Scripting

Use `--output json` for programmatic parsing:

```bash
danube-cli schema get user-events --output json | jq .
```

### 3. Descriptive Names

Use meaningful names for easier debugging:

```bash
danube-cli produce \
  --producer-name order-service-producer \
  --topic /production/orders \
  -m '{"order_id":"123"}'
```

## What's Next?

You can explore more advanced features:

1. 📤 **[Producer Guide](https://danube-docs.dev-state.com/danube_clis/danube_cli/getting-started/)** - CLI getting started
2. 📤 **[Producer Guide](https://danube-docs.dev-state.com/danube_clis/danube_cli/producer/)** - Message production
3. 📥 **[Consumer Guide](https://danube-docs.dev-state.com/danube_clis/danube_cli/consumer/)** - Message consumption
4. 📋 **[Schema Registry](https://danube-docs.dev-state.com/danube_clis/danube_cli/schema_registry/)** - Schema management
# Getting Started with Danube CLI

Welcome! This guide will get you up and running with Danube CLI in just a few minutes. ⚡

## Prerequisites

Before you begin, ensure you have:
- ✅ Rust toolchain installed (for building from source)
- ✅ Access to a running Danube broker
- ✅ Basic understanding of messaging concepts (optional but helpful)

## Installation

### Build from Source

```bash
# Clone the repository (if you haven't already)
git clone https://github.com/danube-messaging/danube.git
cd danube

# Build the CLI
cargo build --release -p danube-cli

# The binary is now available at:
# ./target/release/danube-cli
```

### Add to PATH (Optional)

For easier access, add the binary to your PATH:

```bash
# Option 1: Copy to a directory in your PATH
sudo cp target/release/danube-cli /usr/local/bin/

# Option 2: Add the target directory to your PATH
export PATH="$PATH:$(pwd)/target/release"
```

### Verify Installation

```bash
danube-cli --version
danube-cli --help
```

You should see the CLI version and help information!

## Understanding the Basics

### Connection Parameters

The CLI needs to know where your Danube broker is running:

```bash
-s, --service-addr <URL>    # Broker URL (default: http://127.0.0.1:6650)
```

### Topics

Topics are where messages are published and consumed:
- Format: `/namespace/topic-name`
- Examples: `/default/events`, `/production/orders`

### The Three Commands

```bash
danube-cli produce   # Send messages
danube-cli consume   # Receive messages  
danube-cli schema    # Manage schemas
```

## Your First Workflow

Let's send and receive a simple message!

### Step 1: Start a Consumer

Open a terminal and start consuming:

```bash
danube-cli consume \
  --service-addr http://localhost:6650 \
  --topic /default/getting-started \
  --subscription my-first-subscription
```

Leave this running! You should see:
```
🔍 Checking for schema associated with topic...
ℹ️  Topic has no schema - consuming raw bytes
```

### Step 2: Produce a Message

Open a **new terminal** and send a message:

```bash
danube-cli produce \
  --service-addr http://localhost:6650 \
  --topic /default/getting-started \
  --message "Hello from Danube CLI!"
```

You should see:
```
✅ Producer 'test_producer' created successfully
📤 Message 1/1 sent successfully (ID: ...)
📊 Summary:
   ✅ Success: 1
```

### Step 3: See the Message

Switch back to your consumer terminal. You should see your message!

```
==================================================
Message Received
==================================================
Topic Offset: 0
Producer Name: test_producer
Message: Hello from Danube CLI!
Size: 24 bytes
==================================================
```

🎉 **Congratulations!** You just completed your first Danube workflow!

## Next Steps

Now that you've sent your first message, let's explore more features:

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
  --type json_schema \
  --file /tmp/user-schema.json

# 3. Produce with schema validation
danube-cli produce \
  -s http://localhost:6650 \
  -t /default/user-events \
  --schema-subject user-events \
  -m '{"user_id":"user_123","action":"login"}'

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
  --count 10 \
  --interval 500
```

This sends 10 messages with a 500ms delay between each.

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

### 1. Use Tab Completion

Many shells support tab completion for CLI arguments. Type the first few letters and press TAB.

### 2. Check Examples in Help

Every command has examples built-in:

```bash
danube-cli produce --help     # See producer examples
danube-cli consume --help     # See consumer examples
danube-cli schema --help      # See schema examples
```

### 3. JSON Output for Scripting

Use `--output json` for programmatic parsing:

```bash
danube-cli schema get user-events --output json | jq .
```

### 4. Descriptive Names

Use meaningful names for easier debugging:

```bash
danube-cli produce \
  --producer-name order-service-producer \
  --topic /production/orders \
  -m '{"order_id":"123"}'
```

### 5. Test Locally First

Always test with a local Danube instance before using production:

```bash
# Local testing
-s http://localhost:6650

# Production (after testing!)
-s http://production-broker.example.com:6650
```

## Quick Reference

### Producer Shortcuts
```bash
-s = --service-addr
-t = --topic
-m = --message
-n = --producer-name
-c = --count
-i = --interval
-a = --attributes
-p = --partitions
```

### Consumer Shortcuts
```bash
-s = --service-addr
-t = --topic
-m = --subscription
-n = --consumer-name
```

### Schema Shortcuts
```bash
-s = --service-addr
-t = --type
-f = --file
-o = --output
```

## Troubleshooting

### "Connection refused"
- Verify the broker is running
- Check the service address is correct
- Ensure no firewall is blocking the port

### "Topic not found"
- Topics are created automatically on first use
- Check for typos in topic names

### "Schema validation failed"
- Verify your message matches the schema
- Check the schema with: `danube-cli schema get <subject>`
- Test compatibility with: `danube-cli schema check`

## What's Next?

Now that you've got the basics down, explore more advanced features:

1. 📤 **[Producer Basics](./producer-basics.md)** - Master message production
2. 📥 **[Consumer Basics](./consumer-basics.md)** - Master message consumption
3. 📋 **[Schema Registry](./schema-registry.md)** - Learn schema management
4. 🚀 **[Advanced Features](./producer-advanced.md)** - High-performance patterns
5. 🎯 **[Real Examples](./examples.md)** - See it all in action

## Need Help?

- 📖 Read the [full documentation](./README.md)
- 💬 Run `danube-cli --help` for command reference
- 🔍 Check out [common patterns](./examples.md)

---

Ready to dive deeper? Let's explore [Producer Basics](./producer-basics.md)! 📤

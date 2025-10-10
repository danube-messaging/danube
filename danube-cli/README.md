# danube-cli

Danube CLI is a command-line interface for interacting with Danube message broker. It provides easy-to-use commands for producing and consuming messages, using configuring message delivery options.

## Features

- Message production with configurable schemas (bytes, string, int64, JSON)
- Message consumption with shared or exclusive subscriptions
- Reliable message delivery with configurable storage options
  - In-memory storage
  - Disk storage
  - S3 storage (not functional yet)
- Custom message attributes support
- Topic partitioning
- Flexible retention policies

## Example produce usage

#### Basic message production

```bash
danube-cli produce --service-addr http://localhost:6650 --count 100 --message "Hello Danube"
```

#### Producing with JSON schema

```bash
danube-cli produce -s <http://localhost:6650> -c 100 -y json --json-schema '{"type":"object"}' -m '{"key":"Hello Danube"}'
```

#### Reliable message delivery

```bash
danube-cli produce -s <http://localhost:6650> -m "Hello Danube" -c 100 \
        --reliable
```

#### Producing with attributes

``` bash
danube-cli produce -s <http://localhost:6650> -m "Hello Danube" -a "key1:value1,key2:value2"
```

## Example consume usage

#### Receive messages from a shared subscription (default)

```bash
danube-cli consume --service-addr http://localhost:6650 --subscription my_shared_subscription
```

#### Receive messages from an exclusive subscription

```bash
danube-cli consume -s http://localhost:6650 -m my_exclusive --sub-type exclusive
```

#### Receive messages for a custom consumer name

```bash
danube-cli consume -s http://localhost:6650 -n my_consumer -m my_subscription
```

#### Receive messages from a specific topic

```bash
danube-cli consume -s http://localhost:6650 -t my_topic -m my_subscription
```

# redis-kit

A reusable Redis caching framework for Python.

## Features

- **Key Object System**: Unified key definition, automatic formatting, TTL management
- **Cache Manager**: Singleton pattern, auto-retry, connection pooling
- **Routing Strategies**: Configurable shard routing (hash, range, consistent hash)
- **Distributed Lock**: Single lock, multi-lock, read-write lock
- **Delay Queue**: Task scheduling based on Redis Sorted Set

## Installation

```bash
pip install redis-kit

# With YAML support
pip install redis-kit[yaml]

# With metrics support
pip install redis-kit[metrics]
```

## Quick Start

### Basic Usage

```python
from redis_kit import RedisClient
from redis_kit.types import NodeConfig

# Create client
config = NodeConfig(host="localhost", port=6379)
client = RedisClient(config)

# Basic operations
client.set("key", "value")
value = client.get("key")
```

### Key Object System

```python
from redis_kit.keys import StringKey, HashKey

# Define a key template
user_key = StringKey(
    key_tpl="user:{user_id}:info",
    ttl=3600,
    backend="cache",
    key_prefix="myapp"
)

# Generate key
key = user_key.get_key(user_id=123, shard_key="123")
print(key)  # myapp.user:123:info
```

### Distributed Lock

```python
from redis_kit.lock import RedisLock, service_lock

# Context manager
with RedisLock(client, "my_lock", ttl=30) as lock:
    # Critical section
    pass

# Decorator
@service_lock(client, "service_lock", ttl=60)
def my_service():
    pass
```

### Delay Queue

```python
from redis_kit.queue import DelayQueue, DelayQueueWorker

# Create queue
queue = DelayQueue(client, "my_queue")

# Push delayed task
queue.push("task_1", {"data": "value"}, delay_seconds=60)

# Process tasks
def handler(task_id, task_data):
    print(f"Processing {task_id}: {task_data}")

worker = DelayQueueWorker(queue, handler)
worker.start()
```

### Routing

```python
from redis_kit import RedisProxy
from redis_kit.routing import HashRoutingStrategy, RoutingManager
from redis_kit.core.node import RedisNode
from redis_kit.types import NodeConfig

# Create nodes
nodes = [
    RedisNode(NodeConfig(host="redis1", port=6379)),
    RedisNode(NodeConfig(host="redis2", port=6379)),
]

# Create routing manager
routing_manager = RoutingManager(HashRoutingStrategy(), nodes)

# Create proxy
proxy = RedisProxy("cache", routing_manager=routing_manager)

# Operations are automatically routed
proxy.set("key", "value")
```

## Configuration

### Using Dict

```python
from redis_kit.config import load_config

config = load_config(config={
    "nodes": {
        "default": {"host": "localhost", "port": 6379}
    },
    "key_prefix": "myapp",
})
```

### Using YAML

```yaml
# config.yaml
nodes:
  default:
    host: localhost
    port: 6379
  backup:
    host: localhost
    port: 6380

key_prefix: myapp
routing_strategy: hash
```

```python
config = load_config(yaml_path="config.yaml")
```

### Using Environment Variables

```bash
export REDIS_KIT_NODE_DEFAULT_HOST=localhost
export REDIS_KIT_NODE_DEFAULT_PORT=6379
export REDIS_KIT_KEY_PREFIX=myapp
```

```python
config = load_config()  # Auto-loads from env
```

## License

MIT License

"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit: A reusable Redis caching framework

提供完整的 Redis 缓存管理解决方案，包括：
- 键对象系统：统一键定义、自动格式化、TTL 管理
- 缓存管理器：单例模式、自动重试、连接池
- 通用分片路由：可配置的路由策略接口
- 分布式锁：单锁、多锁、读写锁
- 延迟队列：基于 Sorted Set 的任务延迟执行

Usage:
    from redis_kit import RedisProxy, RedisLock, DelayQueue
    from redis_kit.keys import StringKey, HashKey
    from redis_kit.routing import HashRoutingStrategy, RangeRoutingStrategy

Example:
    # 基础使用
    >>> from redis_kit import RedisClient
    >>> from redis_kit.types import NodeConfig
    >>> config = NodeConfig(host="localhost", port=6379)
    >>> client = RedisClient(config)
    >>> client.set("key", "value")
    >>> client.get("key")
    'value'

    # 使用键对象
    >>> from redis_kit.keys import StringKey
    >>> user_key = StringKey(key_tpl="user:{user_id}:info", ttl=3600, backend="cache")
    >>> key = user_key.get_key(user_id=123)

    # 使用分布式锁
    >>> from redis_kit.lock import RedisLock
    >>> with RedisLock(client, "my_lock", ttl=30):
    ...     # 临界区代码
    ...     pass
"""

__version__ = "1.0.0"
__author__ = "BlueKing Monitor Team"

# =============================================================================
# 类型定义
# =============================================================================
from redis_kit.types import (
    BackendConfig,
    CircuitBreakerConfig,
    NodeConfig,
    PoolConfig,
    RedisClientProtocol,
    RedisNodeProtocol,
    RetryPolicy,
    RoutingStrategyProtocol,
    RoutingTable,
    RoutingTableEntry,
    ShardedKey,
)

# =============================================================================
# 异常
# =============================================================================
from redis_kit.exceptions import (
    AuthenticationError,
    CircuitBreakerError,
    CircuitOpenError,
    ConfigurationError,
    ConnectionRefusedError,
    ConnectionTimeoutError,
    InvalidConfigError,
    InvalidShardKeyError,
    KeyFormatError,
    KeyTemplateError,
    LockAcquisitionError,
    LockError,
    LockExtendError,
    LockReleaseError,
    MissingConfigError,
    NoAvailableNodeError,
    QueueError,
    RedisConnectionError,
    RedisKeyError,
    RedisKitError,
    RetryExhaustedError,
    RoutingError,
    RoutingTableError,
    TaskAlreadyExistsError,
    TaskNotFoundError,
)

# 向后兼容别名（已废弃，建议使用新名称）
ConnectionError = RedisConnectionError  # 已废弃：使用 RedisConnectionError
KeyError = RedisKeyError  # 已废弃：使用 RedisKeyError

# =============================================================================
# 核心模块
# =============================================================================
from redis_kit.core.client import RedisClient, SentinelRedisClient, create_client
from redis_kit.core.node import RedisNode, SentinelRedisNode, create_node
from redis_kit.core.pipeline import PipelineProxy, TransactionProxy
from redis_kit.core.proxy import KeyExtractor, RedisProxy
from redis_kit.core.retry import RetryContext, RetryHandler, with_retry

# =============================================================================
# 键对象系统
# =============================================================================
from redis_kit.keys.base import RedisDataKey
from redis_kit.keys.types import HashKey, ListKey, SetKey, SortedSetKey, StringKey

# =============================================================================
# 路由模块
# =============================================================================
from redis_kit.routing.base import BaseRoutingStrategy, RoutingManager
from redis_kit.routing.hash import ConsistentHashRoutingStrategy, HashRoutingStrategy
from redis_kit.routing.range import ModuloRoutingStrategy, RangeRoutingStrategy

# =============================================================================
# 分布式锁
# =============================================================================
from redis_kit.lock.base import BaseLock
from redis_kit.lock.decorators import (
    RateLimitExceededError,
    rate_limited,
    service_lock,
    singleton_task,
)
from redis_kit.lock.redis_lock import MultiRedisLock, ReadWriteLock, RedisLock

# =============================================================================
# 延迟队列
# =============================================================================
from redis_kit.queue.delay_queue import DelayQueue, DelayQueueManager, DelayQueueWorker

# =============================================================================
# 配置模块
# =============================================================================
from redis_kit.config.loader import (
    CompositeConfigLoader,
    ConfigLoader,
    DictConfigLoader,
    EnvConfigLoader,
    YamlConfigLoader,
    load_config,
)
from redis_kit.config.schema import RedisKitConfig

# =============================================================================
# 公共 API
# =============================================================================
__all__ = [
    # Version
    "__version__",
    # Types
    "ShardedKey",
    "RetryPolicy",
    "CircuitBreakerConfig",
    "PoolConfig",
    "NodeConfig",
    "BackendConfig",
    "RoutingTable",
    "RoutingTableEntry",
    "RedisClientProtocol",
    "RedisNodeProtocol",
    "RoutingStrategyProtocol",
    # Exceptions
    "RedisKitError",
    "ConfigurationError",
    "InvalidConfigError",
    "MissingConfigError",
    "RedisConnectionError",
    "ConnectionError",  # 向后兼容别名
    "ConnectionTimeoutError",
    "ConnectionRefusedError",
    "AuthenticationError",
    "RoutingError",
    "NoAvailableNodeError",
    "InvalidShardKeyError",
    "RoutingTableError",
    "LockError",
    "LockAcquisitionError",
    "LockReleaseError",
    "LockExtendError",
    "CircuitBreakerError",
    "CircuitOpenError",
    "QueueError",
    "TaskNotFoundError",
    "TaskAlreadyExistsError",
    "RedisKeyError",
    "KeyError",  # 向后兼容别名
    "KeyTemplateError",
    "KeyFormatError",
    "RetryExhaustedError",
    # Core
    "RedisClient",
    "SentinelRedisClient",
    "create_client",
    "RedisNode",
    "SentinelRedisNode",
    "create_node",
    "RedisProxy",
    "KeyExtractor",
    "PipelineProxy",
    "TransactionProxy",
    "RetryHandler",
    "RetryContext",
    "with_retry",
    # Keys
    "RedisDataKey",
    "StringKey",
    "HashKey",
    "SetKey",
    "ListKey",
    "SortedSetKey",
    # Routing
    "BaseRoutingStrategy",
    "RoutingManager",
    "HashRoutingStrategy",
    "ConsistentHashRoutingStrategy",
    "RangeRoutingStrategy",
    "ModuloRoutingStrategy",
    # Lock
    "BaseLock",
    "RedisLock",
    "MultiRedisLock",
    "ReadWriteLock",
    "service_lock",
    "singleton_task",
    "rate_limited",
    "RateLimitExceededError",
    # Queue
    "DelayQueue",
    "DelayQueueWorker",
    "DelayQueueManager",
    # Config
    "RedisKitConfig",
    "ConfigLoader",
    "DictConfigLoader",
    "YamlConfigLoader",
    "EnvConfigLoader",
    "CompositeConfigLoader",
    "load_config",
]

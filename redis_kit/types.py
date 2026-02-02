"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit 类型定义模块

定义所有协议接口、数据类型和配置类
"""

import builtins
from dataclasses import dataclass, field
from typing import Any, Protocol, TypeVar
from collections.abc import Callable

T = TypeVar("T")


# =============================================================================
# 协议定义
# =============================================================================


class RedisClientProtocol(Protocol):
    """Redis 客户端协议"""

    def get(self, name: str) -> str | None: ...

    def set(self, name: str, value: str, ex: int = None, nx: bool = False) -> bool: ...

    def delete(self, *names: str) -> int: ...

    def exists(self, *names: str) -> int: ...

    def expire(self, name: str, time: int) -> bool: ...

    def pipeline(self, transaction: bool = True) -> Any: ...


class RedisNodeProtocol(Protocol):
    """Redis 节点协议"""

    @property
    def node_id(self) -> str:
        """节点唯一标识"""
        ...

    @property
    def is_available(self) -> bool:
        """节点是否可用"""
        ...

    def get_client(self, backend: str) -> RedisClientProtocol:
        """获取指定后端的客户端实例"""
        ...


class RoutingStrategyProtocol(Protocol):
    """路由策略协议"""

    def route(
        self, shard_key: str | None, nodes: list[RedisNodeProtocol]
    ) -> RedisNodeProtocol:
        """根据分片键选择节点"""
        ...


class MetricsCollectorProtocol(Protocol):
    """监控指标收集器协议"""

    def record_command(
        self, cmd: str, duration: float, success: bool, node_id: str
    ) -> None:
        """记录命令执行指标"""
        ...

    def record_connection(self, node_id: str, event: str) -> None:
        """记录连接事件"""
        ...


class MiddlewareProtocol(Protocol):
    """中间件协议"""

    def wrap(self, func: Callable, command_name: str, node_id: str) -> Callable:
        """包装命令执行函数"""
        ...


# =============================================================================
# 数据类型定义
# =============================================================================


@dataclass(frozen=True)
class ShardedKey:
    """
    带分片信息的 Redis 键

    使用不可变 dataclass 替代原有的 SimilarStr，解决类变量共享问题。
    frozen=True 确保线程安全。

    Attributes:
        key: Redis 键字符串
        shard_key: 分片键，用于路由选择节点

    Example:
        >>> key = ShardedKey(key="user:123", shard_key="123")
        >>> str(key)
        'user:123'
        >>> key.shard_key
        '123'
    """

    key: str
    shard_key: str | None = None

    def __str__(self) -> str:
        return self.key

    def __hash__(self) -> int:
        return hash(self.key)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            return self.key == other
        if isinstance(other, ShardedKey):
            return self.key == other.key
        return False

    def __repr__(self) -> str:
        return f"ShardedKey(key={self.key!r}, shard_key={self.shard_key!r})"

    def with_shard_key(self, shard_key: str) -> "ShardedKey":
        """创建带有新分片键的副本"""
        return ShardedKey(key=self.key, shard_key=shard_key)


@dataclass
class RetryPolicy:
    """
    重试策略配置

    Attributes:
        max_retries: 最大重试次数
        backoff_base: 退避基础时间（秒）
        backoff_max: 最大退避时间（秒）
        backoff_multiplier: 退避时间乘数
        retryable_exceptions: 可重试的异常类型元组
    """

    max_retries: int = 3
    backoff_base: float = 0.1
    backoff_max: float = 5.0
    backoff_multiplier: float = 2.0
    retryable_exceptions: tuple[type[Exception], ...] = field(
        default_factory=lambda: _get_default_retryable_exceptions()
    )


def _get_default_retryable_exceptions() -> tuple[type[Exception], ...]:
    """
    获取默认可重试异常列表，包含 redis 库和内置的连接相关异常

    使用 builtins.ConnectionError 而非直接使用 ConnectionError 是为了：
    1. 明确区分内置异常和可能的第三方库同名异常
    2. 在不同上下文中保持异常引用的一致性
    3. 提高代码的可读性和可维护性
    """
    exceptions: list[type[Exception]] = [
        builtins.ConnectionError,
        builtins.TimeoutError,
    ]
    try:
        import redis

        exceptions.extend([redis.ConnectionError, redis.TimeoutError])
    except ImportError:
        pass
    return tuple(exceptions)


@dataclass
class CircuitBreakerConfig:
    """
    断路器配置

    Attributes:
        enabled: 是否启用断路器
        failure_threshold: 触发断路的失败次数阈值
        recovery_timeout: 断路器恢复超时时间（秒）
        half_open_requests: 半开状态允许的请求数
    """

    enabled: bool = True
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_requests: int = 3


@dataclass
class PoolConfig:
    """
    连接池配置

    Attributes:
        max_connections: 最大连接数
        min_idle_connections: 最小空闲连接数
        idle_check_interval: 空闲检查间隔（秒）
        connection_timeout: 连接超时时间（秒）
    """

    max_connections: int = 10
    min_idle_connections: int = 1
    idle_check_interval: float = 30.0
    connection_timeout: float = 5.0


@dataclass
class NodeConfig:
    """
    Redis 节点配置

    Attributes:
        host: 主机地址
        port: 端口号
        password: 密码
        db: 数据库编号
        cache_type: 缓存类型 (redis/sentinel)
        master_name: Sentinel 主节点名称
        sentinel_password: Sentinel 密码
        socket_timeout: Socket 超时时间
        socket_connect_timeout: Socket 连接超时时间
        decode_responses: 是否解码响应
        encoding: 编码格式
    """

    host: str = "localhost"
    port: int = 6379
    password: str | None = None
    db: int = 0
    cache_type: str = "redis"  # redis or sentinel
    master_name: str | None = None
    sentinel_password: str | None = None
    socket_timeout: float | None = None
    socket_connect_timeout: float | None = None
    decode_responses: bool = True
    encoding: str = "utf-8"

    def to_connection_kwargs(self) -> dict[str, Any]:
        """转换为 redis-py 连接参数"""
        kwargs = {
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "decode_responses": self.decode_responses,
            "encoding": self.encoding,
        }
        if self.password:
            kwargs["password"] = self.password
        if self.socket_timeout:
            kwargs["socket_timeout"] = self.socket_timeout
        if self.socket_connect_timeout:
            kwargs["socket_connect_timeout"] = self.socket_connect_timeout
        if self.cache_type == "sentinel" and self.master_name:
            kwargs["master_name"] = self.master_name
            if self.sentinel_password:
                kwargs["sentinel_password"] = self.sentinel_password
        return kwargs


@dataclass
class RoutingTableEntry:
    """
    路由表条目

    Attributes:
        score: 路由分数（范围上限）
        node_id: 节点 ID
    """

    score: int
    node_id: str


@dataclass
class BackendConfig:
    """
    后端配置

    Attributes:
        name: 后端名称
        db: 数据库编号
        description: 描述
    """

    name: str
    db: int
    description: str = ""


# =============================================================================
# 类型别名
# =============================================================================

# 路由表类型：[(score, node_id), ...]
RoutingTable = list[tuple[int, str]]

# 节点配置字典类型
NodesConfig = dict[str, NodeConfig]

# 后端配置字典类型
BackendsConfig = dict[str, BackendConfig]

"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit Redis 代理模块

提供 Redis 命令的路由代理功能
"""

import logging
from typing import TYPE_CHECKING, Any
from collections.abc import Callable

from redis_kit.core.retry import RetryHandler
from redis_kit.routing.base import BaseRoutingStrategy, RoutingManager
from redis_kit.routing.hash import HashRoutingStrategy
from redis_kit.types import (
    MiddlewareProtocol,
    RedisNodeProtocol,
    RetryPolicy,
    ShardedKey,
)

if TYPE_CHECKING:
    from redis_kit.core.pipeline import PipelineProxy

logger = logging.getLogger(__name__)


class KeyExtractor:
    """
    键提取器

    从 Redis 命令参数中提取键对象。
    """

    # 不需要键参数的命令
    NO_KEY_COMMANDS = frozenset(
        [
            "ping",
            "echo",
            "info",
            "dbsize",
            "flushdb",
            "flushall",
            "save",
            "bgsave",
            "lastsave",
            "shutdown",
            "slaveof",
            "debug",
            "config",
            "subscribe",
            "psubscribe",
            "unsubscribe",
            "punsubscribe",
            "publish",
            "watch",
            "unwatch",
            "multi",
            "exec",
            "discard",
            "script",
            "eval",
            "evalsha",
            "slowlog",
            "object",
            "client",
            "time",
            "cluster",
            "sentinel",
        ]
    )

    @classmethod
    def extract(cls, command_name: str, args: tuple, kwargs: dict) -> ShardedKey | None:
        """
        从命令参数中提取键

        Args:
            command_name: 命令名称
            args: 位置参数
            kwargs: 关键字参数

        Returns:
            ShardedKey 对象，无键时返回 None
        """
        # 某些命令不需要键
        if command_name.lower() in cls.NO_KEY_COMMANDS:
            return None

        # 优先从 kwargs 中获取
        key = kwargs.get("name") or kwargs.get("key")

        # 从 args 中获取第一个参数
        if key is None and args:
            key = args[0]

        if key is None:
            return None

        if isinstance(key, ShardedKey):
            return key
        elif isinstance(key, str):
            return ShardedKey(key=key)

        return None

    @classmethod
    def extract_shard_key(
        cls, command_name: str, args: tuple, kwargs: dict
    ) -> str | None:
        """
        从命令参数中提取分片键

        Args:
            command_name: 命令名称
            args: 位置参数
            kwargs: 关键字参数

        Returns:
            分片键字符串，无键时返回 None
        """
        key = cls.extract(command_name, args, kwargs)
        return key.shard_key if key else None


class RedisProxy:
    """
    Redis 代理

    负责将 Redis 命令路由到正确的节点执行。
    支持路由策略、重试、中间件等功能。

    Attributes:
        backend: 后端名称
        routing_manager: 路由管理器
        retry_handler: 重试处理器
        middlewares: 中间件列表

    Example:
        >>> proxy = RedisProxy("cache", routing_manager=manager)
        >>> proxy.set("key", "value")
        >>> proxy.get("key")
        'value'
    """

    def __init__(
        self,
        backend: str,
        routing_manager: RoutingManager = None,
        routing_strategy: BaseRoutingStrategy = None,
        nodes: list[RedisNodeProtocol] = None,
        retry_policy: RetryPolicy = None,
        middlewares: list[MiddlewareProtocol] = None,
        db: int = None,
    ):
        """
        初始化 Redis 代理

        Args:
            backend: 后端名称
            routing_manager: 路由管理器（优先使用）
            routing_strategy: 路由策略（无 routing_manager 时使用）
            nodes: 节点列表（无 routing_manager 时使用）
            retry_policy: 重试策略
            middlewares: 中间件列表
            db: 数据库编号
        """
        self.backend = backend
        self.db = db
        self._client_pool: dict[str, Any] = {}
        self._pipeline: PipelineProxy | None = None

        # 初始化路由管理器
        if routing_manager:
            self.routing_manager = routing_manager
        else:
            strategy = routing_strategy or HashRoutingStrategy()
            self.routing_manager = RoutingManager(strategy, nodes or [])

        # 初始化重试处理器
        self.retry_handler = RetryHandler(retry_policy)

        # 中间件链
        self.middlewares = middlewares or []

        # 键提取器
        self.key_extractor = KeyExtractor()

    @property
    def nodes(self) -> list[RedisNodeProtocol]:
        """获取节点列表"""
        return self.routing_manager.nodes

    def pipeline(self, transaction: bool = True) -> "PipelineProxy":
        """
        获取 Pipeline 代理

        Args:
            transaction: 是否使用事务模式

        Returns:
            PipelineProxy 实例
        """
        from redis_kit.core.pipeline import PipelineProxy

        if self._pipeline is None:
            self._pipeline = PipelineProxy(self, transaction=transaction)
        return self._pipeline

    def get_client(self, node: RedisNodeProtocol) -> Any:
        """
        获取节点的客户端实例

        Args:
            node: Redis 节点

        Returns:
            Redis 客户端实例
        """
        if node.node_id not in self._client_pool:
            self._client_pool[node.node_id] = node.get_client(self.backend, self.db)
        return self._client_pool[node.node_id]

    def get_node_by_shard_key(self, shard_key: str | None) -> RedisNodeProtocol:
        """
        根据分片键获取节点

        Args:
            shard_key: 分片键

        Returns:
            目标 Redis 节点

        Raises:
            NoAvailableNodeError: 无可用节点时抛出
        """
        return self.routing_manager.get_node_by_shard_key(shard_key)

    def refresh_client(self, node: RedisNodeProtocol) -> Any:
        """
        刷新节点客户端

        Args:
            node: Redis 节点

        Returns:
            新的客户端实例
        """
        if node.node_id in self._client_pool:
            try:
                self._client_pool[node.node_id].close()
            except Exception:
                pass
            del self._client_pool[node.node_id]

        return self.get_client(node)

    def _wrap_with_middlewares(
        self, func: Callable, command_name: str, node_id: str
    ) -> Callable:
        """
        使用中间件包装命令

        Args:
            func: 原始命令函数
            command_name: 命令名称
            node_id: 节点 ID

        Returns:
            包装后的函数
        """
        wrapped = func
        for middleware in reversed(self.middlewares):
            wrapped = middleware.wrap(wrapped, command_name, node_id)
        return wrapped

    def __getattr__(self, name: str) -> Callable:
        """
        代理 Redis 命令

        自动进行路由和重试。
        """

        def handle(*args, **kwargs):
            # 提取分片键
            shard_key = self.key_extractor.extract_shard_key(name, args, kwargs)

            # 获取目标节点
            node = self.get_node_by_shard_key(shard_key)
            client = self.get_client(node)
            command = getattr(client, name)

            # 应用中间件
            wrapped_command = self._wrap_with_middlewares(command, name, node.node_id)

            # 执行带重试的命令
            def execute():
                return wrapped_command(*args, **kwargs)

            try:
                return self.retry_handler.execute(execute)
            except Exception as e:
                # 连接错误时刷新客户端
                if "connection" in str(e).lower():
                    self.refresh_client(node)
                raise

        return handle

    def execute_on_all_nodes(
        self, command_name: str, *args, **kwargs
    ) -> dict[str, Any]:
        """
        在所有节点上执行命令

        Args:
            command_name: 命令名称
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            节点 ID 到执行结果的映射
        """
        results = {}
        for node in self.nodes:
            if not node.is_available:
                continue
            try:
                client = self.get_client(node)
                command = getattr(client, command_name)
                results[node.node_id] = command(*args, **kwargs)
            except Exception as e:
                logger.warning(
                    f"Failed to execute {command_name} on {node.node_id}: {e}"
                )
                results[node.node_id] = e
        return results

    def close(self) -> None:
        """关闭所有客户端连接"""
        for node_id, client in list(self._client_pool.items()):
            try:
                client.close()
            except Exception as e:
                logger.warning(f"Failed to close client for {node_id}: {e}")
        self._client_pool.clear()

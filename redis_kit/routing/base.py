# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit 路由策略基础模块

定义路由策略基类和路由管理器
"""

from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Dict, List, Optional

from redis_kit.exceptions import NoAvailableNodeError
from redis_kit.types import RedisNodeProtocol, ShardedKey


class BaseRoutingStrategy(ABC):
    """
    路由策略基类

    所有路由策略必须继承此类并实现 route 方法

    Example:
        >>> class MyStrategy(BaseRoutingStrategy):
        ...     def route(self, shard_key, nodes):
        ...         # 自定义路由逻辑
        ...         return nodes[0]
    """

    @abstractmethod
    def route(
        self, shard_key: Optional[str], nodes: List[RedisNodeProtocol]
    ) -> RedisNodeProtocol:
        """
        根据分片键选择 Redis 节点

        Args:
            shard_key: 分片键，可为 None
            nodes: 可用的 Redis 节点列表

        Returns:
            选中的 Redis 节点

        Raises:
            NoAvailableNodeError: 无可用节点时抛出
        """
        pass

    def select_fallback(self, nodes: List[RedisNodeProtocol]) -> RedisNodeProtocol:
        """
        默认回退策略：选择第一个可用节点

        Args:
            nodes: Redis 节点列表

        Returns:
            第一个可用的节点

        Raises:
            NoAvailableNodeError: 无可用节点时抛出
        """
        available = [n for n in nodes if n.is_available]
        if not available:
            raise NoAvailableNodeError(reason="all nodes are unavailable")
        return available[0]

    def filter_available(
        self, nodes: List[RedisNodeProtocol]
    ) -> List[RedisNodeProtocol]:
        """
        过滤可用节点

        Args:
            nodes: Redis 节点列表

        Returns:
            可用节点列表
        """
        return [n for n in nodes if n.is_available]


class RoutingManager:
    """
    路由管理器

    负责管理路由策略和节点列表，提供统一的路由入口。
    支持路由结果缓存以提高性能。

    Attributes:
        strategy: 路由策略实例
        cache_size: LRU 缓存大小

    Example:
        >>> manager = RoutingManager(HashRoutingStrategy(), nodes)
        >>> node = manager.get_node_by_shard_key("user_123")
    """

    def __init__(
        self,
        strategy: BaseRoutingStrategy,
        nodes: List[RedisNodeProtocol] = None,
        cache_size: int = 10000,
    ):
        """
        初始化路由管理器

        Args:
            strategy: 路由策略实例
            nodes: Redis 节点列表
            cache_size: LRU 缓存大小，默认 10000
        """
        self.strategy = strategy
        self._nodes: List[RedisNodeProtocol] = nodes or []
        self._cache_size = cache_size
        self._node_map: Dict[str, RedisNodeProtocol] = {}
        self._update_node_map()
        self._setup_cache()

    def _update_node_map(self) -> None:
        """更新节点 ID 到节点的映射"""
        self._node_map = {node.node_id: node for node in self._nodes}

    def _setup_cache(self) -> None:
        """设置路由缓存"""

        @lru_cache(maxsize=self._cache_size)
        def cached_route(shard_key: Optional[str]) -> str:
            node = self.strategy.route(shard_key, self._nodes)
            return node.node_id

        self._cached_route = cached_route

    @property
    def nodes(self) -> List[RedisNodeProtocol]:
        """获取节点列表"""
        return self._nodes

    def get_node(self, key: ShardedKey) -> RedisNodeProtocol:
        """
        根据 ShardedKey 获取节点

        Args:
            key: 带分片信息的键对象

        Returns:
            目标 Redis 节点

        Raises:
            NoAvailableNodeError: 无可用节点时抛出
        """
        return self.get_node_by_shard_key(key.shard_key)

    def get_node_by_shard_key(self, shard_key: Optional[str]) -> RedisNodeProtocol:
        """
        根据分片键获取节点

        Args:
            shard_key: 分片键

        Returns:
            目标 Redis 节点

        Raises:
            NoAvailableNodeError: 无可用节点时抛出
        """
        if not self._nodes:
            raise NoAvailableNodeError(shard_key, reason="no nodes configured")

        try:
            node_id = self._cached_route(shard_key)
            node = self._node_map.get(node_id)
            if node and node.is_available:
                return node
            # 缓存的节点不可用，清除缓存重新路由
            self.invalidate_cache()
            return self.strategy.route(shard_key, self._nodes)
        except Exception:
            # 缓存路由失败，直接调用策略
            return self.strategy.route(shard_key, self._nodes)

    def get_node_by_id(self, node_id: str) -> Optional[RedisNodeProtocol]:
        """
        根据节点 ID 获取节点

        Args:
            node_id: 节点 ID

        Returns:
            对应的节点，不存在返回 None
        """
        return self._node_map.get(node_id)

    def invalidate_cache(self) -> None:
        """清除路由缓存"""
        self._cached_route.cache_clear()

    def update_nodes(self, nodes: List[RedisNodeProtocol]) -> None:
        """
        更新节点列表

        Args:
            nodes: 新的节点列表
        """
        self._nodes = nodes
        self._update_node_map()
        self.invalidate_cache()

    def add_node(self, node: RedisNodeProtocol) -> None:
        """
        添加节点

        Args:
            node: 要添加的节点
        """
        self._nodes.append(node)
        self._node_map[node.node_id] = node
        self.invalidate_cache()

    def remove_node(self, node_id: str) -> bool:
        """
        移除节点

        Args:
            node_id: 要移除的节点 ID

        Returns:
            是否成功移除
        """
        if node_id not in self._node_map:
            return False
        node = self._node_map.pop(node_id)
        self._nodes.remove(node)
        self.invalidate_cache()
        return True

    def get_cache_info(self) -> dict:
        """
        获取缓存统计信息

        Returns:
            缓存命中率等统计信息
        """
        info = self._cached_route.cache_info()
        return {
            "hits": info.hits,
            "misses": info.misses,
            "maxsize": info.maxsize,
            "currsize": info.currsize,
            "hit_rate": info.hits / (info.hits + info.misses)
            if (info.hits + info.misses) > 0
            else 0,
        }

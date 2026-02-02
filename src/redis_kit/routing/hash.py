"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit Hash 路由策略模块

提供基于 Hash 的路由策略实现
"""

from collections.abc import Callable

from redis_kit.exceptions import NoAvailableNodeError
from redis_kit.routing.base import BaseRoutingStrategy
from redis_kit.types import RedisNodeProtocol


class HashRoutingStrategy(BaseRoutingStrategy):
    """
    Hash 一致性路由策略

    根据分片键的 hash 值选择节点，确保相同分片键始终路由到相同节点。

    Attributes:
        hash_func: 自定义 hash 函数，默认使用内置 hash

    Example:
        >>> strategy = HashRoutingStrategy()
        >>> node = strategy.route("user_123", nodes)

        # 使用自定义 hash 函数
        >>> import hashlib
        >>> def md5_hash(s):
        ...     return int(hashlib.md5(s.encode()).hexdigest(), 16)
        >>> strategy = HashRoutingStrategy(hash_func=md5_hash)
    """

    def __init__(self, hash_func: Callable[[str], int] = None):
        """
        初始化 Hash 路由策略

        Args:
            hash_func: 自定义 hash 函数，接受字符串返回整数
        """
        self.hash_func = hash_func or hash

    def route(
        self, shard_key: str | None, nodes: list[RedisNodeProtocol]
    ) -> RedisNodeProtocol:
        """
        根据分片键的 hash 值选择节点

        Args:
            shard_key: 分片键
            nodes: 可用节点列表

        Returns:
            选中的节点

        Raises:
            NoAvailableNodeError: 无可用节点时抛出
        """
        available = self.filter_available(nodes)
        if not available:
            raise NoAvailableNodeError(shard_key, reason="all nodes are unavailable")

        # 无分片键时返回第一个节点
        if not shard_key:
            return available[0]

        # 计算 hash 并选择节点
        hash_value = self.hash_func(shard_key)
        index = hash_value % len(available)
        return available[index]


class ConsistentHashRoutingStrategy(BaseRoutingStrategy):
    """
    一致性 Hash 路由策略

    使用虚拟节点的一致性 hash 算法，在节点增减时最小化数据迁移。

    Attributes:
        virtual_nodes: 每个物理节点的虚拟节点数量
        hash_func: hash 函数

    Example:
        >>> strategy = ConsistentHashRoutingStrategy(virtual_nodes=150)
        >>> node = strategy.route("user_123", nodes)
    """

    def __init__(
        self, virtual_nodes: int = 150, hash_func: Callable[[str], int] = None
    ):
        """
        初始化一致性 Hash 路由策略

        Args:
            virtual_nodes: 每个物理节点的虚拟节点数量
            hash_func: 自定义 hash 函数
        """
        self.virtual_nodes = virtual_nodes
        self.hash_func = hash_func or self._default_hash
        self._ring: list[tuple] = []  # [(hash_value, node), ...]
        self._nodes_hash: int = 0  # 节点列表的 hash，用于检测变化

    def _default_hash(self, key: str) -> int:
        """默认 hash 函数，使用 FNV-1a 算法"""
        h = 2166136261
        for char in key.encode():
            h ^= char
            h = (h * 16777619) & 0xFFFFFFFF
        return h

    def _build_ring(self, nodes: list[RedisNodeProtocol]) -> None:
        """构建 hash 环"""
        nodes_hash = hash(tuple(n.node_id for n in nodes))
        if nodes_hash == self._nodes_hash and self._ring:
            return

        self._ring = []
        for node in nodes:
            if not node.is_available:
                continue
            for i in range(self.virtual_nodes):
                key = f"{node.node_id}:{i}"
                hash_value = self.hash_func(key)
                self._ring.append((hash_value, node))

        self._ring.sort(key=lambda x: x[0])
        self._nodes_hash = nodes_hash

    def route(
        self, shard_key: str | None, nodes: list[RedisNodeProtocol]
    ) -> RedisNodeProtocol:
        """
        使用一致性 hash 选择节点

        Args:
            shard_key: 分片键
            nodes: 可用节点列表

        Returns:
            选中的节点

        Raises:
            NoAvailableNodeError: 无可用节点时抛出
        """
        available = self.filter_available(nodes)
        if not available:
            raise NoAvailableNodeError(shard_key, reason="all nodes are unavailable")

        if not shard_key:
            return available[0]

        self._build_ring(available)

        if not self._ring:
            return available[0]

        # 计算 key 的 hash 值
        key_hash = self.hash_func(shard_key)

        # 二分查找顺时针最近的节点
        left, right = 0, len(self._ring)
        while left < right:
            mid = (left + right) // 2
            if self._ring[mid][0] < key_hash:
                left = mid + 1
            else:
                right = mid

        # 如果超出范围，环回到第一个节点
        if left >= len(self._ring):
            left = 0

        return self._ring[left][1]

"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit 范围路由策略模块

提供基于数值范围的路由策略实现，替代原有 strategy_score 机制
"""

from bisect import bisect_right

from redis_kit.exceptions import NoAvailableNodeError, RoutingTableError
from redis_kit.routing.base import BaseRoutingStrategy
from redis_kit.types import RedisNodeProtocol, RoutingTable


class RangeRoutingStrategy(BaseRoutingStrategy):
    """
    范围路由策略

    根据分片键的数值范围选择节点，适用于需要按 ID 范围分片的场景。
    替代原有基于 strategy_score 的路由机制。

    路由表格式: [(score, node_id), ...]
    - score 表示该节点负责的范围上限
    - 按 score 升序排列

    Example:
        路由表: [(100, "node1"), (200, "node2"), (1000, "node3")]
        - shard_key <= 100: node1
        - 100 < shard_key <= 200: node2
        - 200 < shard_key <= 1000: node3

        >>> strategy = RangeRoutingStrategy(
        ...     [
        ...         (100, "node1"),
        ...         (200, "node2"),
        ...         (1000, "node3"),
        ...     ]
        ... )
        >>> node = strategy.route("150", nodes)  # 返回 node2
    """

    def __init__(self, routing_table: RoutingTable = None):
        """
        初始化范围路由策略

        Args:
            routing_table: 路由表，格式为 [(score, node_id), ...]
        """
        self._routing_table: RoutingTable = []
        self._scores: list[int] = []
        self._node_ids: list[str] = []

        if routing_table:
            self.update_routing_table(routing_table)

    def update_routing_table(self, routing_table: RoutingTable) -> None:
        """
        更新路由表

        Args:
            routing_table: 新的路由表

        Raises:
            RoutingTableError: 路由表格式无效时抛出
        """
        if not routing_table:
            raise RoutingTableError("routing table cannot be empty")

        # 验证并排序
        try:
            sorted_table = sorted(routing_table, key=lambda x: x[0])
        except (TypeError, IndexError) as e:
            raise RoutingTableError(f"invalid routing table format: {e}")

        self._routing_table = sorted_table
        self._scores = [score for score, _ in sorted_table]
        self._node_ids = [node_id for _, node_id in sorted_table]

    def add_route(self, score: int, node_id: str) -> None:
        """
        添加路由规则

        Args:
            score: 范围上限分数
            node_id: 节点 ID
        """
        new_table = self._routing_table + [(score, node_id)]
        self.update_routing_table(new_table)

    def remove_route(self, score: int) -> bool:
        """
        移除路由规则

        Args:
            score: 要移除的范围分数

        Returns:
            是否成功移除
        """
        new_table = [(s, n) for s, n in self._routing_table if s != score]
        if len(new_table) == len(self._routing_table):
            return False
        if new_table:
            self.update_routing_table(new_table)
        else:
            self._routing_table = []
            self._scores = []
            self._node_ids = []
        return True

    def route(
        self, shard_key: str | None, nodes: list[RedisNodeProtocol]
    ) -> RedisNodeProtocol:
        """
        根据分片键的数值范围选择节点

        Args:
            shard_key: 分片键（应为数字字符串或可转换为数字）
            nodes: 可用节点列表

        Returns:
            选中的节点

        Raises:
            NoAvailableNodeError: 无可用节点或分片键超出范围时抛出
        """
        if not nodes:
            raise NoAvailableNodeError(shard_key, reason="no nodes provided")

        available = self.filter_available(nodes)
        if not available:
            raise NoAvailableNodeError(shard_key, reason="all nodes are unavailable")

        # 无分片键或无路由表时返回默认节点
        if not shard_key or not self._scores:
            return self.select_fallback(available)

        # 尝试将分片键转换为整数
        try:
            key_value = int(shard_key)
        except (ValueError, TypeError):
            # 非整数分片键使用 hash 路由
            key_value = hash(shard_key) % (max(self._scores) if self._scores else 1)

        # 二分查找对应的节点
        idx = bisect_right(self._scores, key_value)
        if idx >= len(self._node_ids):
            raise NoAvailableNodeError(
                shard_key, reason=f"shard_key {key_value} exceeds maximum score"
            )

        target_node_id = self._node_ids[idx]

        # 查找对应节点
        for node in available:
            if node.node_id == target_node_id:
                return node

        # 目标节点不可用，使用回退策略
        return self.select_fallback(available)

    def get_routing_table(self) -> RoutingTable:
        """
        获取当前路由表

        Returns:
            路由表副本
        """
        return self._routing_table.copy()

    def get_node_id_for_key(self, shard_key: str) -> str | None:
        """
        获取分片键对应的节点 ID（不检查节点可用性）

        Args:
            shard_key: 分片键

        Returns:
            节点 ID，无匹配返回 None
        """
        if not shard_key or not self._scores:
            return None

        try:
            key_value = int(shard_key)
        except (ValueError, TypeError):
            key_value = hash(shard_key) % (max(self._scores) if self._scores else 1)

        idx = bisect_right(self._scores, key_value)
        if idx >= len(self._node_ids):
            return None

        return self._node_ids[idx]


class ModuloRoutingStrategy(BaseRoutingStrategy):
    """
    取模路由策略

    根据分片键对节点数量取模选择节点，简单高效。

    Example:
        >>> strategy = ModuloRoutingStrategy()
        >>> # 假设有 3 个节点
        >>> node = strategy.route("100", nodes)  # 100 % 3 = 1, 选择 nodes[1]
    """

    def route(
        self, shard_key: str | None, nodes: list[RedisNodeProtocol]
    ) -> RedisNodeProtocol:
        """
        根据分片键取模选择节点

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

        try:
            key_value = int(shard_key)
        except (ValueError, TypeError):
            key_value = hash(shard_key)

        index = key_value % len(available)
        return available[index]

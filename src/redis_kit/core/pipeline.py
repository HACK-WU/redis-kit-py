"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit Pipeline 代理模块

提供跨节点 Pipeline 批量操作支持
"""

import logging
from typing import TYPE_CHECKING, Any
from collections.abc import Callable

from redis_kit.core.proxy import KeyExtractor

if TYPE_CHECKING:
    from redis_kit.core.proxy import RedisProxy

logger = logging.getLogger(__name__)


class PipelineProxy:
    """
    Pipeline 代理

    支持跨节点的 Pipeline 批量操作，自动按节点分组执行。

    Attributes:
        node_proxy: Redis 代理实例
        transaction: 是否使用事务模式

    Example:
        >>> proxy = RedisProxy("cache", routing_manager=manager)
        >>> pipe = proxy.pipeline()
        >>> pipe.set("key1", "value1")
        >>> pipe.set("key2", "value2")
        >>> results = pipe.execute()
    """

    # 允许直接调用的方法（不需要键参数）
    ALLOWED_METHODS = frozenset(["execute", "reset", "watch", "multi"])

    def __init__(self, node_proxy: "RedisProxy", transaction: bool = True):
        """
        初始化 Pipeline 代理

        Args:
            node_proxy: Redis 代理实例
            transaction: 是否使用事务模式
        """
        self.node_proxy = node_proxy
        self.transaction = transaction
        self._pipeline_pool: dict[str, Any] = {}
        # 记录每个命令的 (node_id, cmd_index)，确保结果顺序正确
        self._command_stack: list[tuple[str, int]] = []
        # 每个节点的命令计数器
        self._node_cmd_counts: dict[str, int] = {}
        self._key_extractor = KeyExtractor()

    def _get_pipeline(self, node_id: str) -> Any:
        """
        获取节点的 Pipeline 实例

        Args:
            node_id: 节点 ID

        Returns:
            Pipeline 实例
        """
        if node_id not in self._pipeline_pool:
            node = self.node_proxy.routing_manager.get_node_by_id(node_id)
            if node is None:
                raise ValueError(f"Node not found: {node_id}")
            client = self.node_proxy.get_client(node)
            self._pipeline_pool[node_id] = client.pipeline(transaction=self.transaction)
        return self._pipeline_pool[node_id]

    def execute(self) -> list[Any]:
        """
        执行所有 Pipeline 命令

        按节点分组执行，然后按原始命令顺序返回结果。

        Returns:
            命令执行结果列表（按添加顺序）
        """
        if not self._command_stack:
            return []

        # 按节点执行 Pipeline
        node_results: dict[str, list[Any]] = {}
        for node_id, pipeline in self._pipeline_pool.items():
            try:
                results = pipeline.execute()
                # 保持原始顺序，通过索引访问
                node_results[node_id] = results
            except Exception as e:
                logger.error(f"Pipeline execute failed for node {node_id}: {e}")
                node_results[node_id] = []

        # 按命令顺序组装结果，使用 (node_id, cmd_index) 精确定位
        results = []
        for node_id, cmd_index in self._command_stack:
            node_result_list = node_results.get(node_id, [])
            if cmd_index < len(node_result_list):
                results.append(node_result_list[cmd_index])
            else:
                results.append(None)

        # 清理状态
        self.reset()

        return results

    def reset(self) -> None:
        """重置 Pipeline 状态"""
        self._command_stack = []
        self._pipeline_pool.clear()
        self._node_cmd_counts.clear()

    def __getattr__(self, name: str) -> Callable:
        """
        代理 Pipeline 命令

        自动按节点分组命令。
        """

        def handle(*args, **kwargs):
            # 提取分片键
            shard_key = self._key_extractor.extract_shard_key(name, args, kwargs)

            # 获取目标节点
            node = self.node_proxy.get_node_by_shard_key(shard_key)

            # 获取节点的 Pipeline
            pipeline = self._get_pipeline(node.node_id)

            # 记录命令对应的节点和索引
            cmd_index = self._node_cmd_counts.get(node.node_id, 0)
            self._command_stack.append((node.node_id, cmd_index))
            self._node_cmd_counts[node.node_id] = cmd_index + 1

            # 执行命令
            command = getattr(pipeline, name)
            return command(*args, **kwargs)

        return handle

    def __enter__(self) -> "PipelineProxy":
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """上下文管理器退出"""
        if exc_type is None:
            self.execute()
        else:
            self.reset()
        return False

    def __len__(self) -> int:
        """获取待执行命令数量"""
        return len(self._command_stack)


class TransactionProxy:
    """
    事务代理

    提供 MULTI/EXEC 事务支持。

    Note:
        由于 Redis 事务只能在单个节点上执行，
        TransactionProxy 不支持跨节点事务。

    Example:
        >>> proxy = RedisProxy("cache", routing_manager=manager)
        >>> with proxy.transaction("shard_key") as tx:
        ...     tx.set("key1", "value1")
        ...     tx.set("key2", "value2")
        ...     results = tx.execute()
    """

    def __init__(self, node_proxy: "RedisProxy", shard_key: str | None = None):
        """
        初始化事务代理

        Args:
            node_proxy: Redis 代理实例
            shard_key: 分片键，用于确定事务执行的节点
        """
        self.node_proxy = node_proxy
        self.shard_key = shard_key
        self._node = None
        self._pipeline = None

    def __enter__(self) -> "TransactionProxy":
        """开始事务"""
        self._node = self.node_proxy.get_node_by_shard_key(self.shard_key)
        client = self.node_proxy.get_client(self._node)
        self._pipeline = client.pipeline(transaction=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """结束事务"""
        if exc_type is None and self._pipeline is not None:
            try:
                self._pipeline.execute()
            except Exception as e:
                logger.error(f"Transaction execute failed: {e}")
                raise
        return False

    def execute(self) -> list[Any]:
        """执行事务"""
        if self._pipeline is None:
            return []
        return self._pipeline.execute()

    def __getattr__(self, name: str) -> Callable:
        """代理事务命令"""
        if self._pipeline is None:
            raise RuntimeError("Transaction not started. Use 'with' statement.")

        def handle(*args, **kwargs):
            command = getattr(self._pipeline, name)
            return command(*args, **kwargs)

        return handle

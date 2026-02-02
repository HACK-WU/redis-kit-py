# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit 配置 Schema 模块

定义完整的配置结构
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from redis_kit.types import BackendConfig, NodeConfig, RoutingTable


@dataclass
class RedisKitConfig:
    """
    redis-kit 完整配置

    Attributes:
        nodes: 节点配置字典，键为节点 ID
        default_node: 默认节点 ID
        routing_strategy: 路由策略名称
        routing_table: 路由表（用于范围路由）
        backends: 后端配置字典
        key_prefix: 键前缀
        public_key_prefix: 公共键前缀
        retry_max_retries: 最大重试次数
        retry_backoff_base: 重试退避基础时间
        retry_backoff_max: 重试退避最大时间
        circuit_breaker_enabled: 是否启用断路器
        circuit_breaker_failure_threshold: 断路器失败阈值
        circuit_breaker_recovery_timeout: 断路器恢复超时

    Example:
        >>> config = RedisKitConfig(
        ...     nodes={
        ...         "default": NodeConfig(host="localhost", port=6379),
        ...         "backup": NodeConfig(host="localhost", port=6380),
        ...     },
        ...     default_node="default",
        ...     key_prefix="myapp",
        ... )
    """

    # 节点配置
    nodes: Dict[str, NodeConfig] = field(default_factory=dict)
    default_node: str = "default"

    # 路由配置
    routing_strategy: str = "hash"  # hash, range, modulo, consistent_hash
    routing_table: RoutingTable = field(default_factory=list)

    # 后端配置
    backends: Dict[str, BackendConfig] = field(default_factory=dict)

    # 键前缀
    key_prefix: str = ""
    public_key_prefix: str = ""

    # 重试配置
    retry_max_retries: int = 3
    retry_backoff_base: float = 0.1
    retry_backoff_max: float = 5.0
    retry_backoff_multiplier: float = 2.0

    # 断路器配置
    circuit_breaker_enabled: bool = False
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    # 连接池配置
    pool_max_connections: int = 10
    pool_min_idle_connections: int = 1

    def get_node(self, node_id: str = None) -> Optional[NodeConfig]:
        """
        获取节点配置

        Args:
            node_id: 节点 ID，None 时返回默认节点

        Returns:
            节点配置，不存在返回 None
        """
        node_id = node_id or self.default_node
        return self.nodes.get(node_id)

    def get_backend(self, backend_name: str) -> Optional[BackendConfig]:
        """
        获取后端配置

        Args:
            backend_name: 后端名称

        Returns:
            后端配置，不存在返回 None
        """
        return self.backends.get(backend_name)

    def get_db_for_backend(self, backend_name: str, default_db: int = 0) -> int:
        """
        获取后端对应的数据库编号

        Args:
            backend_name: 后端名称
            default_db: 默认数据库编号

        Returns:
            数据库编号
        """
        backend = self.backends.get(backend_name)
        return backend.db if backend else default_db

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RedisKitConfig":
        """
        从字典创建配置

        Args:
            data: 配置字典

        Returns:
            RedisKitConfig 实例
        """
        # 转换节点配置
        nodes = {}
        for node_id, node_data in data.get("nodes", {}).items():
            if isinstance(node_data, dict):
                nodes[node_id] = NodeConfig(**node_data)
            elif isinstance(node_data, NodeConfig):
                nodes[node_id] = node_data

        # 转换后端配置
        backends = {}
        for backend_name, backend_data in data.get("backends", {}).items():
            if isinstance(backend_data, dict):
                backends[backend_name] = BackendConfig(
                    name=backend_name, **backend_data
                )
            elif isinstance(backend_data, BackendConfig):
                backends[backend_name] = backend_data

        return cls(
            nodes=nodes,
            default_node=data.get("default_node", "default"),
            routing_strategy=data.get("routing_strategy", "hash"),
            routing_table=data.get("routing_table", []),
            backends=backends,
            key_prefix=data.get("key_prefix", ""),
            public_key_prefix=data.get("public_key_prefix", ""),
            retry_max_retries=data.get("retry_max_retries", 3),
            retry_backoff_base=data.get("retry_backoff_base", 0.1),
            retry_backoff_max=data.get("retry_backoff_max", 5.0),
            retry_backoff_multiplier=data.get("retry_backoff_multiplier", 2.0),
            circuit_breaker_enabled=data.get("circuit_breaker_enabled", False),
            circuit_breaker_failure_threshold=data.get(
                "circuit_breaker_failure_threshold", 5
            ),
            circuit_breaker_recovery_timeout=data.get(
                "circuit_breaker_recovery_timeout", 30.0
            ),
            pool_max_connections=data.get("pool_max_connections", 10),
            pool_min_idle_connections=data.get("pool_min_idle_connections", 1),
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        转换为字典

        Returns:
            配置字典
        """
        return {
            "nodes": {node_id: vars(node) for node_id, node in self.nodes.items()},
            "default_node": self.default_node,
            "routing_strategy": self.routing_strategy,
            "routing_table": self.routing_table,
            "backends": {
                name: vars(backend) for name, backend in self.backends.items()
            },
            "key_prefix": self.key_prefix,
            "public_key_prefix": self.public_key_prefix,
            "retry_max_retries": self.retry_max_retries,
            "retry_backoff_base": self.retry_backoff_base,
            "retry_backoff_max": self.retry_backoff_max,
            "retry_backoff_multiplier": self.retry_backoff_multiplier,
            "circuit_breaker_enabled": self.circuit_breaker_enabled,
            "circuit_breaker_failure_threshold": self.circuit_breaker_failure_threshold,
            "circuit_breaker_recovery_timeout": self.circuit_breaker_recovery_timeout,
            "pool_max_connections": self.pool_max_connections,
            "pool_min_idle_connections": self.pool_min_idle_connections,
        }

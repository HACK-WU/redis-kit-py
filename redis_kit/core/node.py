# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit 节点模块

定义 Redis 节点抽象类
"""

import logging
from typing import Any, Dict

from redis_kit.types import NodeConfig

logger = logging.getLogger(__name__)


class RedisNode:
    """
    Redis 节点

    封装 Redis 节点的连接信息和客户端实例管理。

    Attributes:
        config: 节点配置
        node_id: 节点唯一标识

    Example:
        >>> config = NodeConfig(host="localhost", port=6379)
        >>> node = RedisNode(config)
        >>> client = node.get_client("cache")
    """

    def __init__(self, config: NodeConfig, node_id: str = None):
        """
        初始化 Redis 节点

        Args:
            config: 节点配置
            node_id: 节点 ID，默认根据配置生成
        """
        self.config = config
        self._node_id = node_id or self._generate_node_id()
        self._instance_pool: Dict[str, Any] = {}
        self._available = True

    def _generate_node_id(self) -> str:
        """生成节点 ID"""
        return f"{self.config.cache_type}-{self.config.host}:{self.config.port}"

    @property
    def node_id(self) -> str:
        """节点唯一标识"""
        return self._node_id

    @property
    def is_available(self) -> bool:
        """节点是否可用"""
        return self._available

    @is_available.setter
    def is_available(self, value: bool) -> None:
        """设置节点可用状态"""
        self._available = value

    def get_connection_kwargs(
        self, backend: str = None, db: int = None
    ) -> Dict[str, Any]:
        """
        获取连接参数

        Args:
            backend: 后端名称（未使用，保留兼容）
            db: 数据库编号，覆盖配置中的值

        Returns:
            连接参数字典
        """
        kwargs = self.config.to_connection_kwargs()
        if db is not None:
            kwargs["db"] = db
        return kwargs

    def get_client(self, backend: str, db: int = None) -> Any:
        """
        获取 Redis 客户端实例

        使用实例池缓存客户端，避免重复创建连接。

        Args:
            backend: 后端名称，用于区分不同用途的连接
            db: 数据库编号

        Returns:
            Redis 客户端实例
        """
        cache_key = f"{backend}:{db}" if db is not None else backend

        if cache_key not in self._instance_pool:
            self._instance_pool[cache_key] = self._create_client(backend, db)

        return self._instance_pool[cache_key]

    def _create_client(self, backend: str, db: int = None) -> Any:
        """
        创建 Redis 客户端

        Args:
            backend: 后端名称
            db: 数据库编号

        Returns:
            Redis 客户端实例
        """
        import redis

        kwargs = self.get_connection_kwargs(backend, db)
        return redis.Redis(**kwargs)

    def refresh_client(self, backend: str, db: int = None) -> Any:
        """
        刷新客户端连接

        关闭现有连接并创建新连接。

        Args:
            backend: 后端名称
            db: 数据库编号

        Returns:
            新的 Redis 客户端实例
        """
        cache_key = f"{backend}:{db}" if db is not None else backend

        if cache_key in self._instance_pool:
            try:
                self._instance_pool[cache_key].close()
            except Exception as e:
                logger.warning(f"Failed to close client for {cache_key}: {e}")
            del self._instance_pool[cache_key]

        return self.get_client(backend, db)

    def close(self) -> None:
        """关闭所有客户端连接"""
        for key, client in list(self._instance_pool.items()):
            try:
                client.close()
            except Exception as e:
                logger.warning(f"Failed to close client for {key}: {e}")
        self._instance_pool.clear()

    def ping(self) -> bool:
        """
        检查节点连通性

        Returns:
            节点是否可达
        """
        try:
            client = self._create_client("ping", 0)
            result = client.ping()
            client.close()
            return result
        except Exception as e:
            logger.warning(f"Ping failed for node {self.node_id}: {e}")
            return False

    def __repr__(self) -> str:
        return f"RedisNode(node_id={self.node_id!r}, available={self.is_available})"


class SentinelRedisNode(RedisNode):
    """
    Sentinel Redis 节点

    支持 Redis Sentinel 高可用模式。

    Example:
        >>> config = NodeConfig(
        ...     host="sentinel-host",
        ...     port=26379,
        ...     cache_type="sentinel",
        ...     master_name="mymaster"
        ... )
        >>> node = SentinelRedisNode(config)
    """

    def _generate_node_id(self) -> str:
        """生成节点 ID，包含 master name"""
        master_name = self.config.master_name or "unknown"
        return f"sentinel-{self.config.host}:{self.config.port}-{master_name}"

    def _create_client(self, backend: str, db: int = None) -> Any:
        """
        创建 Sentinel 客户端

        Args:
            backend: 后端名称
            db: 数据库编号

        Returns:
            Sentinel master 客户端实例
        """
        from redis.sentinel import Sentinel

        kwargs = self.get_connection_kwargs(backend, db)
        master_name = kwargs.pop("master_name", None)
        sentinel_password = kwargs.pop("sentinel_password", None)

        if not master_name:
            raise ValueError("master_name is required for Sentinel mode")

        # 创建 Sentinel 实例
        sentinel_kwargs = {}
        if sentinel_password:
            sentinel_kwargs["password"] = sentinel_password

        sentinels = [(self.config.host, self.config.port)]
        sentinel = Sentinel(sentinels, sentinel_kwargs=sentinel_kwargs)

        # 获取 master 客户端
        return sentinel.master_for(master_name, **kwargs)

    def get_slave_client(self, backend: str, db: int = None) -> Any:
        """
        获取 Sentinel slave 客户端（只读）

        Args:
            backend: 后端名称
            db: 数据库编号

        Returns:
            Sentinel slave 客户端实例
        """
        from redis.sentinel import Sentinel

        kwargs = self.get_connection_kwargs(backend, db)
        master_name = kwargs.pop("master_name", None)
        sentinel_password = kwargs.pop("sentinel_password", None)

        if not master_name:
            raise ValueError("master_name is required for Sentinel mode")

        sentinel_kwargs = {}
        if sentinel_password:
            sentinel_kwargs["password"] = sentinel_password

        sentinels = [(self.config.host, self.config.port)]
        sentinel = Sentinel(sentinels, sentinel_kwargs=sentinel_kwargs)

        return sentinel.slave_for(master_name, **kwargs)


def create_node(config: NodeConfig, node_id: str = None) -> RedisNode:
    """
    节点工厂函数

    根据配置创建对应类型的节点实例。

    Args:
        config: 节点配置
        node_id: 节点 ID

    Returns:
        RedisNode 或 SentinelRedisNode 实例
    """
    if config.cache_type == "sentinel":
        return SentinelRedisNode(config, node_id)
    return RedisNode(config, node_id)

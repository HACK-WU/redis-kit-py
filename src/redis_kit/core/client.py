"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit Redis 客户端封装模块

提供 Redis 客户端的高级封装，支持单例模式、自动重试等功能
"""

import logging
import time
from typing import Any

from redis_kit.core.retry import RetryHandler
from redis_kit.types import NodeConfig, RetryPolicy

logger = logging.getLogger(__name__)


class RedisClient:
    """
    Redis 客户端封装

    提供单例模式、自动重试、连接刷新等功能。

    Attributes:
        config: 节点配置
        retry_policy: 重试策略

    Example:
        >>> config = NodeConfig(host="localhost", port=6379)
        >>> client = RedisClient(config)
        >>> client.set("key", "value")
        >>> client.get("key")
        'value'
    """

    # 单例实例缓存
    _instances: dict[str, "RedisClient"] = {}

    def __init__(
        self,
        config: NodeConfig,
        backend: str = "default",
        retry_policy: RetryPolicy = None,
    ):
        """
        初始化 Redis 客户端

        Args:
            config: 节点配置
            backend: 后端名称
            retry_policy: 重试策略
        """
        self.config = config
        self.backend = backend
        self.retry_policy = retry_policy or RetryPolicy()
        self.retry_handler = RetryHandler(self.retry_policy)

        self._instance = None
        self._readonly_instance = None
        self._refresh_time = 0

        self._create_instance()

    @classmethod
    def get_instance(
        cls,
        config: NodeConfig,
        backend: str = "default",
        retry_policy: RetryPolicy = None,
    ) -> "RedisClient":
        """
        获取单例实例

        Args:
            config: 节点配置
            backend: 后端名称
            retry_policy: 重试策略

        Returns:
            RedisClient 单例实例
        """
        # 使用 hashlib 生成稳定的哈希值，避免 Python hash() 的随机性问题
        import hashlib

        password_hash = hashlib.md5((config.password or "").encode("utf-8")).hexdigest()
        cache_key = f"{config.host}:{config.port}:{config.db}:{backend}:{password_hash}"

        if cache_key not in cls._instances:
            cls._instances[cache_key] = cls(config, backend, retry_policy)

        return cls._instances[cache_key]

    @classmethod
    def clear_instances(cls) -> None:
        """清除所有单例实例"""
        for instance in cls._instances.values():
            instance.close()
        cls._instances.clear()

    def _create_instance(self) -> None:
        """创建 Redis 实例"""
        import redis

        kwargs = self.config.to_connection_kwargs()

        for attempt in range(3):
            try:
                self._instance = redis.Redis(**kwargs)
                self._refresh_time = time.time()
                logger.debug(f"Created Redis instance for {self.backend}")
                break
            except Exception as e:
                logger.warning(
                    f"Failed to create Redis instance (attempt {attempt + 1}): {e}"
                )
                if attempt == 2:
                    raise

    def refresh_instance(self) -> None:
        """刷新 Redis 实例"""
        if self._instance is not None:
            try:
                self._instance.close()
            except Exception:
                pass

        if self._readonly_instance is not None:
            try:
                self._readonly_instance.close()
            except Exception:
                pass

        self._create_instance()

    @property
    def instance(self) -> Any:
        """获取 Redis 实例"""
        return self._instance

    @property
    def readonly_instance(self) -> Any | None:
        """获取只读实例（如果有）"""
        return self._readonly_instance

    def close(self) -> None:
        """关闭连接"""
        if self._instance is not None:
            try:
                self._instance.close()
            except Exception:
                pass
            self._instance = None

        if self._readonly_instance is not None:
            try:
                self._readonly_instance.close()
            except Exception:
                pass
            self._readonly_instance = None

    def pipeline(self, transaction: bool = True) -> Any:
        """
        获取 Pipeline

        Args:
            transaction: 是否使用事务模式

        Returns:
            Redis Pipeline 实例
        """
        return self._instance.pipeline(transaction=transaction)

    def __getattr__(self, name: str) -> Any:
        """
        代理 Redis 命令

        自动添加重试功能。
        """
        command = getattr(self._instance, name)

        def handle(*args, **kwargs):
            def execute():
                return command(*args, **kwargs)

            try:
                return self.retry_handler.execute(execute)
            except Exception as e:
                # 连接错误时刷新实例
                if "connection" in str(e).lower():
                    self.refresh_instance()
                raise

        return handle


class SentinelRedisClient(RedisClient):
    """
    Sentinel Redis 客户端封装

    支持 Redis Sentinel 高可用模式。

    Example:
        >>> config = NodeConfig(
        ...     host="sentinel-host",
        ...     port=26379,
        ...     cache_type="sentinel",
        ...     master_name="mymaster",
        ... )
        >>> client = SentinelRedisClient(config)
    """

    def _create_instance(self) -> None:
        """创建 Sentinel 实例"""
        from redis.sentinel import Sentinel

        kwargs = self.config.to_connection_kwargs()
        master_name = kwargs.pop("master_name", None)
        sentinel_password = kwargs.pop("sentinel_password", None)

        if not master_name:
            raise ValueError("master_name is required for Sentinel mode")

        sentinel_kwargs = {}
        if sentinel_password:
            sentinel_kwargs["password"] = sentinel_password

        sentinels = [(self.config.host, self.config.port)]

        for attempt in range(3):
            try:
                sentinel = Sentinel(sentinels, sentinel_kwargs=sentinel_kwargs)
                self._instance = sentinel.master_for(master_name, **kwargs)
                self._readonly_instance = sentinel.slave_for(master_name, **kwargs)
                self._refresh_time = time.time()
                logger.debug(f"Created Sentinel Redis instance for {self.backend}")
                break
            except Exception as e:
                logger.warning(
                    f"Failed to create Sentinel instance (attempt {attempt + 1}): {e}"
                )
                if attempt == 2:
                    raise


def create_client(
    config: NodeConfig,
    backend: str = "default",
    retry_policy: RetryPolicy = None,
    use_singleton: bool = True,
) -> RedisClient:
    """
    客户端工厂函数

    根据配置创建对应类型的客户端实例。

    Args:
        config: 节点配置
        backend: 后端名称
        retry_policy: 重试策略
        use_singleton: 是否使用单例模式

    Returns:
        RedisClient 或 SentinelRedisClient 实例
    """
    client_class: type[RedisClient]

    if config.cache_type == "sentinel":
        client_class = SentinelRedisClient
    else:
        client_class = RedisClient

    if use_singleton:
        return client_class.get_instance(config, backend, retry_policy)
    return client_class(config, backend, retry_policy)

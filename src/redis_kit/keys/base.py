"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit 键对象系统基础模块

提供 RedisDataKey 基类，用于统一管理 Redis 键的定义、格式化和操作
"""

import threading
from typing import TYPE_CHECKING

from redis_kit.exceptions import KeyFormatError, KeyTemplateError
from redis_kit.types import ShardedKey

if TYPE_CHECKING:
    from redis_kit.core.proxy import RedisProxy


class RedisDataKey:
    """
    Redis 键对象管理类

    封装键模板、过期策略及存储后端配置。提供统一的键生成、过期管理接口。

    Attributes:
        key_tpl: 键模板字符串，支持 Python format 语法
        ttl: 键值生存周期（秒）
        backend: Redis 连接池标识符
        is_global: 是否为全局键（影响键前缀）
        key_prefix: 键前缀
        label: 键的描述标签

    Example:
        >>> user_key = RedisDataKey(
        ...     key_tpl="user:{user_id}:profile",
        ...     ttl=3600,
        ...     backend="cache",
        ...     label="用户信息缓存",
        ... )
        >>> key = user_key.get_key(user_id=123, shard_key="123")
        >>> print(key)
        user:123:profile
    """

    def __init__(
        self,
        key_tpl: str = None,
        ttl: int = None,
        backend: str = None,
        is_global: bool = False,
        key_prefix: str = "",
        label: str = "",
        **extra_config,
    ):
        """
        初始化 Redis 键配置

        Args:
            key_tpl: 键模板字符串（必须）
            ttl: 键值过期时间，单位秒（必须）
            backend: Redis 连接池标识（必须）
            is_global: 是否为全局键，默认 False
            key_prefix: 键前缀，默认空字符串
            label: 键描述标签
            **extra_config: 扩展配置参数

        Raises:
            KeyTemplateError: 当缺少必填参数时抛出
        """
        if not key_tpl:
            raise KeyTemplateError("", "key_tpl is required")
        if ttl is None:
            raise KeyTemplateError(key_tpl, "ttl is required")
        if not backend:
            raise KeyTemplateError(key_tpl, "backend is required")

        self.key_tpl = key_tpl
        self.ttl = ttl
        self.backend = backend
        self.is_global = is_global
        self.key_prefix = key_prefix
        self.label = label
        self._proxy: RedisProxy | None = None
        self._proxy_lock = threading.Lock()  # 线程安全锁

        # 注入扩展属性
        for k, v in extra_config.items():
            setattr(self, k, v)

    @property
    def client(self) -> "RedisProxy":
        """
        获取 Redis 代理（延迟初始化，线程安全）

        使用双重检查锁定模式确保多线程环境下只创建一个实例。

        Returns:
            RedisProxy: 封装后的 Redis 连接代理实例
        """
        if self._proxy is None:
            with self._proxy_lock:
                if self._proxy is None:
                    from redis_kit.core.proxy import RedisProxy

                    self._proxy = RedisProxy(self.backend)
        return self._proxy

    def set_client(self, proxy: "RedisProxy") -> None:
        """
        设置 Redis 代理

        用于依赖注入，便于测试。线程安全。

        Args:
            proxy: RedisProxy 实例
        """
        with self._proxy_lock:
            self._proxy = proxy

    def get_key(self, **kwargs) -> ShardedKey:
        """
        生成格式化后的 Redis 键

        Args:
            **kwargs: 键模板格式化参数，可包含 shard_key

        Returns:
            ShardedKey: 带分片信息的键对象

        Raises:
            KeyFormatError: 当格式化失败时抛出

        Example:
            >>> key = data_key.get_key(strategy_id=100, item_id=1, shard_key="100")
            >>> str(key)
            'bk_monitor.cache.strategy.100.1'
        """
        # 提取分片键
        shard_key = kwargs.pop("shard_key", None)
        if shard_key is not None:
            shard_key = str(shard_key)

        # 格式化键模板
        try:
            key = self.key_tpl.format(**kwargs)
        except KeyError as e:
            raise KeyFormatError(self.key_tpl, kwargs, f"missing key: {e}")
        except Exception as e:
            raise KeyFormatError(self.key_tpl, kwargs, str(e))

        # 添加键前缀
        if self.key_prefix and not key.startswith(self.key_prefix):
            key = f"{self.key_prefix}.{key}"

        return ShardedKey(key=key, shard_key=shard_key)

    def expire(self, **key_kwargs) -> bool:
        """
        设置键过期时间

        Args:
            **key_kwargs: 用于生成键的格式化参数

        Returns:
            bool: 操作是否成功

        Note:
            在 pipeline 操作中应使用 pipeline.expire() 代替本方法，
            本方法会直接触发 Redis 网络请求
        """
        key = self.get_key(**key_kwargs)
        return self.client.expire(key, self.ttl)

    def delete(self, **key_kwargs) -> int:
        """
        删除键

        Args:
            **key_kwargs: 用于生成键的格式化参数

        Returns:
            int: 删除的键数量
        """
        key = self.get_key(**key_kwargs)
        return self.client.delete(key)

    def exists(self, **key_kwargs) -> bool:
        """
        检查键是否存在

        Args:
            **key_kwargs: 用于生成键的格式化参数

        Returns:
            bool: 键是否存在
        """
        key = self.get_key(**key_kwargs)
        return self.client.exists(key) > 0

    def ttl_remaining(self, **key_kwargs) -> int:
        """
        获取键的剩余生存时间

        Args:
            **key_kwargs: 用于生成键的格式化参数

        Returns:
            int: 剩余秒数，-1 表示永不过期，-2 表示键不存在
        """
        key = self.get_key(**key_kwargs)
        return self.client.ttl(key)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(key_tpl={self.key_tpl!r}, ttl={self.ttl}, backend={self.backend!r})"

"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit 分布式锁基础模块

定义锁的基类和接口
"""

from abc import ABC, abstractmethod


class BaseLock(ABC):
    """
    分布式锁基类

    所有锁实现必须继承此类。

    Attributes:
        name: 锁名称
        ttl: 锁过期时间（秒）

    Example:
        >>> class MyLock(BaseLock):
        ...     def acquire(self, blocking=True, timeout=None):
        ...         # 实现获取锁逻辑
        ...         pass
        ...
        ...     def release(self):
        ...         # 实现释放锁逻辑
        ...         pass
    """

    def __init__(self, name: str, ttl: int = 60):
        """
        初始化锁

        Args:
            name: 锁名称（键名）
            ttl: 锁过期时间，单位秒，默认 60 秒
        """
        self.name = name
        self.ttl = ttl

    @abstractmethod
    def acquire(self, blocking: bool = True, timeout: float = None) -> bool:
        """
        获取锁

        Args:
            blocking: 是否阻塞等待
            timeout: 等待超时时间（秒），None 表示无限等待

        Returns:
            是否成功获取锁
        """
        pass

    @abstractmethod
    def release(self) -> bool:
        """
        释放锁

        Returns:
            是否成功释放
        """
        pass

    @abstractmethod
    def is_locked(self) -> bool:
        """
        检查锁是否被持有

        Returns:
            锁是否被持有
        """
        pass

    def extend(self, additional_time: int) -> bool:
        """
        延长锁的过期时间

        Args:
            additional_time: 额外增加的时间（秒）

        Returns:
            是否成功延长
        """
        raise NotImplementedError("extend not supported")

    def __enter__(self) -> "BaseLock":
        """上下文管理器入口"""
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """上下文管理器退出"""
        self.release()
        return False

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, ttl={self.ttl})"

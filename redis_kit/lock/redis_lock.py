"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit Redis 分布式锁实现模块

提供基于 Redis 的分布式锁实现
"""

import logging
import time
import uuid
from typing import Any

from redis_kit.exceptions import LockExtendError
from redis_kit.lock.base import BaseLock

# Lua 脚本：原子性释放锁（验证 token 后删除）
RELEASE_LOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

# Lua 脚本：原子性延长锁过期时间（验证 token 后设置新 TTL）
EXTEND_LOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("expire", KEYS[1], ARGV[2])
else
    return 0
end
"""

# Lua 脚本：原子性获取读锁（检查写锁 + 增加计数器）
ACQUIRE_READ_LOCK_SCRIPT = """
if redis.call("exists", KEYS[1]) == 0 then
    local count = redis.call("incr", KEYS[2])
    if count == 1 then
        redis.call("expire", KEYS[2], ARGV[1])
    end
    return 1
else
    return 0
end
"""

# Lua 脚本：原子性获取写锁（检查读者计数 + 设置写锁）
ACQUIRE_WRITE_LOCK_SCRIPT = """
local readers = redis.call("get", KEYS[2])
if (readers == false or tonumber(readers) == 0) then
    if redis.call("set", KEYS[1], ARGV[1], "NX", "EX", ARGV[2]) then
        return 1
    end
end
return 0
"""

# Lua 脚本：原子性释放读锁（减少计数器，到0时删除）
RELEASE_READ_LOCK_SCRIPT = """
local count = redis.call("decr", KEYS[1])
if count <= 0 then
    redis.call("del", KEYS[1])
end
return count
"""

# Lua 脚本：原子性释放写锁（验证 token 后删除）
RELEASE_WRITE_LOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

logger = logging.getLogger(__name__)


class RedisLock(BaseLock):
    """
    Redis 分布式锁

    基于 Redis SET NX 实现的分布式锁，支持自动过期和 token 验证。

    Attributes:
        client: Redis 客户端
        name: 锁名称
        ttl: 锁过期时间（秒）

    Example:
        >>> lock = RedisLock(client, "my_lock", ttl=30)
        >>> if lock.acquire(timeout=5):
        ...     try:
        ...         # 执行临界区代码
        ...         pass
        ...     finally:
        ...         lock.release()

        # 使用上下文管理器
        >>> with RedisLock(client, "my_lock") as lock:
        ...     # 执行临界区代码
        ...     pass
    """

    def __init__(self, client: Any, name: str, ttl: int = 60):
        """
        初始化 Redis 锁

        Args:
            client: Redis 客户端实例
            name: 锁名称
            ttl: 锁过期时间，单位秒
        """
        super().__init__(name, ttl)
        self.client = client
        self._token: str | None = None

    def acquire(self, blocking: bool = True, timeout: float = None) -> bool:
        """
        获取锁

        使用 SET NX EX 原子操作获取锁。

        Args:
            blocking: 是否阻塞等待
            timeout: 等待超时时间（秒）

        Returns:
            是否成功获取锁
        """
        token = str(uuid.uuid4())
        deadline = time.time() + timeout if blocking and timeout else None

        while True:
            # 尝试获取锁
            if self.client.set(self.name, token, ex=self.ttl, nx=True):
                self._token = token
                logger.debug(f"Acquired lock: {self.name}")
                return True

            # 非阻塞模式直接返回
            if not blocking:
                return False

            # 检查超时
            if deadline and time.time() >= deadline:
                logger.debug(f"Lock acquisition timeout: {self.name}")
                return False

            # 短暂休眠后重试
            time.sleep(0.01)

    def release(self) -> bool:
        """
        释放锁

        使用 Lua 脚本原子性验证 token 并删除锁，避免竞态条件。

        Returns:
            是否成功释放
        """
        if not self._token:
            logger.warning(f"Cannot release lock without token: {self.name}")
            return False

        # 使用 Lua 脚本原子性释放锁
        result = self.client.eval(RELEASE_LOCK_SCRIPT, 1, self.name, self._token)
        if result:
            self._token = None
            logger.debug(f"Released lock: {self.name}")
            return True
        else:
            logger.warning(f"Lock token mismatch, cannot release: {self.name}")
            return False

    def is_locked(self) -> bool:
        """
        检查锁是否被持有

        Returns:
            锁是否被持有
        """
        return self.client.exists(self.name) > 0

    def is_owned(self) -> bool:
        """
        检查锁是否被当前实例持有

        Returns:
            锁是否被当前实例持有
        """
        if not self._token:
            return False
        current_token = self.client.get(self.name)
        return current_token == self._token

    def extend(self, additional_time: int) -> bool:
        """
        延长锁的过期时间

        使用 Lua 脚本原子性验证 token 并延长过期时间。
        注意：此方法直接在 Redis 中延长锁的过期时间，不在本地累加 TTL。

        Args:
            additional_time: 额外增加的时间（秒）

        Returns:
            是否成功延长

        Raises:
            LockExtendError: 延长失败时抛出
        """
        if not self._token:
            raise LockExtendError(self.name, "no token")

        # 获取当前剩余 TTL
        current_ttl = self.client.ttl(self.name)
        if current_ttl < 0:
            raise LockExtendError(self.name, "lock expired or not exists")

        # 计算新的过期时间
        new_ttl = current_ttl + additional_time

        # 使用 Lua 脚本原子性延长过期时间
        result = self.client.eval(
            EXTEND_LOCK_SCRIPT, 1, self.name, self._token, new_ttl
        )
        if result:
            return True

        raise LockExtendError(self.name, "token mismatch or expire failed")

    def refresh(self) -> bool:
        """
        刷新锁的过期时间（重置为初始 TTL）

        使用 Lua 脚本原子性验证 token 并重置过期时间。

        Returns:
            是否成功刷新
        """
        if not self._token:
            return False

        # 使用 Lua 脚本原子性刷新过期时间
        result = self.client.eval(
            EXTEND_LOCK_SCRIPT, 1, self.name, self._token, self.ttl
        )
        return bool(result)

    @property
    def token(self) -> str | None:
        """获取当前锁的 token"""
        return self._token


class MultiRedisLock:
    """
    Redis 批量锁

    支持同时获取多个锁，适用于需要锁定多个资源的场景。

    Attributes:
        client: Redis 客户端
        keys: 要锁定的键列表
        ttl: 锁过期时间（秒）

    Example:
        >>> lock = MultiRedisLock(client, ["key1", "key2", "key3"])
        >>> locked_keys = lock.acquire()
        >>> try:
        ...     for key in locked_keys:
        ...         # 处理已锁定的资源
        ...         pass
        ... finally:
        ...     lock.release()
    """

    def __init__(self, client: Any, keys: list[str], ttl: int = 60):
        """
        初始化批量锁

        Args:
            client: Redis 客户端实例
            keys: 要锁定的键列表
            ttl: 锁过期时间，单位秒
        """
        self.client = client
        self.keys = list(set(keys))  # 去重
        self.ttl = ttl
        self._token = str(uuid.uuid4())
        self._locked_keys: set[str] = set()

    def acquire(self) -> set[str]:
        """
        批量获取锁

        使用 Pipeline 批量执行 SET NX，返回成功获取的键集合。

        Returns:
            成功获取锁的键集合
        """
        if not self.keys:
            return set()

        pipeline = self.client.pipeline(transaction=False)
        for key in self.keys:
            pipeline.set(key, self._token, ex=self.ttl, nx=True)

        results = pipeline.execute()

        for key, success in zip(self.keys, results):
            if success:
                self._locked_keys.add(key)

        logger.debug(f"Acquired {len(self._locked_keys)}/{len(self.keys)} locks")
        return self._locked_keys.copy()

    def release(self) -> list[str]:
        """
        释放所有已获取的锁

        只释放 token 匹配的锁。

        Returns:
            成功释放的键列表
        """
        if not self._locked_keys:
            return []

        locked_keys = list(self._locked_keys)

        # 获取当前 token
        tokens = self.client.mget(locked_keys)

        # 只删除 token 匹配的键
        keys_to_delete = [
            key for key, token in zip(locked_keys, tokens) if token == self._token
        ]

        if keys_to_delete:
            self.client.delete(*keys_to_delete)

        self._locked_keys -= set(keys_to_delete)
        logger.debug(f"Released {len(keys_to_delete)} locks")
        return keys_to_delete

    def is_locked(self, key: str) -> bool:
        """
        检查指定键是否已获得锁

        Args:
            key: 键名

        Returns:
            该键是否已获得锁
        """
        return key in self._locked_keys

    def get_locked_keys(self) -> set[str]:
        """
        获取已锁定的键集合

        Returns:
            已锁定的键集合副本
        """
        return self._locked_keys.copy()

    def get_failed_keys(self) -> set[str]:
        """
        获取未能锁定的键集合

        Returns:
            未能锁定的键集合
        """
        return set(self.keys) - self._locked_keys

    def __enter__(self) -> "MultiRedisLock":
        """上下文管理器入口"""
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """上下文管理器退出"""
        self.release()
        return False

    def __len__(self) -> int:
        """获取已锁定的键数量"""
        return len(self._locked_keys)


class ReadWriteLock:
    """
    读写锁

    支持多读单写的分布式锁实现。

    - 读锁：多个读者可以同时持有
    - 写锁：独占，与所有其他锁互斥

    Example:
        >>> rw_lock = ReadWriteLock(client, "resource")

        # 获取读锁
        >>> with rw_lock.read_lock():
        ...     # 读取操作
        ...     pass

        # 获取写锁
        >>> with rw_lock.write_lock():
        ...     # 写入操作
        ...     pass
    """

    def __init__(self, client: Any, name: str, ttl: int = 60):
        """
        初始化读写锁

        警告：此读写锁实现为简化版本，适用于对一致性要求不高的场景。
        对于高一致性要求场景，建议使用 Redlock 算法或其他成熟实现。

        Args:
            client: Redis 客户端实例
            name: 锁基础名称
            ttl: 锁过期时间，单位秒
        """
        self.client = client
        self.name = name
        self.ttl = ttl
        self._read_lock_name = f"{name}:read"
        self._write_lock_name = f"{name}:write"
        self._reader_count_name = f"{name}:readers"
        self._write_token: str | None = None

    def read_lock(self) -> "_ReadLockContext":
        """
        获取读锁上下文

        Returns:
            读锁上下文管理器
        """
        return _ReadLockContext(self)

    def write_lock(self) -> "_WriteLockContext":
        """
        获取写锁上下文

        Returns:
            写锁上下文管理器
        """
        return _WriteLockContext(self)

    def acquire_read(self, blocking: bool = True, timeout: float = None) -> bool:
        """
        获取读锁

        使用 Lua 脚本原子性检查写锁并增加读者计数。

        Args:
            blocking: 是否阻塞等待
            timeout: 等待超时时间

        Returns:
            是否成功获取
        """
        deadline = time.time() + timeout if blocking and timeout else None

        while True:
            # 使用 Lua 脚本原子性获取读锁
            result = self.client.eval(
                ACQUIRE_READ_LOCK_SCRIPT,
                2,
                self._write_lock_name,
                self._reader_count_name,
                self.ttl,
            )
            if result:
                return True

            if not blocking:
                return False

            if deadline and time.time() >= deadline:
                return False

            time.sleep(0.01)

    def release_read(self) -> bool:
        """
        释放读锁

        使用 Lua 脚本原子性减少计数器并在计数为0时删除。

        Returns:
            是否成功释放
        """
        self.client.eval(RELEASE_READ_LOCK_SCRIPT, 1, self._reader_count_name)
        return True

    def acquire_write(self, blocking: bool = True, timeout: float = None) -> bool:
        """
        获取写锁

        使用 Lua 脚本原子性检查读者计数并获取写锁。

        Args:
            blocking: 是否阻塞等待
            timeout: 等待超时时间

        Returns:
            是否成功获取
        """
        token = str(uuid.uuid4())
        deadline = time.time() + timeout if blocking and timeout else None

        while True:
            # 使用 Lua 脚本原子性获取写锁（检查读者计数 + 设置写锁）
            result = self.client.eval(
                ACQUIRE_WRITE_LOCK_SCRIPT,
                2,
                self._write_lock_name,
                self._reader_count_name,
                token,
                self.ttl,
            )
            if result:
                self._write_token = token
                return True

            if not blocking:
                return False

            if deadline and time.time() >= deadline:
                return False

            time.sleep(0.01)

    def release_write(self) -> bool:
        """
        释放写锁

        使用 Lua 脚本原子性验证 token 并释放写锁。

        Returns:
            是否成功释放
        """
        if not self._write_token:
            logger.warning(
                f"Releasing write lock without token: {self._write_lock_name}"
            )
            return bool(self.client.delete(self._write_lock_name))

        result = self.client.eval(
            RELEASE_WRITE_LOCK_SCRIPT, 1, self._write_lock_name, self._write_token
        )
        if result:
            self._write_token = None
            return True
        return False


class _ReadLockContext:
    """读锁上下文管理器"""

    def __init__(self, rw_lock: ReadWriteLock):
        self._rw_lock = rw_lock

    def __enter__(self) -> "_ReadLockContext":
        self._rw_lock.acquire_read()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self._rw_lock.release_read()
        return False


class _WriteLockContext:
    """写锁上下文管理器"""

    def __init__(self, rw_lock: ReadWriteLock):
        self._rw_lock = rw_lock

    def __enter__(self) -> "_WriteLockContext":
        self._rw_lock.acquire_write()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self._rw_lock.release_write()
        return False

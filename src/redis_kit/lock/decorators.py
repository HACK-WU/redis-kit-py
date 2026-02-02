"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit 锁装饰器模块

提供服务锁装饰器，用于方法级别的锁保护
"""

import functools
import logging
from typing import Any, TypeVar
from collections.abc import Callable

from redis_kit.exceptions import LockAcquisitionError
from redis_kit.lock.redis_lock import RedisLock

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def service_lock(
    client: Any,
    lock_name: str | Callable[..., str],
    ttl: int = 60,
    blocking: bool = True,
    timeout: float = None,
    raise_on_fail: bool = True,
    return_on_fail: Any = None,
) -> Callable[[F], F]:
    """
    服务锁装饰器

    为函数添加分布式锁保护。

    Args:
        client: Redis 客户端
        lock_name: 锁名称，可以是字符串或返回字符串的可调用对象
        ttl: 锁过期时间（秒）
        blocking: 是否阻塞等待
        timeout: 等待超时时间（秒）
        raise_on_fail: 获取失败时是否抛出异常
        return_on_fail: 获取失败时的返回值（当 raise_on_fail=False 时有效）

    Returns:
        装饰器函数

    Example:
        >>> @service_lock(client, "my_service_lock", ttl=30)
        ... def my_service():
        ...     # 临界区代码
        ...     pass

        # 动态锁名
        >>> @service_lock(client, lambda args, kwargs: f"lock:{kwargs['user_id']}")
        ... def process_user(user_id):
        ...     pass
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # 确定锁名
            if callable(lock_name):
                actual_lock_name = lock_name(args, kwargs)
            else:
                actual_lock_name = lock_name

            lock = RedisLock(client, actual_lock_name, ttl)

            if not lock.acquire(blocking=blocking, timeout=timeout):
                if raise_on_fail:
                    raise LockAcquisitionError(actual_lock_name, timeout)
                logger.warning(f"Failed to acquire lock: {actual_lock_name}")
                return return_on_fail

            try:
                return func(*args, **kwargs)
            finally:
                lock.release()

        return wrapper  # type: ignore

    return decorator


def singleton_task(
    client: Any,
    task_name: str,
    ttl: int = 300,
    extend_on_success: bool = False,
) -> Callable[[F], F]:
    """
    单例任务装饰器

    确保同一任务在集群中只有一个实例运行。

    Args:
        client: Redis 客户端
        task_name: 任务名称
        ttl: 锁过期时间（秒），应大于任务预期执行时间
        extend_on_success: 成功执行后是否延长锁时间

    Returns:
        装饰器函数

    Example:
        >>> @singleton_task(client, "daily_cleanup", ttl=3600)
        ... def daily_cleanup():
        ...     # 每天只执行一次的清理任务
        ...     pass
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            lock_name = f"singleton:{task_name}"
            lock = RedisLock(client, lock_name, ttl)

            if not lock.acquire(blocking=False):
                logger.info(f"Singleton task already running: {task_name}")
                return None

            try:
                result = func(*args, **kwargs)
                if extend_on_success:
                    lock.extend(ttl)
                return result
            finally:
                lock.release()

        return wrapper  # type: ignore

    return decorator


def rate_limited(
    client: Any,
    key_prefix: str,
    max_calls: int,
    period: int,
    key_func: Callable[..., str] = None,
) -> Callable[[F], F]:
    """
    速率限制装饰器

    限制函数的调用频率。

    Args:
        client: Redis 客户端
        key_prefix: 键前缀
        max_calls: 时间窗口内最大调用次数
        period: 时间窗口（秒）
        key_func: 用于生成限流键的函数，接收 (args, kwargs) 返回字符串

    Returns:
        装饰器函数

    Example:
        >>> @rate_limited(client, "api_call", max_calls=100, period=60)
        ... def api_call():
        ...     pass

        # 按用户限流
        >>> @rate_limited(
        ...     client,
        ...     "user_api",
        ...     max_calls=10,
        ...     period=60,
        ...     key_func=lambda args, kwargs: kwargs.get("user_id", "default"),
        ... )
        ... def user_api(user_id):
        ...     pass
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # 生成限流键
            if key_func:
                key_suffix = key_func(args, kwargs)
            else:
                key_suffix = "global"

            rate_key = f"rate_limit:{key_prefix}:{key_suffix}"

            # 使用 INCR + EXPIRE 实现计数
            current = client.incr(rate_key)
            if current == 1:
                client.expire(rate_key, period)

            if current > max_calls:
                ttl = client.ttl(rate_key)
                raise RateLimitExceededError(key_prefix, max_calls, period, ttl)

            return func(*args, **kwargs)

        return wrapper  # type: ignore

    return decorator


class RateLimitExceededError(Exception):
    """速率限制超出错误"""

    def __init__(self, key: str, max_calls: int, period: int, retry_after: int = None):
        self.key = key
        self.max_calls = max_calls
        self.period = period
        self.retry_after = retry_after
        msg = f"Rate limit exceeded for '{key}': {max_calls} calls per {period}s"
        if retry_after:
            msg += f", retry after {retry_after}s"
        super().__init__(msg)

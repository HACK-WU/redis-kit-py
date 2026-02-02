# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit 重试处理器模块

提供重试策略和重试装饰器
"""

import random
import time
from functools import wraps
from typing import Callable, Tuple, Type, TypeVar

from redis_kit.exceptions import RetryExhaustedError
from redis_kit.types import RetryPolicy

T = TypeVar("T")


class RetryHandler:
    """
    重试处理器

    支持可配置的重试策略，包括最大重试次数、退避时间等。

    Attributes:
        policy: 重试策略配置

    Example:
        >>> handler = RetryHandler(RetryPolicy(max_retries=3))
        >>> result = handler.execute(some_function, arg1, arg2)
    """

    def __init__(self, policy: RetryPolicy = None):
        """
        初始化重试处理器

        Args:
            policy: 重试策略配置，None 时使用默认配置
        """
        self.policy = policy or RetryPolicy()

    def execute(self, func: Callable[..., T], *args, **kwargs) -> T:
        """
        执行带重试的操作

        Args:
            func: 要执行的函数
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            函数执行结果

        Raises:
            RetryExhaustedError: 重试次数耗尽后仍失败
        """
        last_exception = None

        for attempt in range(self.policy.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except self.policy.retryable_exceptions as e:
                last_exception = e
                if attempt < self.policy.max_retries:
                    delay = self._calculate_delay(attempt)
                    time.sleep(delay)

        raise RetryExhaustedError(
            operation=func.__name__ if hasattr(func, "__name__") else str(func),
            attempts=self.policy.max_retries + 1,
            last_error=last_exception,
        )

    def _calculate_delay(self, attempt: int) -> float:
        """
        计算退避延迟时间

        使用指数退避算法，并添加随机抖动以避免惊群效应。

        Args:
            attempt: 当前重试次数（从 0 开始）

        Returns:
            延迟时间（秒）
        """
        # 指数退避
        delay = self.policy.backoff_base * (self.policy.backoff_multiplier**attempt)
        # 限制最大延迟
        delay = min(delay, self.policy.backoff_max)
        # 添加随机抖动（±50%）
        jitter = delay * (0.5 + random.random())
        return jitter


def with_retry(
    max_retries: int = 3,
    backoff_base: float = 0.1,
    backoff_max: float = 5.0,
    backoff_multiplier: float = 2.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    重试装饰器

    为函数添加自动重试功能。

    Args:
        max_retries: 最大重试次数
        backoff_base: 退避基础时间（秒）
        backoff_max: 最大退避时间（秒）
        backoff_multiplier: 退避时间乘数
        retryable_exceptions: 可重试的异常类型

    Returns:
        装饰器函数

    Example:
        >>> @with_retry(max_retries=3)
        ... def fetch_data():
        ...     # 可能失败的操作
        ...     pass
    """
    if retryable_exceptions is None:
        retryable_exceptions = (ConnectionError, TimeoutError)

    policy = RetryPolicy(
        max_retries=max_retries,
        backoff_base=backoff_base,
        backoff_max=backoff_max,
        backoff_multiplier=backoff_multiplier,
        retryable_exceptions=retryable_exceptions,
    )
    handler = RetryHandler(policy)

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            return handler.execute(func, *args, **kwargs)

        return wrapper

    return decorator


class RetryContext:
    """
    重试上下文管理器

    提供更精细的重试控制。

    Example:
        >>> with RetryContext(max_retries=3) as ctx:
        ...     while ctx.should_retry():
        ...         try:
        ...             result = risky_operation()
        ...             break
        ...         except Exception as e:
        ...             ctx.record_failure(e)
    """

    def __init__(self, policy: RetryPolicy = None):
        """
        初始化重试上下文

        Args:
            policy: 重试策略配置
        """
        self.policy = policy or RetryPolicy()
        self._attempt = 0
        self._last_exception = None

    def __enter__(self) -> "RetryContext":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        return False

    def should_retry(self) -> bool:
        """
        检查是否应该继续重试

        Returns:
            True 表示可以继续重试
        """
        return self._attempt <= self.policy.max_retries

    def record_failure(self, exception: Exception) -> None:
        """
        记录失败

        Args:
            exception: 捕获的异常

        Raises:
            RetryExhaustedError: 重试次数耗尽时抛出
        """
        self._last_exception = exception
        self._attempt += 1

        if self._attempt > self.policy.max_retries:
            raise RetryExhaustedError(
                operation="retry_context",
                attempts=self._attempt,
                last_error=self._last_exception,
            )

        # 计算延迟并等待
        delay = self._calculate_delay()
        time.sleep(delay)

    def _calculate_delay(self) -> float:
        """计算退避延迟"""
        delay = self.policy.backoff_base * (
            self.policy.backoff_multiplier ** (self._attempt - 1)
        )
        delay = min(delay, self.policy.backoff_max)
        return delay * (0.5 + random.random())

    @property
    def attempt(self) -> int:
        """当前重试次数"""
        return self._attempt

    @property
    def last_exception(self) -> Exception:
        """最后一次异常"""
        return self._last_exception

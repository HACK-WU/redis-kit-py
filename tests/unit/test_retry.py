"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""

import pytest
import time
from unittest.mock import Mock, patch
from redis_kit.core.retry import RetryHandler, RetryContext, with_retry
from redis_kit.types import RetryPolicy
from redis_kit.exceptions import RetryExhaustedError


class TestRetryHandler:
    """测试 RetryHandler 类"""

    def test_successful_execution(self):
        """测试成功执行（无需重试）"""
        policy = RetryPolicy(max_retries=3, base_delay=0.01)
        handler = RetryHandler(policy)

        func = Mock(return_value="success")
        result = handler.execute(func, 1, 2, key="value")

        assert result == "success"
        func.assert_called_once_with(1, 2, key="value")

    def test_retry_on_exception(self):
        """测试异常时重试"""
        policy = RetryPolicy(
            max_retries=3, base_delay=0.01, retryable_exceptions=(ValueError,)
        )
        handler = RetryHandler(policy)

        func = Mock(side_effect=[ValueError(), ValueError(), "success"])
        result = handler.execute(func)

        assert result == "success"
        assert func.call_count == 3

    def test_retry_exhausted(self):
        """测试重试次数耗尽"""
        policy = RetryPolicy(
            max_retries=2, base_delay=0.01, retryable_exceptions=(ValueError,)
        )
        handler = RetryHandler(policy)

        func = Mock(side_effect=ValueError("persistent error"))

        with pytest.raises(RetryExhaustedError) as exc_info:
            handler.execute(func)

        assert func.call_count == 3  # 初始尝试 + 2次重试
        assert exc_info.value.attempts == 3

    def test_non_retryable_exception(self):
        """测试不可重试的异常"""
        policy = RetryPolicy(
            max_retries=3, base_delay=0.01, retryable_exceptions=(ValueError,)
        )
        handler = RetryHandler(policy)

        func = Mock(side_effect=TypeError("not retryable"))

        with pytest.raises(TypeError):
            handler.execute(func)

        func.assert_called_once()  # 不应重试

    def test_exponential_backoff(self):
        """测试指数退避"""
        policy = RetryPolicy(
            max_retries=3,
            base_delay=0.1,
            exponential_base=2,
            retryable_exceptions=(ValueError,),
        )
        handler = RetryHandler(policy)

        func = Mock(side_effect=[ValueError(), ValueError(), "success"])

        start_time = time.time()
        handler.execute(func)
        elapsed = time.time() - start_time

        # 预期延迟：0.1 * (0.5~1.5) + 0.2 * (0.5~1.5) ≈ 0.15 + 0.3 = 0.45秒
        assert elapsed >= 0.15, f"Expected delay >= 0.15s, got {elapsed}s"

    def test_max_delay_cap(self):
        """测试最大延迟上限"""
        policy = RetryPolicy(
            max_retries=5,
            base_delay=1.0,
            max_delay=2.0,
            exponential_base=10,
            retryable_exceptions=(ValueError,),
        )
        handler = RetryHandler(policy)

        delays = []
        original_sleep = time.sleep

        def mock_sleep(duration):
            delays.append(duration)
            original_sleep(0.001)  # 实际只睡很短时间以加速测试

        with patch("time.sleep", side_effect=mock_sleep):
            func = Mock(side_effect=[ValueError()] * 5 + ["success"])
            handler.execute(func)

        # 所有延迟都应该 <= max_delay * 1.5 (考虑抖动)
        assert all(d <= 2.0 * 1.5 for d in delays)


class TestRetryContext:
    """测试 RetryContext 上下文管理器"""

    def test_context_manager_success(self):
        """测试上下文管理器成功执行"""
        policy = RetryPolicy(max_retries=3, base_delay=0.01)

        call_count = 0
        with RetryContext(policy) as retry:
            for attempt in retry:
                call_count += 1
                break  # 第一次就成功

        assert call_count == 1

    def test_context_manager_retry(self):
        """测试上下文管理器重试"""
        policy = RetryPolicy(
            max_retries=3, base_delay=0.01, retryable_exceptions=(ValueError,)
        )

        attempts = []
        with RetryContext(policy) as retry:
            for attempt in retry:
                attempts.append(attempt)
                if attempt < 2:
                    raise ValueError("retry me")

        assert len(attempts) == 3

    def test_context_manager_exhausted(self):
        """测试上下文管理器重试耗尽"""
        policy = RetryPolicy(
            max_retries=2, base_delay=0.01, retryable_exceptions=(ValueError,)
        )

        with pytest.raises(RetryExhaustedError):
            with RetryContext(policy) as retry:
                for attempt in retry:
                    raise ValueError("always fail")


class TestWithRetryDecorator:
    """测试 with_retry 装饰器"""

    def test_decorator_basic(self):
        """测试装饰器基本功能"""

        @with_retry(max_retries=3, base_delay=0.01)
        def flaky_function():
            if not hasattr(flaky_function, "call_count"):
                flaky_function.call_count = 0
            flaky_function.call_count += 1

            if flaky_function.call_count < 3:
                raise ConnectionError("retry me")
            return "success"

        result = flaky_function()
        assert result == "success"
        assert flaky_function.call_count == 3

    def test_decorator_with_args(self):
        """测试装饰器带参数"""

        @with_retry(max_retries=2, base_delay=0.01)
        def add(a, b):
            return a + b

        result = add(1, 2)
        assert result == 3

    def test_decorator_custom_exceptions(self):
        """测试装饰器自定义可重试异常"""

        @with_retry(max_retries=3, base_delay=0.01, retryable_exceptions=(ValueError,))
        def raises_type_error():
            raise TypeError("not retryable")

        with pytest.raises(TypeError):
            raises_type_error()

"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""

from redis_kit.exceptions import (
    RedisKitError,
    ConfigurationError,
    InvalidConfigError,
    MissingConfigError,
    RedisConnectionError,
    ConnectionTimeoutError,
    ConnectionRefusedError,
    AuthenticationError,
    RoutingError,
    NoAvailableNodeError,
    InvalidShardKeyError,
    RoutingTableError,
    LockError,
    LockAcquisitionError,
    LockReleaseError,
    LockExtendError,
    CircuitBreakerError,
    CircuitOpenError,
    QueueError,
    TaskNotFoundError,
    TaskAlreadyExistsError,
    RedisKeyError,
    KeyTemplateError,
    KeyFormatError,
    RetryExhaustedError,
)


class TestExceptionHierarchy:
    """测试异常继承层次"""

    def test_base_exception(self):
        """测试基础异常"""
        exc = RedisKitError("test error")
        assert str(exc) == "test error"
        assert isinstance(exc, Exception)

    def test_configuration_errors(self):
        """测试配置相关异常"""
        exc1 = ConfigurationError("config error")
        exc2 = InvalidConfigError("key", "invalid_value", "invalid")
        exc3 = MissingConfigError("missing_key")

        assert isinstance(exc1, RedisKitError)
        assert isinstance(exc2, ConfigurationError)
        assert isinstance(exc3, ConfigurationError)

    def test_connection_errors(self):
        """测试连接相关异常"""
        exc1 = RedisConnectionError("connection failed")
        exc2 = ConnectionTimeoutError("localhost", 6379)
        exc3 = ConnectionRefusedError("localhost", 6379)
        exc4 = AuthenticationError("localhost", 6379)

        assert isinstance(exc1, RedisKitError)
        assert isinstance(exc2, RedisConnectionError)
        assert isinstance(exc3, RedisConnectionError)
        assert isinstance(exc4, RedisConnectionError)

    def test_routing_errors(self):
        """测试路由相关异常"""
        exc1 = RoutingError("routing failed")
        exc2 = NoAvailableNodeError("no nodes")
        exc3 = InvalidShardKeyError("invalid shard key")
        exc4 = RoutingTableError("bad routing table")

        assert isinstance(exc1, RedisKitError)
        assert isinstance(exc2, RoutingError)
        assert isinstance(exc3, RoutingError)
        assert isinstance(exc4, RoutingError)

    def test_lock_errors(self):
        """测试锁相关异常"""
        exc1 = LockError("lock failed")
        exc2 = LockAcquisitionError("acquire failed")
        exc3 = LockReleaseError("release failed")
        exc4 = LockExtendError("extend failed")

        assert isinstance(exc1, RedisKitError)
        assert isinstance(exc2, LockError)
        assert isinstance(exc3, LockError)
        assert isinstance(exc4, LockError)

    def test_circuit_breaker_errors(self):
        """测试熔断器相关异常"""
        exc1 = CircuitBreakerError("circuit breaker error")
        exc2 = CircuitOpenError("circuit is open")

        assert isinstance(exc1, RedisKitError)
        assert isinstance(exc2, CircuitBreakerError)

    def test_queue_errors(self):
        """测试队列相关异常"""
        exc1 = QueueError("queue error")
        exc2 = TaskNotFoundError("task_123")
        exc3 = TaskAlreadyExistsError("task_456")

        assert isinstance(exc1, RedisKitError)
        assert isinstance(exc2, QueueError)
        assert isinstance(exc3, QueueError)

    def test_key_errors(self):
        """测试键相关异常"""
        exc1 = RedisKeyError("key error")
        exc2 = KeyTemplateError("template", "invalid template")
        exc3 = KeyFormatError("template", {"key": "value"}, "format error")

        assert isinstance(exc1, RedisKitError)
        assert isinstance(exc2, RedisKeyError)
        assert isinstance(exc3, RedisKeyError)

    def test_retry_errors(self):
        """测试重试相关异常"""
        exc = RetryExhaustedError("retry exhausted", attempts=3)
        assert isinstance(exc, RedisKitError)
        assert "retry exhausted" in str(exc)


class TestExceptionAttributes:
    """测试异常属性"""

    def test_task_not_found_error(self):
        """测试 TaskNotFoundError 属性"""
        exc = TaskNotFoundError("task_123")
        assert exc.task_id == "task_123"
        assert "task_123" in str(exc)

    def test_task_already_exists_error(self):
        """测试 TaskAlreadyExistsError 属性"""
        exc = TaskAlreadyExistsError("task_456")
        assert exc.task_id == "task_456"
        assert "task_456" in str(exc)

    def test_retry_exhausted_error(self):
        """测试 RetryExhaustedError 属性"""
        exc = RetryExhaustedError("operation failed", attempts=5)
        assert exc.attempts == 5
        assert "5" in str(exc)

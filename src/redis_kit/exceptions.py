"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit 异常定义模块

定义完整的异常层次结构，便于精确的错误处理
"""


class RedisKitError(Exception):
    """
    redis-kit 基础异常

    所有 redis-kit 相关异常的基类
    """

    pass


# =============================================================================
# 配置相关异常
# =============================================================================


class ConfigurationError(RedisKitError):
    """
    配置相关错误

    当配置无效、缺失或格式错误时抛出
    """

    pass


class InvalidConfigError(ConfigurationError):
    """配置值无效"""

    def __init__(self, key: str, value: any, reason: str = None):
        self.key = key
        self.value = value
        self.reason = reason
        msg = f"Invalid configuration for '{key}': {value}"
        if reason:
            msg += f", reason: {reason}"
        super().__init__(msg)


class MissingConfigError(ConfigurationError):
    """必需的配置项缺失"""

    def __init__(self, key: str):
        self.key = key
        super().__init__(f"Missing required configuration: {key}")


# =============================================================================
# 连接相关异常
# =============================================================================


class RedisConnectionError(RedisKitError):
    """
    连接相关错误

    当无法建立或维持 Redis 连接时抛出
    """

    pass


class ConnectionTimeoutError(RedisConnectionError):
    """连接超时"""

    def __init__(self, host: str, port: int, timeout: float = None):
        self.host = host
        self.port = port
        self.timeout = timeout
        msg = f"Connection timeout to {host}:{port}"
        if timeout:
            msg += f" after {timeout}s"
        super().__init__(msg)


class ConnectionRefusedError(RedisConnectionError):
    """连接被拒绝"""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        super().__init__(f"Connection refused to {host}:{port}")


class AuthenticationError(RedisConnectionError):
    """认证失败"""

    def __init__(self, host: str, port: int, reason: str = None):
        self.host = host
        self.port = port
        self.reason = reason
        msg = f"Authentication failed for {host}:{port}"
        if reason:
            msg += f": {reason}"
        super().__init__(msg)


# =============================================================================
# 路由相关异常
# =============================================================================


class RoutingError(RedisKitError):
    """
    路由相关错误

    当路由决策失败时抛出
    """

    pass


class NoAvailableNodeError(RoutingError):
    """
    无可用节点错误

    当没有可用的 Redis 节点来处理请求时抛出
    """

    def __init__(self, shard_key: str = None, reason: str = None):
        self.shard_key = shard_key
        self.reason = reason
        if shard_key:
            msg = f"No available node for shard_key: {shard_key}"
        else:
            msg = "No available node"
        if reason:
            msg += f", reason: {reason}"
        super().__init__(msg)


class InvalidShardKeyError(RoutingError):
    """分片键无效"""

    def __init__(self, shard_key: any, reason: str = None):
        self.shard_key = shard_key
        self.reason = reason
        msg = f"Invalid shard key: {shard_key}"
        if reason:
            msg += f", reason: {reason}"
        super().__init__(msg)


class RoutingTableError(RoutingError):
    """路由表错误"""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"Routing table error: {reason}")


# =============================================================================
# 锁相关异常
# =============================================================================


class LockError(RedisKitError):
    """
    锁相关错误

    分布式锁操作失败时抛出
    """

    pass


class LockAcquisitionError(LockError):
    """
    获取锁失败

    当无法在指定时间内获取锁时抛出
    """

    def __init__(self, lock_name: str, timeout: float = None, reason: str = None):
        self.lock_name = lock_name
        self.timeout = timeout
        self.reason = reason
        msg = f"Failed to acquire lock: {lock_name}"
        if timeout:
            msg += f" within {timeout}s"
        if reason:
            msg += f", reason: {reason}"
        super().__init__(msg)


class LockReleaseError(LockError):
    """
    释放锁失败

    当无法释放锁时抛出（如锁已过期或被其他进程持有）
    """

    def __init__(self, lock_name: str, reason: str = None):
        self.lock_name = lock_name
        self.reason = reason
        msg = f"Failed to release lock: {lock_name}"
        if reason:
            msg += f", reason: {reason}"
        super().__init__(msg)


class LockExtendError(LockError):
    """延长锁时间失败"""

    def __init__(self, lock_name: str, reason: str = None):
        self.lock_name = lock_name
        self.reason = reason
        msg = f"Failed to extend lock: {lock_name}"
        if reason:
            msg += f", reason: {reason}"
        super().__init__(msg)


# =============================================================================
# 断路器相关异常
# =============================================================================


class CircuitBreakerError(RedisKitError):
    """断路器相关错误"""

    pass


class CircuitOpenError(CircuitBreakerError):
    """
    断路器打开错误

    当断路器处于打开状态，拒绝请求时抛出
    """

    def __init__(self, node_id: str, recovery_time: float = None):
        self.node_id = node_id
        self.recovery_time = recovery_time
        msg = f"Circuit breaker is open for node: {node_id}"
        if recovery_time:
            msg += f", will try recovery in {recovery_time:.1f}s"
        super().__init__(msg)


# =============================================================================
# 队列相关异常
# =============================================================================


class QueueError(RedisKitError):
    """队列相关错误"""

    pass


class TaskNotFoundError(QueueError):
    """任务未找到"""

    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"Task not found: {task_id}")


class TaskAlreadyExistsError(QueueError):
    """任务已存在"""

    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"Task already exists: {task_id}")


# =============================================================================
# 键对象相关异常
# =============================================================================


class RedisKeyError(RedisKitError):
    """键对象相关错误"""

    pass


class KeyTemplateError(RedisKeyError):
    """键模板错误"""

    def __init__(self, template: str, reason: str):
        self.template = template
        self.reason = reason
        super().__init__(f"Invalid key template '{template}': {reason}")


class KeyFormatError(RedisKeyError):
    """键格式化错误"""

    def __init__(self, template: str, kwargs: dict, reason: str):
        self.template = template
        self.kwargs = kwargs
        self.reason = reason
        super().__init__(f"Failed to format key '{template}' with {kwargs}: {reason}")


# =============================================================================
# 重试相关异常
# =============================================================================


class RetryExhaustedError(RedisKitError):
    """
    重试次数耗尽

    当重试次数用完仍然失败时抛出
    """

    def __init__(self, operation: str, attempts: int, last_error: Exception = None):
        self.operation = operation
        self.attempts = attempts
        self.last_error = last_error
        msg = f"Retry exhausted for '{operation}' after {attempts} attempts"
        if last_error:
            msg += f", last error: {last_error}"
        super().__init__(msg)

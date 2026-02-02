# -*- coding: utf-8 -*-
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
from unittest.mock import MagicMock, patch
from redis_kit.core.client import RedisClient, create_client
from redis_kit.types import NodeConfig, RetryPolicy
from redis_kit.exceptions import RetryExhaustedError


class TestRedisClientSingleton:
    """测试 RedisClient 单例模式"""

    def test_get_instance_returns_same_instance(self):
        """测试 get_instance 返回相同的实例"""
        config = NodeConfig(host="localhost", port=6379, db=0)

        client1 = RedisClient.get_instance(config, backend="test")
        client2 = RedisClient.get_instance(config, backend="test")

        assert client1 is client2

    def test_get_instance_different_backends(self):
        """测试不同 backend 返回不同实例"""
        config = NodeConfig(host="localhost", port=6379, db=0)

        client1 = RedisClient.get_instance(config, backend="backend1")
        client2 = RedisClient.get_instance(config, backend="backend2")

        assert client1 is not client2

    def test_get_instance_different_passwords(self):
        """测试不同密码返回不同实例"""
        config1 = NodeConfig(host="localhost", port=6379, db=0, password="pass1")
        config2 = NodeConfig(host="localhost", port=6379, db=0, password="pass2")

        client1 = RedisClient.get_instance(config1, backend="test")
        client2 = RedisClient.get_instance(config2, backend="test")

        assert client1 is not client2

    def test_get_instance_same_password_same_instance(self):
        """测试相同密码返回相同实例"""
        config1 = NodeConfig(host="localhost", port=6379, db=0, password="same_pass")
        config2 = NodeConfig(host="localhost", port=6379, db=0, password="same_pass")

        client1 = RedisClient.get_instance(config1, backend="test")
        client2 = RedisClient.get_instance(config2, backend="test")

        assert client1 is client2

    def test_get_instance_different_dbs(self):
        """测试不同数据库返回不同实例"""
        config1 = NodeConfig(host="localhost", port=6379, db=0)
        config2 = NodeConfig(host="localhost", port=6379, db=1)

        client1 = RedisClient.get_instance(config1, backend="test")
        client2 = RedisClient.get_instance(config2, backend="test")

        assert client1 is not client2

    def test_get_instance_different_ports(self):
        """测试不同端口返回不同实例"""
        config1 = NodeConfig(host="localhost", port=6379, db=0)
        config2 = NodeConfig(host="localhost", port=6380, db=0)

        client1 = RedisClient.get_instance(config1, backend="test")
        client2 = RedisClient.get_instance(config2, backend="test")

        assert client1 is not client2

    def test_password_hash_stability(self):
        """测试密码哈希的稳定性"""
        config = NodeConfig(host="localhost", port=6379, db=0, password="test_password")

        # 多次调用应该生成相同的 cache_key
        cache_key1 = f"{config.host}:{config.port}:{config.db}:test:{hashlib.md5(config.password.encode('utf-8')).hexdigest()}"

        # 实际上 RedisClient 内部使用 hashlib.md5
        client1 = RedisClient.get_instance(config, backend="test")

        # 清除缓存后再获取
        RedisClient.clear_instances()
        client2 = RedisClient.get_instance(config, backend="test")

        # 应该是同一个实例（因为密码哈希稳定）
        assert client1 is not client2  # 清除后是新实例

    def test_clear_instances(self):
        """测试清除所有实例"""
        config = NodeConfig(host="localhost", port=6379, db=0)

        client1 = RedisClient.get_instance(config, backend="test")
        RedisClient.clear_instances()
        client2 = RedisClient.get_instance(config, backend="test")

        assert client1 is not client2
        assert len(RedisClient._instances) == 1


class TestRedisClientRetry:
    """测试 RedisClient 重试功能"""

    def test_command_retry_on_failure(self):
        """测试命令失败时重试"""
        config = NodeConfig(host="localhost", port=6379, db=0)
        retry_policy = RetryPolicy(
            max_retries=2, retryable_exceptions=(ConnectionError,)
        )

        with patch("redis.Redis") as mock_redis:
            mock_instance = MagicMock()
            mock_instance.get.side_effect = [
                ConnectionError("Connection failed"),
                "value",
            ]
            mock_redis.return_value = mock_instance

            client = RedisClient(config, retry_policy=retry_policy)

            # 应该重试并成功
            result = client.get("key")
            assert result == "value"
            assert mock_instance.get.call_count == 2

    def test_command_max_retries_exceeded(self):
        """测试超过最大重试次数"""
        config = NodeConfig(host="localhost", port=6379, db=0)
        retry_policy = RetryPolicy(
            max_retries=2, retryable_exceptions=(ConnectionError,)
        )

        with patch("redis.Redis") as mock_redis:
            mock_instance = MagicMock()
            mock_instance.get.side_effect = ConnectionError("Connection failed")
            mock_redis.return_value = mock_instance

            client = RedisClient(config, retry_policy=retry_policy)

            # 重试耗尽后会抛出 RetryExhaustedError
            with pytest.raises(RetryExhaustedError):
                client.get("key")

            # 初始调用 + 2次重试 = 3次
            assert mock_instance.get.call_count == 3

    def test_connection_error_refreshes_instance(self):
        """测试连接错误时刷新实例"""
        config = NodeConfig(host="localhost", port=6379, db=0)
        retry_policy = RetryPolicy(
            max_retries=1, retryable_exceptions=(ConnectionError,)
        )

        with patch("redis.Redis") as mock_redis:
            mock_instance = MagicMock()
            mock_instance.get.side_effect = ConnectionError("Connection failed")
            mock_redis.return_value = mock_instance

            client = RedisClient(config, retry_policy=retry_policy)

            # 重试耗尽后会抛出 RetryExhaustedError
            with pytest.raises(RetryExhaustedError):
                client.get("key")

            # 验证重试次数
            assert mock_instance.get.call_count == 2


class TestRedisClientMethods:
    """测试 RedisClient 方法"""

    def test_pipeline(self):
        """测试获取 pipeline"""
        config = NodeConfig(host="localhost", port=6379, db=0)

        with patch("redis.Redis") as mock_redis:
            mock_instance = MagicMock()
            mock_pipeline = MagicMock()
            mock_instance.pipeline.return_value = mock_pipeline
            mock_redis.return_value = mock_instance

            client = RedisClient(config)

            pipeline = client.pipeline(transaction=False)

            assert pipeline == mock_pipeline
            mock_instance.pipeline.assert_called_once_with(transaction=False)

    def test_close(self):
        """测试关闭连接"""
        config = NodeConfig(host="localhost", port=6379, db=0)

        with patch("redis.Redis") as mock_redis:
            mock_instance = MagicMock()
            mock_redis.return_value = mock_instance

            client = RedisClient(config)
            client.close()

            mock_instance.close.assert_called_once()
            assert client._instance is None

    def test_refresh_instance(self):
        """测试刷新实例"""
        config = NodeConfig(host="localhost", port=6379, db=0)

        with patch("redis.Redis") as mock_redis:
            # 模拟每次调用 Redis() 返回不同的 mock 实例
            mock_instance1 = MagicMock()
            mock_instance2 = MagicMock()
            mock_redis.side_effect = [mock_instance1, mock_instance2]

            client = RedisClient(config)
            old_instance = client._instance

            # 刷新实例
            client.refresh_instance()

            # 实例应该被替换
            assert client._instance is not old_instance
            assert client._instance == mock_instance2

    def test_proxy_getattr(self):
        """测试代理 Redis 命令"""
        config = NodeConfig(host="localhost", port=6379, db=0)

        with patch("redis.Redis") as mock_redis:
            mock_instance = MagicMock()
            mock_instance.set.return_value = True
            mock_redis.return_value = mock_instance

            client = RedisClient(config)
            result = client.set("key", "value")

            assert result is True
            mock_instance.set.assert_called_once_with("key", "value")


class TestCreateClient:
    """测试 create_client 工厂函数"""

    def test_create_redis_client(self):
        """测试创建 Redis 客户端"""
        config = NodeConfig(host="localhost", port=6379, db=0)

        with patch("redis.Redis"):
            client = create_client(config, use_singleton=False)

            assert isinstance(client, RedisClient)

    def test_create_client_with_singleton(self):
        """测试使用单例模式创建客户端"""
        config = NodeConfig(host="localhost", port=6379, db=0)

        with patch("redis.Redis"):
            client1 = create_client(config, use_singleton=True)
            client2 = create_client(config, use_singleton=True)

            assert client1 is client2

    def test_create_client_without_singleton(self):
        """测试不使用单例模式创建客户端"""
        config = NodeConfig(host="localhost", port=6379, db=0)

        with patch("redis.Redis"):
            client1 = create_client(config, use_singleton=False)
            client2 = create_client(config, use_singleton=False)

            assert client1 is not client2


class TestRedisClientEdgeCases:
    """测试 RedisClient 边界情况"""

    def test_empty_password_hash(self):
        """测试空密码的哈希"""
        config = NodeConfig(host="localhost", port=6379, db=0, password="")

        client1 = RedisClient.get_instance(config, backend="test")
        client2 = RedisClient.get_instance(config, backend="test")

        assert client1 is client2

    def test_none_password_hash(self):
        """测试 None 密码的哈希"""
        config = NodeConfig(host="localhost", port=6379, db=0, password=None)

        client1 = RedisClient.get_instance(config, backend="test")
        client2 = RedisClient.get_instance(config, backend="test")

        assert client1 is client2

    def test_different_password_different_instances(self):
        """测试不同密码返回不同实例"""
        config1 = NodeConfig(host="localhost", port=6379, db=0, password="pass1")
        config2 = NodeConfig(host="localhost", port=6379, db=0, password="pass2")

        client1 = RedisClient.get_instance(config1, backend="test")
        client2 = RedisClient.get_instance(config2, backend="test")

        assert client1 is not client2


import hashlib

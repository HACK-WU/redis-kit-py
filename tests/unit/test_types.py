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
from redis_kit.types import (
    ShardedKey,
    NodeConfig,
    PoolConfig,
    RetryPolicy,
    BackendConfig,
)


class TestShardedKey:
    """测试 ShardedKey 数据类"""

    def test_basic_creation(self):
        """测试基本创建"""
        key = ShardedKey(key="user:123", shard_key="123")
        assert key.key == "user:123"
        assert key.shard_key == "123"

    def test_immutability(self):
        """测试不可变性"""
        key = ShardedKey(key="user:123", shard_key="123")
        with pytest.raises(AttributeError):
            key.key = "new_key"  # type: ignore

        with pytest.raises(AttributeError):
            key.shard_key = "new_shard"  # type: ignore

    def test_equality(self):
        """测试相等性比较"""
        key1 = ShardedKey(key="user:123", shard_key="123")
        key2 = ShardedKey(key="user:123", shard_key="123")
        key3 = ShardedKey(key="user:456", shard_key="456")

        assert key1 == key2
        assert key1 != key3

    def test_hashable(self):
        """测试可哈希性"""
        key1 = ShardedKey(key="user:123", shard_key="123")
        key2 = ShardedKey(key="user:123", shard_key="123")

        # 可以作为字典键
        d = {key1: "value1"}
        assert d[key2] == "value1"

        # 可以作为集合元素
        s = {key1, key2}
        assert len(s) == 1


class TestNodeConfig:
    """测试 NodeConfig 数据类"""

    def test_default_values(self):
        """测试默认值"""
        config = NodeConfig(host="localhost", port=6379)
        assert config.host == "localhost"
        assert config.port == 6379
        assert config.db == 0
        assert config.password is None
        assert config.pool_config is not None

    def test_custom_pool_config(self):
        """测试自定义连接池配置"""
        pool_config = PoolConfig(max_connections=100)
        config = NodeConfig(host="localhost", port=6379, pool_config=pool_config)
        assert config.pool_config.max_connections == 100

    def test_with_password(self):
        """测试带密码配置"""
        config = NodeConfig(host="localhost", port=6379, password="secret123")
        assert config.password == "secret123"


class TestRetryPolicy:
    """测试 RetryPolicy 数据类"""

    def test_default_values(self):
        """测试默认值"""
        policy = RetryPolicy()
        assert policy.max_retries == 3
        assert policy.base_delay == 0.1
        assert policy.max_delay == 10.0
        assert policy.exponential_base == 2

    def test_custom_values(self):
        """测试自定义值"""
        policy = RetryPolicy(
            max_retries=5, base_delay=0.5, max_delay=30.0, exponential_base=3
        )
        assert policy.max_retries == 5
        assert policy.base_delay == 0.5
        assert policy.max_delay == 30.0
        assert policy.exponential_base == 3

    def test_retryable_exceptions(self):
        """测试可重试异常配置"""
        policy = RetryPolicy(retryable_exceptions=(ConnectionError, TimeoutError))
        assert ConnectionError in policy.retryable_exceptions
        assert TimeoutError in policy.retryable_exceptions


class TestBackendConfig:
    """测试 BackendConfig 数据类"""

    def test_single_node_backend(self):
        """测试单节点后端配置"""
        config = BackendConfig(name="cache", db=0, node_name="default")
        assert config.name == "cache"
        assert config.db == 0
        assert config.node_name == "default"
        assert config.routing_table is None

    def test_multi_node_backend(self):
        """测试多节点后端配置"""
        routing_table = [
            {"node_name": "node1", "min": 0, "max": 100},
            {"node_name": "node2", "min": 101, "max": 200},
        ]
        config = BackendConfig(name="sharded_cache", db=1, routing_table=routing_table)
        assert config.name == "sharded_cache"
        assert config.routing_table == routing_table
        assert config.node_name is None

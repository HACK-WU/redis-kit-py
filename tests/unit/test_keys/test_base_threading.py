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
import threading
from unittest.mock import MagicMock
from redis_kit.keys.base import RedisDataKey
from redis_kit.exceptions import KeyTemplateError, KeyFormatError
from redis_kit.types import ShardedKey


class TestRedisDataKey:
    """测试 RedisDataKey 类"""

    def test_initialization(self):
        """测试初始化"""
        key = RedisDataKey(
            key_tpl="user:{user_id}", ttl=3600, backend="cache", label="用户信息"
        )

        assert key.key_tpl == "user:{user_id}"
        assert key.ttl == 3600
        assert key.backend == "cache"
        assert key.label == "用户信息"

    def test_initialization_missing_required_params(self):
        """测试缺少必填参数时抛出异常"""
        with pytest.raises(KeyTemplateError):
            RedisDataKey(ttl=3600, backend="cache")

        with pytest.raises(KeyTemplateError):
            RedisDataKey(key_tpl="user:{id}", backend="cache")

        with pytest.raises(KeyTemplateError):
            RedisDataKey(key_tpl="user:{id}", ttl=3600)

    def test_get_key(self):
        """测试生成键"""
        key = RedisDataKey(key_tpl="user:{user_id}:profile", ttl=3600, backend="cache")

        sharded_key = key.get_key(user_id=123, shard_key="123")

        assert isinstance(sharded_key, ShardedKey)
        assert str(sharded_key) == "user:123:profile"
        assert sharded_key.shard_key == "123"

    def test_get_key_with_prefix(self):
        """测试带前缀的键"""
        key = RedisDataKey(
            key_tpl="user:{user_id}",
            ttl=3600,
            backend="cache",
            key_prefix="bk_monitor.cache",
        )

        sharded_key = key.get_key(user_id=123)

        assert str(sharded_key) == "bk_monitor.cache.user:123"

    def test_get_key_format_error(self):
        """测试格式化失败时抛出异常"""
        key = RedisDataKey(key_tpl="user:{user_id}:{name}", ttl=3600, backend="cache")

        with pytest.raises(KeyFormatError):
            key.get_key(user_id=123)  # 缺少 name 参数

    def test_get_key_shard_key_extraction(self):
        """测试分片键提取"""
        key = RedisDataKey(key_tpl="item:{item_id}", ttl=3600, backend="cache")

        # shard_key 会从 kwargs 中提取并移除
        sharded_key = key.get_key(item_id=456, shard_key="456", extra="value")

        assert sharded_key.shard_key == "456"

    def test_delete(self):
        """测试删除键"""
        key = RedisDataKey(key_tpl="test:{id}", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.delete.return_value = 1
        key.set_client(mock_proxy)

        result = key.delete(id=1)

        assert result == 1
        mock_proxy.delete.assert_called_once()

    def test_exists(self):
        """测试检查键是否存在"""
        key = RedisDataKey(key_tpl="test:{id}", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.exists.return_value = 1
        key.set_client(mock_proxy)

        result = key.exists(id=1)

        assert result is True

    def test_expire(self):
        """测试设置过期时间"""
        key = RedisDataKey(key_tpl="test:{id}", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.expire.return_value = True
        key.set_client(mock_proxy)

        result = key.expire(id=1)

        assert result is True
        mock_proxy.expire.assert_called_once()

    def test_ttl_remaining(self):
        """测试获取剩余 TTL"""
        key = RedisDataKey(key_tpl="test:{id}", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.ttl.return_value = 1800
        key.set_client(mock_proxy)

        result = key.ttl_remaining(id=1)

        assert result == 1800

    def test_repr(self):
        """测试 repr"""
        key = RedisDataKey(key_tpl="user:{id}", ttl=3600, backend="cache")

        repr_str = repr(key)

        assert "RedisDataKey" in repr_str
        assert "user:{id}" in repr_str
        assert "3600" in repr_str


class TestRedisDataKeyClientProperty:
    """测试 RedisDataKey 的 client 属性"""

    def test_client_lazy_initialization(self):
        """测试 client 延迟初始化"""
        mock_proxy = MagicMock()

        # 直接设置 mock proxy
        key = RedisDataKey(key_tpl="test:{id}", ttl=3600, backend="cache")
        key._proxy = mock_proxy

        # 在访问 client 之前，应该有 proxy
        assert key._proxy == mock_proxy

        # 访问 client 应该返回设置的 proxy
        client = key.client

        assert client == mock_proxy

    def test_set_client(self):
        """测试设置 client"""
        key = RedisDataKey(key_tpl="test:{id}", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        key.set_client(mock_proxy)

        assert key._proxy == mock_proxy

    def test_set_client_overrides_lazy_initialization(self):
        """测试 set_client 覆盖延迟初始化"""
        mock_proxy1 = MagicMock()
        mock_proxy2 = MagicMock()

        key = RedisDataKey(key_tpl="test:{id}", ttl=3600, backend="cache")

        # 设置第一个 proxy
        key.set_client(mock_proxy1)

        # 设置第二个 proxy
        key.set_client(mock_proxy2)

        # 访问 client 应该返回最后设置的 proxy
        client = key.client

        assert client == mock_proxy2


class TestRedisDataKeyThreadSafety:
    """测试 RedisDataKey 的线程安全性"""

    def test_client_property_thread_safety(self):
        """测试 client 属性的线程安全性"""
        mock_proxy = MagicMock()

        key = RedisDataKey(key_tpl="test:{id}", ttl=3600, backend="cache")
        key._proxy = mock_proxy

        clients = []
        errors = []

        def worker():
            try:
                client = key.client
                clients.append(client)
            except Exception as e:
                errors.append(e)

        # 启动多个线程
        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 检查没有错误
        assert len(errors) == 0

        # 所有线程应该获得相同的 client 实例
        assert all(c == clients[0] for c in clients)

    def test_set_client_thread_safety(self):
        """测试 set_client 的线程安全性"""
        key = RedisDataKey(key_tpl="test:{id}", ttl=3600, backend="cache")

        mock_proxies = [MagicMock() for _ in range(5)]
        results = []
        errors = []

        def worker(proxy):
            try:
                key.set_client(proxy)
                results.append(key._proxy)
            except Exception as e:
                errors.append(e)

        # 启动多个线程设置不同的 proxy
        threads = [
            threading.Thread(target=worker, args=(mock_proxies[i],)) for i in range(5)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 检查没有错误
        assert len(errors) == 0

        # 最终状态应该是一致的（某个线程设置的值）
        assert key._proxy in mock_proxies

    def test_concurrent_client_access_and_set(self):
        """测试并发访问 client 和 set_client"""
        mock_proxy1 = MagicMock()
        mock_proxy2 = MagicMock()

        key = RedisDataKey(key_tpl="test:{id}", ttl=3600, backend="cache")
        key._proxy = mock_proxy1

        clients = []
        errors = []

        def access_worker():
            try:
                client = key.client
                clients.append(client)
            except Exception as e:
                errors.append(e)

        def set_worker():
            try:
                key.set_client(mock_proxy2)
            except Exception as e:
                errors.append(e)

        # 启动多个线程
        threads = []
        for i in range(5):
            threads.append(threading.Thread(target=access_worker))
            threads.append(threading.Thread(target=set_worker))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 检查没有错误
        assert len(errors) == 0


class TestRedisDataKeyEdgeCases:
    """测试 RedisDataKey 的边界情况"""

    def test_key_prefix_already_in_template(self):
        """测试模板中已包含前缀"""
        key = RedisDataKey(
            key_tpl="bk_monitor.cache.user:{id}",
            ttl=3600,
            backend="cache",
            key_prefix="bk_monitor.cache",
        )

        sharded_key = key.get_key(id=1)

        # 不应该重复添加前缀
        assert str(sharded_key) == "bk_monitor.cache.user:1"

    def test_empty_key_prefix(self):
        """测试空键前缀"""
        key = RedisDataKey(
            key_tpl="user:{id}", ttl=3600, backend="cache", key_prefix=""
        )

        sharded_key = key.get_key(id=1)

        assert str(sharded_key) == "user:1"

    def test_global_key(self):
        """测试全局键"""
        key = RedisDataKey(
            key_tpl="global_setting", ttl=3600, backend="cache", is_global=True
        )

        sharded_key = key.get_key()

        assert str(sharded_key) == "global_setting"

    def test_extra_config_attributes(self):
        """测试扩展配置属性"""
        key = RedisDataKey(
            key_tpl="test:{id}", ttl=3600, backend="cache", custom_attr="custom_value"
        )

        assert hasattr(key, "custom_attr")
        assert key.custom_attr == "custom_value"

    def test_shard_key_converted_to_string(self):
        """测试分片键转换为字符串"""
        key = RedisDataKey(key_tpl="item:{id}", ttl=3600, backend="cache")

        sharded_key = key.get_key(id=123, shard_key=123)

        assert sharded_key.shard_key == "123"

    def test_ttl_negative_key_not_exists(self):
        """测试 TTL 为 -2 时键不存在"""
        key = RedisDataKey(key_tpl="test:{id}", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.ttl.return_value = -2
        key.set_client(mock_proxy)

        result = key.ttl_remaining(id=1)

        assert result == -2

    def test_ttl_negative_no_expire(self):
        """测试 TTL 为 -1 时永不过期"""
        key = RedisDataKey(key_tpl="test:{id}", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.ttl.return_value = -1
        key.set_client(mock_proxy)

        result = key.ttl_remaining(id=1)

        assert result == -1

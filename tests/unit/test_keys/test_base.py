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
from unittest.mock import MagicMock, PropertyMock
from redis_kit.keys.base import RedisDataKey
from redis_kit.types import ShardedKey


class TestRedisDataKey:
    """测试 RedisDataKey 基类"""

    def test_basic_key_creation(self):
        """测试基本键创建"""
        key_obj = RedisDataKey(key_tpl="user:{user_id}", ttl=3600, backend="cache")

        assert key_obj.key_tpl == "user:{user_id}"
        assert key_obj.ttl == 3600
        assert key_obj.backend == "cache"

    def test_get_key_with_format(self):
        """测试键格式化"""
        key_obj = RedisDataKey(
            key_tpl="user:{user_id}:profile", ttl=3600, backend="cache"
        )

        sharded_key = key_obj.get_key(user_id=123)

        assert isinstance(sharded_key, ShardedKey)
        assert sharded_key.key == "user:123:profile"
        assert sharded_key.shard_key == "123"

    def test_get_key_with_multiple_params(self):
        """测试多参数键格式化"""
        key_obj = RedisDataKey(
            key_tpl="project:{project_id}:user:{user_id}", ttl=3600, backend="cache"
        )

        sharded_key = key_obj.get_key(project_id=456, user_id=123)

        assert sharded_key.key == "project:456:user:123"
        # shard_key 应该使用第一个参数
        assert sharded_key.shard_key == "456"

    def test_get_key_missing_params(self):
        """测试缺少参数时抛出异常"""
        key_obj = RedisDataKey(
            key_tpl="user:{user_id}:profile", ttl=3600, backend="cache"
        )

        with pytest.raises(KeyError):
            key_obj.get_key()

    def test_prefix_handling(self):
        """测试前缀处理"""
        key_obj = RedisDataKey(
            key_tpl="user:{user_id}", ttl=3600, backend="cache", prefix="myapp"
        )

        sharded_key = key_obj.get_key(user_id=123)
        assert sharded_key.key == "myapp:user:123"

    def test_expire_method(self):
        """测试 expire 方法"""
        key_obj = RedisDataKey(key_tpl="user:{user_id}", ttl=3600, backend="cache")

        # Mock proxy
        mock_proxy = MagicMock()
        mock_proxy.expire.return_value = True
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        sharded_key = key_obj.get_key(user_id=123)
        result = key_obj.expire(sharded_key, 7200)

        assert result is True
        mock_proxy.expire.assert_called_once()

    def test_delete_method(self):
        """测试 delete 方法"""
        key_obj = RedisDataKey(key_tpl="user:{user_id}", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.delete.return_value = 1
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        sharded_key = key_obj.get_key(user_id=123)
        result = key_obj.delete(sharded_key)

        assert result == 1
        mock_proxy.delete.assert_called_once()

    def test_exists_method(self):
        """测试 exists 方法"""
        key_obj = RedisDataKey(key_tpl="user:{user_id}", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.exists.return_value = 1
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        sharded_key = key_obj.get_key(user_id=123)
        result = key_obj.exists(sharded_key)

        assert result is True
        mock_proxy.exists.assert_called_once()

    def test_ttl_remaining_method(self):
        """测试 ttl_remaining 方法"""
        key_obj = RedisDataKey(key_tpl="user:{user_id}", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.ttl.return_value = 1800
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        sharded_key = key_obj.get_key(user_id=123)
        result = key_obj.ttl_remaining(sharded_key)

        assert result == 1800
        mock_proxy.ttl.assert_called_once()

    def test_no_ttl(self):
        """测试无 TTL 的键"""
        key_obj = RedisDataKey(key_tpl="persistent:{key}", ttl=None, backend="cache")

        assert key_obj.ttl is None

    def test_custom_shard_key(self):
        """测试自定义 shard_key"""
        key_obj = RedisDataKey(
            key_tpl="user:{user_id}:session:{session_id}", ttl=3600, backend="cache"
        )

        # 默认使用第一个参数作为 shard_key
        sharded_key = key_obj.get_key(user_id=123, session_id="abc")
        assert sharded_key.shard_key == "123"

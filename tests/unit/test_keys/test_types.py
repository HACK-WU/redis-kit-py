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

from unittest.mock import MagicMock, PropertyMock
from redis_kit.keys.types import StringKey, HashKey, SetKey, ListKey, SortedSetKey


class TestStringKey:
    """测试 StringKey 类"""

    def test_get_method(self):
        """测试 get 方法"""
        key_obj = StringKey(key_tpl="user:{user_id}:name", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.get.return_value = "Alice"
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.get(user_id=123)
        assert result == "Alice"

    def test_set_method(self):
        """测试 set 方法"""
        key_obj = StringKey(key_tpl="user:{user_id}:name", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.set.return_value = True
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.set("Bob", user_id=123)
        assert result is True

    def test_setnx_method(self):
        """测试 setnx 方法"""
        key_obj = StringKey(key_tpl="lock:{resource}", ttl=30, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.set.return_value = True
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.setnx("token123", resource="res1")
        assert result is True

    def test_incr_method(self):
        """测试 incr 方法"""
        key_obj = StringKey(key_tpl="counter:{name}", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.incr.return_value = 5
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.incr(name="page_views")
        assert result == 5


class TestHashKey:
    """测试 HashKey 类"""

    def test_hget_method(self):
        """测试 hget 方法"""
        key_obj = HashKey(
            key_tpl="user:{user_id}:profile",
            field_tpl="field:{field_name}",
            ttl=3600,
            backend="cache",
        )

        mock_proxy = MagicMock()
        mock_proxy.hget.return_value = "value"
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.hget(user_id=123, field_name="email")
        assert result == "value"

    def test_hset_method(self):
        """测试 hset 方法"""
        key_obj = HashKey(
            key_tpl="user:{user_id}:profile", field_tpl=None, ttl=3600, backend="cache"
        )

        mock_proxy = MagicMock()
        mock_proxy.hset.return_value = 1
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.hset("email", "alice@example.com", user_id=123)
        assert result == 1

    def test_hgetall_method(self):
        """测试 hgetall 方法"""
        key_obj = HashKey(key_tpl="user:{user_id}:settings", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.hgetall.return_value = {"theme": "dark", "lang": "en"}
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.hgetall(user_id=123)
        assert result == {"theme": "dark", "lang": "en"}

    def test_hmset_method(self):
        """测试 hmset 方法"""
        key_obj = HashKey(key_tpl="user:{user_id}:profile", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.hset.return_value = 2
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        mapping = {"name": "Alice", "age": "30"}
        result = key_obj.hmset(mapping, user_id=123)
        assert result == 2


class TestSetKey:
    """测试 SetKey 类"""

    def test_sadd_method(self):
        """测试 sadd 方法"""
        key_obj = SetKey(key_tpl="user:{user_id}:tags", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.sadd.return_value = 2
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.sadd(["python", "redis"], user_id=123)
        assert result == 2

    def test_smembers_method(self):
        """测试 smembers 方法"""
        key_obj = SetKey(key_tpl="user:{user_id}:tags", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.smembers.return_value = {"python", "redis"}
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.smembers(user_id=123)
        assert result == {"python", "redis"}

    def test_sismember_method(self):
        """测试 sismember 方法"""
        key_obj = SetKey(key_tpl="user:{user_id}:tags", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.sismember.return_value = True
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.sismember("python", user_id=123)
        assert result is True


class TestListKey:
    """测试 ListKey 类"""

    def test_lpush_method(self):
        """测试 lpush 方法"""
        key_obj = ListKey(key_tpl="queue:{name}", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.lpush.return_value = 3
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.lpush(["task3", "task2", "task1"], name="tasks")
        assert result == 3

    def test_lrange_method(self):
        """测试 lrange 方法"""
        key_obj = ListKey(key_tpl="queue:{name}", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.lrange.return_value = ["task1", "task2", "task3"]
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.lrange(0, -1, name="tasks")
        assert result == ["task1", "task2", "task3"]

    def test_llen_method(self):
        """测试 llen 方法"""
        key_obj = ListKey(key_tpl="queue:{name}", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.llen.return_value = 10
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.llen(name="tasks")
        assert result == 10


class TestSortedSetKey:
    """测试 SortedSetKey 类"""

    def test_zadd_method(self):
        """测试 zadd 方法"""
        key_obj = SortedSetKey(
            key_tpl="leaderboard:{game_id}", ttl=3600, backend="cache"
        )

        mock_proxy = MagicMock()
        mock_proxy.zadd.return_value = 2
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.zadd({"player1": 100, "player2": 200}, game_id=1)
        assert result == 2

    def test_zrange_method(self):
        """测试 zrange 方法"""
        key_obj = SortedSetKey(
            key_tpl="leaderboard:{game_id}", ttl=3600, backend="cache"
        )

        mock_proxy = MagicMock()
        mock_proxy.zrange.return_value = ["player1", "player2"]
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.zrange(0, 10, game_id=1)
        assert result == ["player1", "player2"]

    def test_zscore_method(self):
        """测试 zscore 方法"""
        key_obj = SortedSetKey(
            key_tpl="leaderboard:{game_id}", ttl=3600, backend="cache"
        )

        mock_proxy = MagicMock()
        mock_proxy.zscore.return_value = 100.0
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.zscore("player1", game_id=1)
        assert result == 100.0

    def test_zrangebyscore_method(self):
        """测试 zrangebyscore 方法"""
        key_obj = SortedSetKey(key_tpl="delay_queue:{name}", ttl=3600, backend="cache")

        mock_proxy = MagicMock()
        mock_proxy.zrangebyscore.return_value = ["task1", "task2"]
        type(key_obj)._proxy = PropertyMock(return_value=mock_proxy)

        result = key_obj.zrangebyscore(0, 1000, name="tasks")
        assert result == ["task1", "task2"]

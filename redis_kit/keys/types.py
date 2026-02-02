# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit 键类型定义模块

提供各种 Redis 数据结构对应的键类型实现
"""

from typing import Any, Dict, List, Optional

from redis_kit.exceptions import KeyTemplateError
from redis_kit.keys.base import RedisDataKey


class StringKey(RedisDataKey):
    """
    String 数据结构的 Key 对象

    用于管理 Redis String 类型的键

    Example:
        >>> config_key = StringKey(
        ...     key_tpl="config:{config_name}",
        ...     ttl=3600,
        ...     backend="cache"
        ... )
        >>> key = config_key.get_key(config_name="app_settings")
    """

    def get(self, **key_kwargs) -> Optional[str]:
        """获取字符串值"""
        key = self.get_key(**key_kwargs)
        return self.client.get(key)

    def set(self, value: str, **key_kwargs) -> bool:
        """设置字符串值"""
        key = self.get_key(**key_kwargs)
        return self.client.set(key, value, ex=self.ttl)

    def setnx(self, value: str, **key_kwargs) -> bool:
        """仅当键不存在时设置值"""
        key = self.get_key(**key_kwargs)
        return self.client.set(key, value, ex=self.ttl, nx=True)

    def incr(self, amount: int = 1, **key_kwargs) -> int:
        """递增值"""
        key = self.get_key(**key_kwargs)
        return self.client.incrby(key, amount)

    def decr(self, amount: int = 1, **key_kwargs) -> int:
        """递减值"""
        key = self.get_key(**key_kwargs)
        return self.client.decrby(key, amount)


class HashKey(RedisDataKey):
    """
    Hash 数据结构的 Key 对象

    用于管理 Redis Hash 类型的键，支持字段模板

    Attributes:
        field_tpl: 字段模板字符串

    Example:
        >>> user_hash = HashKey(
        ...     key_tpl="user:{user_id}",
        ...     field_tpl="{field_name}",
        ...     ttl=3600,
        ...     backend="cache"
        ... )
        >>> key = user_hash.get_key(user_id=123)
        >>> field = user_hash.get_field(field_name="email")
    """

    def __init__(
        self,
        key_tpl: str = None,
        ttl: int = None,
        backend: str = None,
        field_tpl: str = None,
        **extra_config,
    ):
        super().__init__(key_tpl, ttl, backend, **extra_config)
        if not field_tpl:
            raise KeyTemplateError(key_tpl or "", "field_tpl is required for HashKey")
        self.field_tpl = field_tpl

    def get_field(self, **kwargs) -> str:
        """
        生成格式化后的字段名

        Args:
            **kwargs: 字段模板格式化参数

        Returns:
            str: 格式化后的字段名
        """
        return self.field_tpl.format(**kwargs)

    def hget(self, field_kwargs: Dict[str, Any] = None, **key_kwargs) -> Optional[str]:
        """获取 Hash 字段值"""
        key = self.get_key(**key_kwargs)
        field = self.get_field(**(field_kwargs or {}))
        return self.client.hget(key, field)

    def hset(
        self, value: str, field_kwargs: Dict[str, Any] = None, **key_kwargs
    ) -> int:
        """设置 Hash 字段值"""
        key = self.get_key(**key_kwargs)
        field = self.get_field(**(field_kwargs or {}))
        result = self.client.hset(key, field, value)
        self.client.expire(key, self.ttl)
        return result

    def hdel(self, field_kwargs: Dict[str, Any] = None, **key_kwargs) -> int:
        """删除 Hash 字段"""
        key = self.get_key(**key_kwargs)
        field = self.get_field(**(field_kwargs or {}))
        return self.client.hdel(key, field)

    def hgetall(self, **key_kwargs) -> Dict[str, str]:
        """获取 Hash 所有字段和值"""
        key = self.get_key(**key_kwargs)
        return self.client.hgetall(key)

    def hmset(self, mapping: Dict[str, str], **key_kwargs) -> bool:
        """批量设置 Hash 字段"""
        key = self.get_key(**key_kwargs)
        result = self.client.hset(key, mapping=mapping)
        self.client.expire(key, self.ttl)
        return result

    def hmget(self, fields: List[str], **key_kwargs) -> List[Optional[str]]:
        """批量获取 Hash 字段值"""
        key = self.get_key(**key_kwargs)
        return self.client.hmget(key, fields)

    def hexists(self, field_kwargs: Dict[str, Any] = None, **key_kwargs) -> bool:
        """检查 Hash 字段是否存在"""
        key = self.get_key(**key_kwargs)
        field = self.get_field(**(field_kwargs or {}))
        return self.client.hexists(key, field)

    def hincrby(
        self, amount: int = 1, field_kwargs: Dict[str, Any] = None, **key_kwargs
    ) -> int:
        """递增 Hash 字段值"""
        key = self.get_key(**key_kwargs)
        field = self.get_field(**(field_kwargs or {}))
        return self.client.hincrby(key, field, amount)


class SetKey(RedisDataKey):
    """
    Set 数据结构的 Key 对象

    用于管理 Redis Set 类型的键

    Example:
        >>> tags_key = SetKey(
        ...     key_tpl="article:{article_id}:tags",
        ...     ttl=86400,
        ...     backend="cache"
        ... )
    """

    def sadd(self, *values: str, **key_kwargs) -> int:
        """添加成员到集合"""
        key = self.get_key(**key_kwargs)
        result = self.client.sadd(key, *values)
        self.client.expire(key, self.ttl)
        return result

    def srem(self, *values: str, **key_kwargs) -> int:
        """从集合移除成员"""
        key = self.get_key(**key_kwargs)
        return self.client.srem(key, *values)

    def smembers(self, **key_kwargs) -> set:
        """获取集合所有成员"""
        key = self.get_key(**key_kwargs)
        return self.client.smembers(key)

    def sismember(self, value: str, **key_kwargs) -> bool:
        """检查成员是否在集合中"""
        key = self.get_key(**key_kwargs)
        return self.client.sismember(key, value)

    def scard(self, **key_kwargs) -> int:
        """获取集合成员数量"""
        key = self.get_key(**key_kwargs)
        return self.client.scard(key)

    def spop(self, count: int = 1, **key_kwargs) -> Optional[str]:
        """随机移除并返回成员"""
        key = self.get_key(**key_kwargs)
        return self.client.spop(key, count)

    def srandmember(self, count: int = 1, **key_kwargs) -> List[str]:
        """随机获取成员（不移除）"""
        key = self.get_key(**key_kwargs)
        return self.client.srandmember(key, count)


class ListKey(RedisDataKey):
    """
    List 数据结构的 Key 对象

    用于管理 Redis List 类型的键

    Example:
        >>> queue_key = ListKey(
        ...     key_tpl="queue:{queue_name}",
        ...     ttl=3600,
        ...     backend="queue"
        ... )
    """

    def lpush(self, *values: str, **key_kwargs) -> int:
        """从左侧插入元素"""
        key = self.get_key(**key_kwargs)
        result = self.client.lpush(key, *values)
        self.client.expire(key, self.ttl)
        return result

    def rpush(self, *values: str, **key_kwargs) -> int:
        """从右侧插入元素"""
        key = self.get_key(**key_kwargs)
        result = self.client.rpush(key, *values)
        self.client.expire(key, self.ttl)
        return result

    def lpop(self, count: int = None, **key_kwargs) -> Optional[str]:
        """从左侧弹出元素"""
        key = self.get_key(**key_kwargs)
        if count:
            return self.client.lpop(key, count)
        return self.client.lpop(key)

    def rpop(self, count: int = None, **key_kwargs) -> Optional[str]:
        """从右侧弹出元素"""
        key = self.get_key(**key_kwargs)
        if count:
            return self.client.rpop(key, count)
        return self.client.rpop(key)

    def lrange(self, start: int = 0, end: int = -1, **key_kwargs) -> List[str]:
        """获取列表范围内的元素"""
        key = self.get_key(**key_kwargs)
        return self.client.lrange(key, start, end)

    def llen(self, **key_kwargs) -> int:
        """获取列表长度"""
        key = self.get_key(**key_kwargs)
        return self.client.llen(key)

    def lindex(self, index: int, **key_kwargs) -> Optional[str]:
        """获取指定位置的元素"""
        key = self.get_key(**key_kwargs)
        return self.client.lindex(key, index)

    def lset(self, index: int, value: str, **key_kwargs) -> bool:
        """设置指定位置的元素"""
        key = self.get_key(**key_kwargs)
        return self.client.lset(key, index, value)

    def ltrim(self, start: int, end: int, **key_kwargs) -> bool:
        """修剪列表"""
        key = self.get_key(**key_kwargs)
        return self.client.ltrim(key, start, end)


class SortedSetKey(RedisDataKey):
    """
    SortedSet (ZSet) 数据结构的 Key 对象

    用于管理 Redis Sorted Set 类型的键

    Example:
        >>> leaderboard_key = SortedSetKey(
        ...     key_tpl="leaderboard:{game_id}",
        ...     ttl=86400,
        ...     backend="cache"
        ... )
    """

    def zadd(
        self,
        mapping: Dict[str, float],
        nx: bool = False,
        xx: bool = False,
        **key_kwargs,
    ) -> int:
        """添加成员到有序集合"""
        key = self.get_key(**key_kwargs)
        result = self.client.zadd(key, mapping, nx=nx, xx=xx)
        self.client.expire(key, self.ttl)
        return result

    def zrem(self, *members: str, **key_kwargs) -> int:
        """移除成员"""
        key = self.get_key(**key_kwargs)
        return self.client.zrem(key, *members)

    def zscore(self, member: str, **key_kwargs) -> Optional[float]:
        """获取成员分数"""
        key = self.get_key(**key_kwargs)
        return self.client.zscore(key, member)

    def zrank(self, member: str, **key_kwargs) -> Optional[int]:
        """获取成员排名（从小到大）"""
        key = self.get_key(**key_kwargs)
        return self.client.zrank(key, member)

    def zrevrank(self, member: str, **key_kwargs) -> Optional[int]:
        """获取成员排名（从大到小）"""
        key = self.get_key(**key_kwargs)
        return self.client.zrevrank(key, member)

    def zrange(
        self, start: int = 0, end: int = -1, withscores: bool = False, **key_kwargs
    ) -> List:
        """获取范围内的成员（按分数从小到大）"""
        key = self.get_key(**key_kwargs)
        return self.client.zrange(key, start, end, withscores=withscores)

    def zrevrange(
        self, start: int = 0, end: int = -1, withscores: bool = False, **key_kwargs
    ) -> List:
        """获取范围内的成员（按分数从大到小）"""
        key = self.get_key(**key_kwargs)
        return self.client.zrevrange(key, start, end, withscores=withscores)

    def zrangebyscore(
        self,
        min_score: float,
        max_score: float,
        start: int = None,
        num: int = None,
        withscores: bool = False,
        **key_kwargs,
    ) -> List:
        """按分数范围获取成员"""
        key = self.get_key(**key_kwargs)
        return self.client.zrangebyscore(
            key, min_score, max_score, start=start, num=num, withscores=withscores
        )

    def zcard(self, **key_kwargs) -> int:
        """获取成员数量"""
        key = self.get_key(**key_kwargs)
        return self.client.zcard(key)

    def zcount(self, min_score: float, max_score: float, **key_kwargs) -> int:
        """统计分数范围内的成员数量"""
        key = self.get_key(**key_kwargs)
        return self.client.zcount(key, min_score, max_score)

    def zincrby(self, member: str, amount: float = 1.0, **key_kwargs) -> float:
        """增加成员分数"""
        key = self.get_key(**key_kwargs)
        return self.client.zincrby(key, amount, member)

    def zremrangebyscore(self, min_score: float, max_score: float, **key_kwargs) -> int:
        """按分数范围移除成员"""
        key = self.get_key(**key_kwargs)
        return self.client.zremrangebyscore(key, min_score, max_score)

    def zremrangebyrank(self, start: int, end: int, **key_kwargs) -> int:
        """按排名范围移除成员"""
        key = self.get_key(**key_kwargs)
        return self.client.zremrangebyrank(key, start, end)

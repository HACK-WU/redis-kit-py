"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit 锁模块
"""

from redis_kit.lock.base import BaseLock
from redis_kit.lock.decorators import (
    RateLimitExceededError,
    rate_limited,
    service_lock,
    singleton_task,
)
from redis_kit.lock.redis_lock import MultiRedisLock, ReadWriteLock, RedisLock

__all__ = [
    "BaseLock",
    "RedisLock",
    "MultiRedisLock",
    "ReadWriteLock",
    "service_lock",
    "singleton_task",
    "rate_limited",
    "RateLimitExceededError",
]

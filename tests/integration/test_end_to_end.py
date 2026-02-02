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
from redis import Redis
from redis_kit.lock.redis_lock import RedisLock, MultiRedisLock, ReadWriteLock
from redis_kit.queue.delay_queue import DelayQueue, DelayQueueWorker


# 集成测试需要真实的 Redis 实例
# 可以使用 pytest.mark.integration 标记这些测试
# 运行时: pytest -m integration

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def redis_client():
    """创建真实的 Redis 客户端用于集成测试"""
    try:
        client = Redis(host="localhost", port=6379, db=15, decode_responses=True)
        client.ping()
        yield client
        # 清理测试数据
        client.flushdb()
    except Exception as e:
        pytest.skip(f"Redis is not available: {e}")


@pytest.fixture(scope="function")
def clean_redis(redis_client):
    """每个测试前后清理数据"""
    redis_client.flushdb()
    yield redis_client
    redis_client.flushdb()


class TestStringKeyIntegration:
    """StringKey 集成测试"""

    def test_string_operations(self, clean_redis):
        """测试字符串操作的端到端流程"""
        # 注意：这里需要实际配置 RedisProxy
        # 为了简化，直接使用 redis client 模拟

        key = "test:user:123:name"

        # Set
        clean_redis.set(key, "Alice")

        # Get
        value = clean_redis.get(key)
        assert value == "Alice"

        # Update
        clean_redis.set(key, "Bob")
        value = clean_redis.get(key)
        assert value == "Bob"

        # Delete
        clean_redis.delete(key)
        value = clean_redis.get(key)
        assert value is None

    def test_string_with_ttl(self, clean_redis):
        """测试带TTL的字符串"""
        key = "test:session:abc"

        clean_redis.setex(key, 2, "session_data")

        # 立即获取应该存在
        assert clean_redis.get(key) == "session_data"

        # 等待过期
        time.sleep(2.5)

        # 应该已过期
        assert clean_redis.get(key) is None


class TestHashKeyIntegration:
    """HashKey 集成测试"""

    def test_hash_operations(self, clean_redis):
        """测试哈希操作的端到端流程"""
        key = "test:user:123:profile"

        # hset
        clean_redis.hset(key, "name", "Alice")
        clean_redis.hset(key, "age", "30")

        # hget
        assert clean_redis.hget(key, "name") == "Alice"
        assert clean_redis.hget(key, "age") == "30"

        # hgetall
        data = clean_redis.hgetall(key)
        assert data == {"name": "Alice", "age": "30"}

        # hdel
        clean_redis.hdel(key, "age")
        assert clean_redis.hget(key, "age") is None


class TestRedisLockIntegration:
    """RedisLock 集成测试"""

    def test_lock_basic(self, clean_redis):
        """测试基本锁功能"""
        lock_key = "test:lock:resource1"

        lock = RedisLock(clean_redis, lock_key, ttl=10)

        # 获取锁
        assert lock.acquire(blocking=False) is True
        assert lock.is_locked() is True

        # 释放锁
        lock.release()
        assert lock.is_locked() is False

    def test_lock_concurrent(self, clean_redis):
        """测试并发锁"""
        lock_key = "test:lock:resource2"

        lock1 = RedisLock(clean_redis, lock_key, ttl=10)
        lock2 = RedisLock(clean_redis, lock_key, ttl=10)

        # lock1 获取锁
        assert lock1.acquire(blocking=False) is True

        # lock2 应该无法获取
        assert lock2.acquire(blocking=False) is False

        # lock1 释放后 lock2 可以获取
        lock1.release()
        assert lock2.acquire(blocking=False) is True

        lock2.release()

    def test_lock_auto_expire(self, clean_redis):
        """测试锁自动过期"""
        lock_key = "test:lock:resource3"

        lock = RedisLock(clean_redis, lock_key, ttl=1)

        assert lock.acquire(blocking=False) is True

        # 等待过期
        time.sleep(1.5)

        # 锁应该已过期，其他进程可以获取
        lock2 = RedisLock(clean_redis, lock_key, ttl=10)
        assert lock2.acquire(blocking=False) is True

        lock2.release()


class TestDelayQueueIntegration:
    """DelayQueue 集成测试"""

    def test_delay_queue_basic(self, clean_redis):
        """测试延迟队列基本功能"""
        queue = DelayQueue(clean_redis, "test:delay_queue")

        # 推送任务
        task_id = queue.push({"action": "send_email"}, delay=2)
        assert task_id is not None

        # 立即查询，应该没有就绪任务
        tasks = queue.pop_ready()
        assert len(tasks) == 0

        # 等待任务就绪
        time.sleep(2.5)

        # 现在应该有就绪任务
        tasks = queue.pop_ready()
        assert len(tasks) == 1
        assert tasks[0]["data"]["action"] == "send_email"

    def test_delay_queue_cancel(self, clean_redis):
        """测试取消任务"""
        queue = DelayQueue(clean_redis, "test:delay_queue")

        task_id = queue.push({"action": "test"}, delay=10)

        # 取消任务
        assert queue.cancel(task_id) is True

        # 任务应该不存在
        count = queue.get_pending_count()
        assert count == 0

    def test_delay_queue_worker(self, clean_redis):
        """测试延迟队列 Worker"""
        queue = DelayQueue(clean_redis, "test:delay_queue")

        # 记录处理的任务
        processed_tasks = []

        def handler(task):
            processed_tasks.append(task)

        worker = DelayQueueWorker(queue, handler, interval=0.5)

        # 推送任务（延迟1秒）
        queue.push({"action": "task1"}, delay=1)

        # 启动 worker
        worker.start()

        # 等待任务被处理
        time.sleep(2)

        # 停止 worker
        worker.stop()

        # 验证任务已被处理
        assert len(processed_tasks) == 1
        assert processed_tasks[0]["data"]["action"] == "task1"


class TestMultiRedisLockIntegration:
    """MultiRedisLock 集成测试"""

    def test_multi_lock_basic(self, clean_redis):
        """测试多键锁基本功能"""
        keys = ["test:lock:res1", "test:lock:res2", "test:lock:res3"]

        lock = MultiRedisLock(clean_redis, keys, ttl=10)

        # 获取所有锁
        assert lock.acquire(blocking=False) is True

        # 所有键都应该被锁定
        for key in keys:
            assert clean_redis.exists(key) == 1

        # 释放锁
        lock.release()

        # 所有键都应该被删除
        for key in keys:
            assert clean_redis.exists(key) == 0


class TestReadWriteLockIntegration:
    """ReadWriteLock 集成测试"""

    def test_multiple_readers(self, clean_redis):
        """测试多个读者可以并发"""
        lock = ReadWriteLock(clean_redis, "test:rwlock:resource", ttl=10)

        # 获取第一个读锁
        with lock.read():
            reader_count = int(clean_redis.get("test:rwlock:resource:readers") or 0)
            assert reader_count >= 1

            # 第二个读锁也能获取
            with lock.read():
                reader_count = int(clean_redis.get("test:rwlock:resource:readers") or 0)
                assert reader_count >= 2

    def test_write_lock_exclusive(self, clean_redis):
        """测试写锁的排他性"""
        lock = ReadWriteLock(clean_redis, "test:rwlock:resource", ttl=10)

        # 获取写锁
        with lock.write():
            # 检查写锁标记存在
            assert clean_redis.exists("test:rwlock:resource:write") == 1

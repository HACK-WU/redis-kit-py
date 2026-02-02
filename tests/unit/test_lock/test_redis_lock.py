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
import threading
from unittest.mock import MagicMock
from redis_kit.lock.redis_lock import (
    RedisLock,
    MultiRedisLock,
    ReadWriteLock,
    RELEASE_LOCK_SCRIPT,
    EXTEND_LOCK_SCRIPT,
    ACQUIRE_READ_LOCK_SCRIPT,
    RELEASE_READ_LOCK_SCRIPT,
    ACQUIRE_WRITE_LOCK_SCRIPT,
    RELEASE_WRITE_LOCK_SCRIPT,
)
from redis_kit.exceptions import LockAcquisitionError, LockExtendError


class TestRedisLock:
    """测试 RedisLock 类"""

    def test_acquire_and_release(self):
        """测试获取和释放锁"""
        client = MagicMock()
        client.set.return_value = True
        client.get.return_value = None

        lock = RedisLock(client, "test_lock", ttl=10)

        # 获取锁
        assert lock.acquire(blocking=False) is True
        assert lock.is_locked() is True

        # 模拟锁已持有
        client.get.return_value = lock._token

        # 释放锁
        client.delete.return_value = 1
        lock.release()
        assert lock.is_locked() is False

    def test_acquire_already_locked(self):
        """测试锁已被占用时的获取"""
        client = MagicMock()
        client.set.return_value = False  # SET NX 失败

        lock = RedisLock(client, "test_lock", ttl=10)

        # 非阻塞模式应该立即返回 False
        assert lock.acquire(blocking=False) is False

    def test_acquire_with_timeout(self):
        """测试带超时的获取"""
        client = MagicMock()
        # 第一次失败，第二次成功
        client.set.side_effect = [False, False, True]

        lock = RedisLock(client, "test_lock", ttl=10)

        start = time.time()
        result = lock.acquire(blocking=True, timeout=1.0)
        elapsed = time.time() - start

        assert result is True
        assert elapsed < 1.5  # 应该在超时前成功

    def test_acquire_timeout_exhausted(self):
        """测试超时耗尽"""
        client = MagicMock()
        client.set.return_value = False  # 始终失败

        lock = RedisLock(client, "test_lock", ttl=10)

        start = time.time()
        result = lock.acquire(blocking=True, timeout=0.5)
        elapsed = time.time() - start

        assert result is False
        assert 0.4 < elapsed < 0.7

    def test_context_manager(self):
        """测试上下文管理器"""
        client = MagicMock()
        client.set.return_value = True
        client.delete.return_value = 1

        lock = RedisLock(client, "test_lock", ttl=10)

        # 模拟获取锁后 get 返回 token
        def set_side_effect(*args, **kwargs):
            client.get.return_value = lock._token
            return True

        client.set.side_effect = set_side_effect

        with lock:
            assert lock.is_locked() is True

        assert lock.is_locked() is False

    def test_context_manager_acquire_fail(self):
        """测试上下文管理器获取锁失败"""
        client = MagicMock()
        client.set.return_value = False

        lock = RedisLock(client, "test_lock", ttl=10)

        with pytest.raises(LockAcquisitionError):
            with lock:
                pass

    def test_extend_lock(self):
        """测试延长锁时间"""
        client = MagicMock()
        client.set.return_value = True
        client.expire.return_value = True
        client.get.return_value = None

        lock = RedisLock(client, "test_lock", ttl=10)
        lock.acquire(blocking=False)

        # 模拟锁已持有
        client.get.return_value = lock._token

        result = lock.extend(20)
        assert result is True
        client.expire.assert_called()

    def test_release_without_acquire(self):
        """测试未获取锁就释放"""
        client = MagicMock()
        lock = RedisLock(client, "test_lock", ttl=10)

        # 应该不会抛出异常，只是不做任何操作
        lock.release()


class TestMultiRedisLock:
    """测试 MultiRedisLock 类"""

    def test_acquire_all_locks(self):
        """测试获取多个锁"""
        client = MagicMock()
        client.set.return_value = True
        client.delete.return_value = 1

        keys = ["lock1", "lock2", "lock3"]
        lock = MultiRedisLock(client, keys, ttl=10)

        # 模拟所有锁都成功获取
        client.get.return_value = lock._token

        assert lock.acquire(blocking=False) is True
        assert lock.is_locked() is True

        lock.release()
        assert lock.is_locked() is False

    def test_acquire_partial_failure(self):
        """测试部分锁获取失败"""
        client = MagicMock()
        # 第一个锁成功，第二个失败
        client.set.side_effect = [True, False]
        client.delete.return_value = 1

        keys = ["lock1", "lock2"]
        lock = MultiRedisLock(client, keys, ttl=10)

        # 应该回滚已获取的锁
        assert lock.acquire(blocking=False) is False

    def test_context_manager(self):
        """测试上下文管理器"""
        client = MagicMock()
        client.set.return_value = True
        client.delete.return_value = 1

        keys = ["lock1", "lock2", "lock3"]
        lock = MultiRedisLock(client, keys, ttl=10)

        # 模拟获取锁后 get 返回 token
        client.get.return_value = lock._token

        with lock:
            assert lock.is_locked() is True

        assert lock.is_locked() is False


class TestReadWriteLock:
    """测试 ReadWriteLock 类"""

    def test_multiple_readers(self):
        """测试多个读锁"""
        client = MagicMock()
        client.incr.return_value = 1
        client.decr.return_value = 0
        client.get.return_value = None  # 没有写锁

        lock = ReadWriteLock(client, "resource", ttl=10)

        # 第一个读者
        with lock.read():
            client.incr.assert_called()

            # 第二个读者
            client.incr.return_value = 2
            with lock.read():
                pass

    def test_read_blocked_by_write(self):
        """测试读锁被写锁阻塞"""
        client = MagicMock()
        client.get.return_value = "write_token"  # 有写锁

        lock = ReadWriteLock(client, "resource", ttl=10)

        # 读锁应该被阻塞
        with pytest.raises(LockAcquisitionError):
            with lock.read():
                pass

    def test_write_lock(self):
        """测试写锁"""
        client = MagicMock()
        client.set.return_value = True  # 获取写锁成功
        client.get.return_value = "0"  # 没有读者
        client.delete.return_value = 1

        lock = ReadWriteLock(client, "resource", ttl=10)

        # 模拟获取锁后的状态
        def set_side_effect(*args, **kwargs):
            client.get.return_value = lock._write_token
            return True

        client.set.side_effect = set_side_effect

        with lock.write():
            client.set.assert_called()

    def test_write_blocked_by_readers(self):
        """测试写锁被读锁阻塞"""
        client = MagicMock()
        client.get.side_effect = [None, "5"]  # 写锁可获取，但有5个读者

        lock = ReadWriteLock(client, "resource", ttl=10)

        # 写锁应该被阻塞
        with pytest.raises(LockAcquisitionError):
            with lock.write():
                pass

    def test_write_blocks_read(self):
        """测试写锁阻塞读锁"""
        client = MagicMock()
        # 先获取写锁
        client.set.return_value = True
        client.get.side_effect = ["0", "write_token"]
        client.delete.return_value = 1

        lock = ReadWriteLock(client, "resource", ttl=10)

        # 模拟写锁已持有
        def set_side_effect(*args, **kwargs):
            client.get.side_effect = ["write_token", "write_token"]
            return True

        client.set.side_effect = set_side_effect

        with lock.write():
            # 尝试获取读锁应该失败
            client.get.side_effect = ["write_token"]
            with pytest.raises(LockAcquisitionError):
                with lock.read():
                    pass


class TestRedisLockLuaScript:
    """测试 Lua 脚本的原子性"""

    def test_release_uses_lua_script(self):
        """测试 release 使用 Lua 脚本"""
        client = MagicMock()
        client.eval.return_value = 1  # Lua 脚本返回成功

        lock = RedisLock(client, "test_lock", ttl=10)
        lock.acquire(blocking=False)

        # 手动设置 token（模拟 Redis 成功返回的情况）
        lock._token = "test_token_123"

        lock.release()

        # 验证调用的是 Lua 脚本，而不是直接的 delete
        client.eval.assert_called_once()
        call_args = client.eval.call_args
        assert call_args[0][0] == RELEASE_LOCK_SCRIPT
        assert call_args[0][1] == 1  # KEYS 数量
        assert call_args[0][2] == "test_lock"  # KEYS[1]
        assert call_args[0][3] == "test_token_123"  # ARGV[1]

    def test_extend_uses_lua_script(self):
        """测试 extend 使用 Lua 脚本"""
        client = MagicMock()
        client.ttl.return_value = 30  # 剩余 TTL
        client.eval.return_value = 1  # Lua 脚本返回成功

        lock = RedisLock(client, "test_lock", ttl=10)
        lock.acquire(blocking=False)

        # 手动设置 token
        lock._token = "test_token_123"

        lock.extend(20)

        # 验证调用的是 Lua 脚本
        client.eval.assert_called_once()
        call_args = client.eval.call_args
        assert call_args[0][0] == EXTEND_LOCK_SCRIPT
        assert call_args[0][1] == 1
        assert call_args[0][2] == "test_lock"
        assert call_args[0][3] == "test_token_123"
        assert call_args[0][4] == 50  # new_ttl = 30 + 20

    def test_extend_lock_expired(self):
        """测试延长已过期的锁"""
        client = MagicMock()
        client.ttl.return_value = -2  # 锁不存在

        lock = RedisLock(client, "test_lock", ttl=10)
        lock._token = "test_token"

        with pytest.raises(LockExtendError):
            lock.extend(20)

    def test_extend_without_token(self):
        """测试无 token 时延长锁"""
        client = MagicMock()

        lock = RedisLock(client, "test_lock", ttl=10)

        with pytest.raises(LockExtendError):
            lock.extend(20)

    def test_release_token_mismatch(self):
        """测试 token 不匹配时释放锁失败"""
        client = MagicMock()
        client.eval.return_value = 0  # Lua 脚本返回失败

        lock = RedisLock(client, "test_lock", ttl=10)
        lock._token = "test_token"

        # 模拟 token 不匹配
        client.get.return_value = "other_token"

        result = lock.release()
        assert result is False
        # token 不应被清除
        assert lock._token is not None

    def test_refresh_uses_extend_script(self):
        """测试 refresh 使用 EXTEND_LOCK_SCRIPT"""
        client = MagicMock()
        client.eval.return_value = 1

        lock = RedisLock(client, "test_lock", ttl=10)
        lock._token = "test_token_123"

        lock.refresh()

        # 验证调用的是 EXTEND_LOCK_SCRIPT
        client.eval.assert_called_once()
        call_args = client.eval.call_args
        assert call_args[0][0] == EXTEND_LOCK_SCRIPT
        assert call_args[0][4] == 10  # 使用初始 TTL


class TestReadWriteLockLuaScript:
    """测试读写锁 Lua 脚本的原子性"""

    def test_acquire_read_uses_lua_script(self):
        """测试获取读锁使用 Lua 脚本"""
        client = MagicMock()
        client.eval.return_value = 1  # 成功获取读锁

        lock = ReadWriteLock(client, "resource", ttl=10)
        result = lock.acquire_read(blocking=False)

        assert result is True
        client.eval.assert_called_once()
        call_args = client.eval.call_args
        assert call_args[0][0] == ACQUIRE_READ_LOCK_SCRIPT
        assert call_args[0][1] == 2  # KEYS 数量
        assert call_args[0][2] == "resource:write"  # KEYS[1]
        assert call_args[0][3] == "resource:readers"  # KEYS[2]

    def test_release_read_uses_lua_script(self):
        """测试释放读锁使用 Lua 脚本"""
        client = MagicMock()
        client.eval.return_value = 0  # 计数器归零

        lock = ReadWriteLock(client, "resource", ttl=10)
        lock.release_read()

        client.eval.assert_called_once()
        call_args = client.eval.call_args
        assert call_args[0][0] == RELEASE_READ_LOCK_SCRIPT
        assert call_args[0][1] == 1  # KEYS 数量
        assert call_args[0][2] == "resource:readers"  # KEYS[1]

    def test_acquire_write_uses_lua_script(self):
        """测试获取写锁使用 Lua 脚本"""
        client = MagicMock()
        client.get.return_value = False  # 没有读者
        client.eval.return_value = 1  # 成功获取写锁

        lock = ReadWriteLock(client, "resource", ttl=10)
        result = lock.acquire_write(blocking=False)

        assert result is True
        assert lock._write_token is not None
        client.eval.assert_called_once()
        call_args = client.eval.call_args
        assert call_args[0][0] == ACQUIRE_WRITE_LOCK_SCRIPT
        assert call_args[0][1] == 2  # KEYS 数量

    def test_release_write_uses_lua_script(self):
        """测试释放写锁使用 Lua 脚本"""
        client = MagicMock()
        client.eval.return_value = 1  # 成功释放

        lock = ReadWriteLock(client, "resource", ttl=10)
        lock._write_token = "test_token"

        result = lock.release_write()

        assert result is True
        assert lock._write_token is None
        client.eval.assert_called_once()
        call_args = client.eval.call_args
        assert call_args[0][0] == RELEASE_WRITE_LOCK_SCRIPT

    def test_release_write_without_token_warning(self):
        """测试无 token 时释放写锁记录警告"""
        client = MagicMock()
        client.delete.return_value = 1

        lock = ReadWriteLock(client, "resource", ttl=10)
        lock._write_token = None

        result = lock.release_write()

        assert result is True
        # 应该调用 delete 而不是 Lua 脚本
        client.delete.assert_called_once()


class TestRedisLockConcurrency:
    """测试 RedisLock 的并发场景"""

    def test_concurrent_acquire_release(self):
        """测试并发获取和释放锁"""
        client = MagicMock()
        client.set.return_value = True
        client.delete.return_value = 1
        client.get.side_effect = lambda k: f"token_{id(threading.current_thread())}"
        client.eval.return_value = 1

        lock = RedisLock(client, "test_lock", ttl=10)
        results = []
        errors = []

        def worker():
            try:
                if lock.acquire(blocking=False):
                    time.sleep(0.01)
                    lock.release()
                    results.append(True)
                else:
                    results.append(False)
            except Exception as e:
                errors.append(e)

        # 启动多个线程
        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 检查没有错误
        assert len(errors) == 0
        # 至少有一个成功
        assert any(results)

    def test_multiple_extend_calls(self):
        """测试多次延长锁"""
        client = MagicMock()
        client.set.return_value = True
        client.ttl.return_value = 30
        client.eval.return_value = 1

        lock = RedisLock(client, "test_lock", ttl=10)
        lock.acquire(blocking=False)

        # 多次延长
        lock.extend(10)
        lock.extend(10)
        lock.extend(10)

        # 验证每次都调用了 Lua 脚本
        assert client.eval.call_count == 3


class TestRedisLockEdgeCases:
    """测试 RedisLock 的边界情况"""

    def test_release_without_token_returns_false(self):
        """测试无 token 时释放锁返回 False"""
        client = MagicMock()

        lock = RedisLock(client, "test_lock", ttl=10)

        result = lock.release()
        assert result is False

    def test_is_locked_false_when_no_lock(self):
        """测试无锁时 is_locked 返回 False"""
        client = MagicMock()
        client.exists.return_value = 0

        lock = RedisLock(client, "test_lock", ttl=10)

        assert lock.is_locked() is False

    def test_is_owned_false_without_token(self):
        """测试无 token 时 is_owned 返回 False"""
        client = MagicMock()

        lock = RedisLock(client, "test_lock", ttl=10)

        assert lock.is_owned() is False

    def test_is_owned_true_with_correct_token(self):
        """测试 token 正确时 is_owned 返回 True"""
        client = MagicMock()
        client.set.return_value = True
        client.get.return_value = "test_token"

        lock = RedisLock(client, "test_lock", ttl=10)
        lock._token = "test_token"

        assert lock.is_owned() is True

    def test_multi_redis_lock_empty_keys(self):
        """测试空键列表"""
        client = MagicMock()

        lock = MultiRedisLock(client, [], ttl=10)

        result = lock.acquire()
        assert result == set()

    def test_multi_redis_lock_duplicate_keys(self):
        """测试重复键"""
        client = MagicMock()
        client.set.return_value = True
        client.get.return_value = "token"

        lock = MultiRedisLock(client, ["key1", "key1", "key2"], ttl=10)

        result = lock.acquire()
        # MultiRedisLock 不会对重复键去重，会返回所有键的结果
        assert len(result) == 3


class TestReadWriteLockEdgeCases:
    """测试 ReadWriteLock 的边界情况"""

    def test_multiple_read_lock_acquisitions(self):
        """测试多次获取读锁增加计数器"""
        client = MagicMock()
        client.eval.return_value = 1
        client.incr.side_effect = [1, 2, 3]

        lock = ReadWriteLock(client, "resource", ttl=10)

        lock.acquire_read()
        lock.acquire_read()
        lock.acquire_read()

        assert client.incr.call_count == 3

    def test_write_lock_with_existing_readers(self):
        """测试有读者时获取写锁失败"""
        client = MagicMock()
        client.get.return_value = "5"  # 有 5 个读者
        client.eval.return_value = 0  # 获取失败

        lock = ReadWriteLock(client, "resource", ttl=10)

        result = lock.acquire_write(blocking=False)
        assert result is False

    def test_read_lock_with_existing_write(self):
        """测试有写锁时获取读锁失败"""
        client = MagicMock()
        client.exists.return_value = 1  # 存在写锁
        client.eval.return_value = 0  # 获取失败

        lock = ReadWriteLock(client, "resource", ttl=10)

        result = lock.acquire_read(blocking=False)
        assert result is False

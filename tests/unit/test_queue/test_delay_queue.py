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
import time
import json
from unittest.mock import MagicMock, Mock
from redis_kit.queue.delay_queue import DelayQueue, DelayQueueWorker, DelayQueueManager
from redis_kit.exceptions import TaskNotFoundError, TaskAlreadyExistsError


class TestDelayQueue:
    """测试 DelayQueue 类"""

    def test_push_task(self):
        """测试推送任务"""
        client = MagicMock()
        client.zadd.return_value = 1
        client.hset.return_value = 1

        queue = DelayQueue(client, "test_queue")

        task_id = queue.push("task_data", delay=10)

        assert task_id is not None
        client.zadd.assert_called_once()
        client.hset.assert_called_once()

    def test_push_with_custom_task_id(self):
        """测试使用自定义任务ID推送"""
        client = MagicMock()
        client.zadd.return_value = 0  # 任务已存在

        queue = DelayQueue(client, "test_queue")

        with pytest.raises(TaskAlreadyExistsError):
            queue.push("task_data", delay=10, task_id="custom_id")

    def test_push_many_tasks(self):
        """测试批量推送任务"""
        client = MagicMock()
        client.zadd.return_value = 3

        queue = DelayQueue(client, "test_queue")

        tasks = [
            {"data": "task1", "delay": 5},
            {"data": "task2", "delay": 10},
            {"data": "task3", "delay": 15},
        ]

        task_ids = queue.push_many(tasks)

        assert len(task_ids) == 3
        client.zadd.assert_called()

    def test_pop_ready_tasks(self):
        """测试弹出就绪任务"""
        client = MagicMock()

        # 模拟有2个就绪任务
        now = time.time()
        task1_id = "task1"
        task2_id = "task2"

        client.zrangebyscore.return_value = [task1_id, task2_id]
        client.hmget.return_value = [
            json.dumps({"id": task1_id, "data": "data1"}),
            json.dumps({"id": task2_id, "data": "data2"}),
        ]
        client.zrem.return_value = 2
        client.hdel.return_value = 2

        queue = DelayQueue(client, "test_queue")

        tasks = queue.pop_ready(count=10)

        assert len(tasks) == 2
        assert tasks[0]["id"] == task1_id
        assert tasks[1]["id"] == task2_id

    def test_pop_ready_no_tasks(self):
        """测试弹出就绪任务（无任务）"""
        client = MagicMock()
        client.zrangebyscore.return_value = []

        queue = DelayQueue(client, "test_queue")

        tasks = queue.pop_ready(count=10)

        assert len(tasks) == 0

    def test_peek_ready_tasks(self):
        """测试查看就绪任务（不删除）"""
        client = MagicMock()

        task_id = "task1"
        client.zrangebyscore.return_value = [task_id]
        client.hmget.return_value = [json.dumps({"id": task_id, "data": "data1"})]

        queue = DelayQueue(client, "test_queue")

        tasks = queue.peek_ready(count=10)

        assert len(tasks) == 1
        assert tasks[0]["id"] == task_id

        # 不应该删除任务
        client.zrem.assert_not_called()
        client.hdel.assert_not_called()

    def test_cancel_task(self):
        """测试取消任务"""
        client = MagicMock()
        client.zrem.return_value = 1
        client.hdel.return_value = 1

        queue = DelayQueue(client, "test_queue")

        result = queue.cancel("task1")

        assert result is True
        client.zrem.assert_called_once()
        client.hdel.assert_called_once()

    def test_cancel_nonexistent_task(self):
        """测试取消不存在的任务"""
        client = MagicMock()
        client.zrem.return_value = 0

        queue = DelayQueue(client, "test_queue")

        with pytest.raises(TaskNotFoundError):
            queue.cancel("nonexistent_task")

    def test_cancel_many_tasks(self):
        """测试批量取消任务"""
        client = MagicMock()
        client.zrem.return_value = 3
        client.hdel.return_value = 3

        queue = DelayQueue(client, "test_queue")

        count = queue.cancel_many(["task1", "task2", "task3"])

        assert count == 3

    def test_reschedule_task(self):
        """测试重新调度任务"""
        client = MagicMock()
        client.zadd.return_value = 0  # XX 模式，更新分数

        queue = DelayQueue(client, "test_queue")

        result = queue.reschedule("task1", new_delay=20)

        assert result is True
        client.zadd.assert_called()

    def test_get_pending_count(self):
        """测试获取待处理任务数"""
        client = MagicMock()
        client.zcard.return_value = 100

        queue = DelayQueue(client, "test_queue")

        count = queue.get_pending_count()

        assert count == 100

    def test_get_ready_count(self):
        """测试获取就绪任务数"""
        client = MagicMock()
        client.zcount.return_value = 10

        queue = DelayQueue(client, "test_queue")

        count = queue.get_ready_count()

        assert count == 10


class TestDelayQueueWorker:
    """测试 DelayQueueWorker 类"""

    def test_worker_basic_operation(self):
        """测试 Worker 基本操作"""
        client = MagicMock()
        client.zrangebyscore.return_value = []

        queue = DelayQueue(client, "test_queue")
        handler = Mock()

        worker = DelayQueueWorker(queue, handler, interval=0.1)

        # 运行一小段时间后停止
        worker.start()
        time.sleep(0.3)
        worker.stop()

        assert worker._stopped is True

    def test_worker_processes_tasks(self):
        """测试 Worker 处理任务"""
        client = MagicMock()

        task_id = "task1"
        task_data = {"id": task_id, "data": "test_data"}

        # 第一次调用返回任务，后续返回空
        client.zrangebyscore.side_effect = [[task_id], []]
        client.hmget.return_value = [json.dumps(task_data)]
        client.zrem.return_value = 1
        client.hdel.return_value = 1

        queue = DelayQueue(client, "test_queue")
        handler = Mock()

        worker = DelayQueueWorker(queue, handler, interval=0.1)

        worker.start()
        time.sleep(0.3)
        worker.stop()

        # 确保处理器被调用
        handler.assert_called()

    def test_worker_error_handling(self):
        """测试 Worker 错误处理"""
        client = MagicMock()

        task_id = "task1"
        task_data = {"id": task_id, "data": "test_data"}

        client.zrangebyscore.side_effect = [[task_id], []]
        client.hmget.return_value = [json.dumps(task_data)]
        client.zrem.return_value = 1
        client.hdel.return_value = 1

        queue = DelayQueue(client, "test_queue")

        # 处理器抛出异常
        handler = Mock(side_effect=ValueError("processing error"))
        error_handler = Mock()

        worker = DelayQueueWorker(
            queue, handler, interval=0.1, error_handler=error_handler
        )

        worker.start()
        time.sleep(0.3)
        worker.stop()

        # 错误处理器应该被调用
        error_handler.assert_called()


class TestDelayQueueManager:
    """测试 DelayQueueManager 类"""

    def test_manager_multiple_queues(self):
        """测试管理器管理多个队列"""
        client = MagicMock()
        client.zrangebyscore.return_value = []

        handler1 = Mock()
        handler2 = Mock()

        manager = DelayQueueManager(client)
        manager.register_queue("queue1", handler1, interval=0.1)
        manager.register_queue("queue2", handler2, interval=0.1)

        manager.start_all()
        time.sleep(0.3)
        manager.stop_all()

        # 两个队列的 worker 都应该被启动
        assert len(manager._workers) == 2

    def test_manager_stop_specific_queue(self):
        """测试停止特定队列"""
        client = MagicMock()
        client.zrangebyscore.return_value = []

        handler = Mock()

        manager = DelayQueueManager(client)
        manager.register_queue("queue1", handler, interval=0.1)
        manager.register_queue("queue2", handler, interval=0.1)

        manager.start_all()
        time.sleep(0.2)

        manager.stop_queue("queue1")

        # queue1 应该被停止
        assert manager._workers["queue1"]._stopped is True
        # queue2 仍在运行
        assert manager._workers["queue2"]._stopped is False

        manager.stop_all()

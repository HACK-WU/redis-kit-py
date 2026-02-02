"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit 延迟队列模块

基于 Redis Sorted Set 实现的延迟任务队列
"""

import json
import logging
import threading
import time
from typing import Any
from collections.abc import Callable

from redis_kit.exceptions import TaskAlreadyExistsError

logger = logging.getLogger(__name__)


class DelayQueue:
    """
    延迟队列

    基于 Redis Sorted Set 实现的延迟任务队列。
    使用两个数据结构：
    - Sorted Set：存储任务 ID 和执行时间（score）
    - Hash：存储任务详细数据

    Attributes:
        client: Redis 客户端
        queue_name: 队列名称（Sorted Set 键名）
        storage_name: 存储名称（Hash 键名）

    Example:
        >>> queue = DelayQueue(client, "my_queue")
        >>> queue.push("task_1", {"data": "value"}, delay_seconds=60)
        >>> tasks = queue.pop_ready()
        >>> for task_id, task_data in tasks:
        ...     process_task(task_id, task_data)
    """

    def __init__(
        self,
        client: Any,
        queue_name: str = "delay_queue",
        storage_name: str = None,
        ttl: int = None,
    ):
        """
        初始化延迟队列

        Args:
            client: Redis 客户端实例
            queue_name: 队列名称
            storage_name: 存储名称，默认为 {queue_name}:storage
            ttl: 数据过期时间（秒），None 表示不过期
        """
        self.client = client
        self.queue_name = queue_name
        self.storage_name = storage_name or f"{queue_name}:storage"
        self.ttl = ttl

    def push(
        self,
        task_id: str,
        task_data: Any,
        delay_seconds: float = 0,
        execute_at: float = None,
        overwrite: bool = False,
    ) -> bool:
        """
        推送延迟任务

        Args:
            task_id: 任务 ID（唯一标识）
            task_data: 任务数据（将被 JSON 序列化）
            delay_seconds: 延迟秒数
            execute_at: 执行时间戳，优先于 delay_seconds
            overwrite: 是否覆盖已存在的任务

        Returns:
            是否成功推送

        Raises:
            TaskAlreadyExistsError: 任务已存在且 overwrite=False
        """
        # 检查任务是否已存在
        if not overwrite and self.exists(task_id):
            raise TaskAlreadyExistsError(task_id)

        # 计算执行时间
        if execute_at is None:
            execute_at = time.time() + delay_seconds

        # 序列化数据
        serialized_data = json.dumps(task_data)

        # 使用 Pipeline 原子写入
        pipeline = self.client.pipeline()
        pipeline.hset(self.storage_name, task_id, serialized_data)
        pipeline.zadd(self.queue_name, {task_id: execute_at})

        if self.ttl:
            pipeline.expire(self.storage_name, self.ttl)
            pipeline.expire(self.queue_name, self.ttl)

        results = pipeline.execute()
        return all(r is not None for r in results[:2])

    def push_many(self, tasks: list[tuple[str, Any, float]]) -> int:
        """
        批量推送任务

        Args:
            tasks: 任务列表，每项为 (task_id, task_data, delay_seconds)

        Returns:
            成功推送的任务数量
        """
        if not tasks:
            return 0

        now = time.time()
        pipeline = self.client.pipeline()

        for task_id, task_data, delay_seconds in tasks:
            execute_at = now + delay_seconds
            serialized_data = json.dumps(task_data)
            pipeline.hset(self.storage_name, task_id, serialized_data)
            pipeline.zadd(self.queue_name, {task_id: execute_at})

        if self.ttl:
            pipeline.expire(self.storage_name, self.ttl)
            pipeline.expire(self.queue_name, self.ttl)

        results = pipeline.execute()

        # 计算成功数量（每个任务对应 2 个操作）
        task_count = len(tasks)
        success_count = 0
        for i in range(task_count):
            if results[i * 2] is not None and results[i * 2 + 1] is not None:
                success_count += 1

        return success_count

    def pop_ready(self, batch_size: int = 100) -> list[tuple[str, Any]]:
        """
        获取并移除已到期的任务

        使用原子操作防止并发重复处理。

        Args:
            batch_size: 单次获取的最大任务数

        Returns:
            任务列表，每项为 (task_id, task_data)
        """
        now = time.time()

        # 获取到期的任务 ID
        task_ids = self.client.zrangebyscore(
            self.queue_name, 0, now, start=0, num=batch_size
        )

        if not task_ids:
            return []

        # 原子性移除（使用 ZREM 返回值判断是否成功）
        pipeline = self.client.pipeline()
        for task_id in task_ids:
            pipeline.zrem(self.queue_name, task_id)
        results = pipeline.execute()

        # 只处理成功移除的任务
        removed_ids = [
            task_id for task_id, removed in zip(task_ids, results) if removed
        ]

        if not removed_ids:
            return []

        # 获取任务数据
        task_data_list = self.client.hmget(self.storage_name, removed_ids)

        # 删除任务数据
        self.client.hdel(self.storage_name, *removed_ids)

        # 组装结果
        tasks = []
        for task_id, task_data in zip(removed_ids, task_data_list):
            if task_data:
                try:
                    data = json.loads(task_data)
                    tasks.append((task_id, data))
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode task data for {task_id}: {e}")

        return tasks

    def peek_ready(self, batch_size: int = 100) -> list[tuple[str, Any]]:
        """
        查看已到期的任务（不移除）

        Args:
            batch_size: 单次获取的最大任务数

        Returns:
            任务列表，每项为 (task_id, task_data)
        """
        now = time.time()
        task_ids = self.client.zrangebyscore(
            self.queue_name, 0, now, start=0, num=batch_size
        )

        if not task_ids:
            return []

        task_data_list = self.client.hmget(self.storage_name, task_ids)

        tasks = []
        for task_id, task_data in zip(task_ids, task_data_list):
            if task_data:
                try:
                    data = json.loads(task_data)
                    tasks.append((task_id, data))
                except json.JSONDecodeError:
                    pass

        return tasks

    def get(self, task_id: str) -> tuple[Any, float] | None:
        """
        获取任务详情

        Args:
            task_id: 任务 ID

        Returns:
            (task_data, execute_at) 元组，不存在返回 None
        """
        pipeline = self.client.pipeline()
        pipeline.hget(self.storage_name, task_id)
        pipeline.zscore(self.queue_name, task_id)
        task_data, score = pipeline.execute()

        if task_data is None:
            return None

        try:
            data = json.loads(task_data)
            return (data, score)
        except json.JSONDecodeError:
            return None

    def cancel(self, task_id: str) -> bool:
        """
        取消延迟任务

        Args:
            task_id: 任务 ID

        Returns:
            是否成功取消
        """
        pipeline = self.client.pipeline()
        pipeline.zrem(self.queue_name, task_id)
        pipeline.hdel(self.storage_name, task_id)
        results = pipeline.execute()
        return any(results)

    def cancel_many(self, task_ids: list[str]) -> int:
        """
        批量取消任务

        Args:
            task_ids: 任务 ID 列表

        Returns:
            成功取消的任务数量
        """
        if not task_ids:
            return 0

        removed = self.client.zrem(self.queue_name, *task_ids)
        self.client.hdel(self.storage_name, *task_ids)
        return removed

    def reschedule(
        self, task_id: str, delay_seconds: float = 0, execute_at: float = None
    ) -> bool:
        """
        重新调度任务

        Args:
            task_id: 任务 ID
            delay_seconds: 新的延迟秒数
            execute_at: 新的执行时间戳

        Returns:
            是否成功重新调度
        """
        if execute_at is None:
            execute_at = time.time() + delay_seconds

        # 检查任务是否存在
        if not self.exists(task_id):
            return False

        return bool(self.client.zadd(self.queue_name, {task_id: execute_at}))

    def exists(self, task_id: str) -> bool:
        """
        检查任务是否存在

        Args:
            task_id: 任务 ID

        Returns:
            任务是否存在
        """
        return self.client.hexists(self.storage_name, task_id)

    def get_pending_count(self) -> int:
        """
        获取待处理任务总数

        Returns:
            待处理任务数量
        """
        return self.client.zcard(self.queue_name)

    def get_ready_count(self) -> int:
        """
        获取已到期任务数量

        Returns:
            已到期任务数量
        """
        now = time.time()
        return self.client.zcount(self.queue_name, 0, now)

    def clear(self) -> int:
        """
        清空队列

        Returns:
            清除的任务数量
        """
        count = self.client.zcard(self.queue_name)
        pipeline = self.client.pipeline()
        pipeline.delete(self.queue_name)
        pipeline.delete(self.storage_name)
        pipeline.execute()
        return count


class DelayQueueWorker:
    """
    延迟队列工作器

    持续轮询延迟队列并处理到期任务。

    Attributes:
        queue: 延迟队列实例
        handler: 任务处理函数
        poll_interval: 轮询间隔（秒）
        batch_size: 单次处理的最大任务数

    Example:
        >>> def handle_task(task_id, task_data):
        ...     print(f"Processing {task_id}: {task_data}")

        >>> worker = DelayQueueWorker(queue, handle_task)
        >>> worker.start()  # 在后台线程中运行
        >>> # ... 业务代码 ...
        >>> worker.stop()
    """

    def __init__(
        self,
        queue: DelayQueue,
        handler: Callable[[str, Any], None],
        poll_interval: float = 1.0,
        batch_size: int = 100,
        error_handler: Callable[[str, Any, Exception], None] = None,
    ):
        """
        初始化工作器

        Args:
            queue: 延迟队列实例
            handler: 任务处理函数，接收 (task_id, task_data)
            poll_interval: 轮询间隔（秒）
            batch_size: 单次处理的最大任务数
            error_handler: 错误处理函数，接收 (task_id, task_data, exception)
        """
        self.queue = queue
        self.handler = handler
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.error_handler = error_handler or self._default_error_handler

        self._running = False
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def _default_error_handler(
        self, task_id: str, task_data: Any, error: Exception
    ) -> None:
        """默认错误处理"""
        logger.exception(f"Failed to handle task {task_id}: {error}")

    def _run_loop(self) -> None:
        """工作循环"""
        while not self._stop_event.is_set():
            try:
                tasks = self.queue.pop_ready(self.batch_size)
                for task_id, task_data in tasks:
                    try:
                        self.handler(task_id, task_data)
                    except Exception as e:
                        self.error_handler(task_id, task_data, e)
            except Exception as e:
                logger.exception(f"Failed to pop tasks: {e}")

            self._stop_event.wait(self.poll_interval)

    def start(self, daemon: bool = True) -> None:
        """
        启动工作器

        Args:
            daemon: 是否作为守护线程运行
        """
        if self._running:
            logger.warning("Worker is already running")
            return

        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=daemon)
        self._thread.start()
        logger.info(f"DelayQueueWorker started for queue: {self.queue.queue_name}")

    def stop(self, timeout: float = None) -> None:
        """
        停止工作器

        Args:
            timeout: 等待停止的超时时间（秒）
        """
        if not self._running:
            return

        self._running = False
        self._stop_event.set()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout)

        logger.info(f"DelayQueueWorker stopped for queue: {self.queue.queue_name}")

    def run_once(self) -> int:
        """
        执行一次处理循环

        Returns:
            处理的任务数量
        """
        tasks = self.queue.pop_ready(self.batch_size)
        count = 0
        for task_id, task_data in tasks:
            try:
                self.handler(task_id, task_data)
                count += 1
            except Exception as e:
                self.error_handler(task_id, task_data, e)
        return count

    @property
    def is_running(self) -> bool:
        """工作器是否正在运行"""
        return self._running


class DelayQueueManager:
    """
    延迟队列管理器

    管理多个延迟队列，提供统一的刷新接口。
    兼容原有 bk-monitor 的 DelayQueueManager 接口。

    Example:
        >>> manager = DelayQueueManager()
        >>> manager.add_queue(client, "queue1")
        >>> manager.add_queue(client, "queue2")
        >>> manager.refresh()  # 刷新所有队列
    """

    def __init__(self):
        """初始化队列管理器"""
        self._queues: dict[str, DelayQueue] = {}
        self._workers: dict[str, DelayQueueWorker] = {}

    def add_queue(
        self,
        client: Any,
        queue_name: str,
        storage_name: str = None,
        handler: Callable[[str, Any], None] = None,
    ) -> DelayQueue:
        """
        添加队列

        Args:
            client: Redis 客户端
            queue_name: 队列名称
            storage_name: 存储名称
            handler: 任务处理函数

        Returns:
            创建的队列实例
        """
        queue = DelayQueue(client, queue_name, storage_name)
        self._queues[queue_name] = queue

        if handler:
            worker = DelayQueueWorker(queue, handler)
            self._workers[queue_name] = worker

        return queue

    def get_queue(self, queue_name: str) -> DelayQueue | None:
        """获取队列"""
        return self._queues.get(queue_name)

    def remove_queue(self, queue_name: str) -> bool:
        """移除队列"""
        if queue_name in self._workers:
            self._workers[queue_name].stop()
            del self._workers[queue_name]

        if queue_name in self._queues:
            del self._queues[queue_name]
            return True

        return False

    def refresh(self, run_seconds: int = 60) -> None:
        """
        刷新所有队列（处理到期任务）

        由定时任务调度，持续运行指定时间。

        Args:
            run_seconds: 运行时间（秒）
        """
        start_time = time.time()
        while time.time() - start_time < run_seconds:
            for queue_name, worker in self._workers.items():
                try:
                    worker.run_once()
                except Exception as e:
                    logger.exception(f"Error refreshing queue {queue_name}: {e}")
            time.sleep(1)

    def start_all(self, daemon: bool = True) -> None:
        """启动所有工作器"""
        for worker in self._workers.values():
            worker.start(daemon)

    def stop_all(self, timeout: float = None) -> None:
        """停止所有工作器"""
        for worker in self._workers.values():
            worker.stop(timeout)

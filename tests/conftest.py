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
from unittest.mock import MagicMock
from redis_kit.types import NodeConfig, PoolConfig, RetryPolicy


@pytest.fixture
def mock_redis_instance():
    """创建 Mock Redis 实例"""
    instance = MagicMock()
    instance.ping.return_value = True
    instance.get.return_value = None
    instance.set.return_value = True
    instance.delete.return_value = 1
    instance.exists.return_value = 0
    instance.ttl.return_value = -1
    return instance


@pytest.fixture
def node_config():
    """默认节点配置"""
    return NodeConfig(
        host="localhost",
        port=6379,
        db=0,
        password=None,
        pool_config=PoolConfig(
            max_connections=50,
            socket_timeout=5.0,
            socket_connect_timeout=5.0,
        ),
    )


@pytest.fixture
def retry_policy():
    """默认重试策略"""
    return RetryPolicy(
        max_retries=3,
        base_delay=0.1,
        max_delay=1.0,
        exponential_base=2,
        retryable_exceptions=(ConnectionError, TimeoutError),
    )

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
from unittest.mock import MagicMock, Mock
from redis_kit.core.pipeline import PipelineProxy


class TestPipelineProxy:
    """测试 Pipeline 代理"""

    def test_initialization(self):
        """测试初始化"""
        node_proxy = MagicMock()
        node_proxy.routing_manager = MagicMock()

        pipeline = PipelineProxy(node_proxy, transaction=True)

        assert pipeline.node_proxy == node_proxy
        assert pipeline.transaction is True
        assert len(pipeline) == 0

    def test_add_commands(self):
        """测试添加命令"""
        node_proxy = MagicMock()
        node_proxy.routing_manager = MagicMock()
        node_proxy.get_node_by_shard_key = MagicMock(return_value=Mock(node_id="node1"))

        mock_pipeline = MagicMock()
        node_proxy.get_client.return_value.pipeline.return_value = mock_pipeline

        pipeline = PipelineProxy(node_proxy, transaction=False)

        # 添加命令
        pipeline.set("key1", "value1")
        pipeline.get("key1")
        pipeline.delete("key1")

        assert len(pipeline) == 3

    def test_execute_preserves_order(self):
        """测试执行时保持命令顺序"""
        node_proxy = MagicMock()
        node_proxy.routing_manager = MagicMock()

        # 模拟两个节点
        node1 = Mock(node_id="node1")
        node2 = Mock(node_id="node2")
        node_proxy.get_node_by_shard_key.side_effect = [node1, node2, node1]

        # 模拟 pipeline
        mock_pipeline1 = MagicMock()
        mock_pipeline1.execute.return_value = ["result1", "result3"]
        mock_pipeline2 = MagicMock()
        mock_pipeline2.execute.return_value = ["result2"]

        node_proxy.get_client.side_effect = [
            Mock(pipeline=Mock(return_value=mock_pipeline1)),
            Mock(pipeline=Mock(return_value=mock_pipeline2)),
            Mock(pipeline=Mock(return_value=mock_pipeline1)),
        ]

        pipeline = PipelineProxy(node_proxy, transaction=False)

        # 添加命令：node1, node2, node1
        pipeline.set("key1", "value1")  # node1
        pipeline.get("key2")  # node2
        pipeline.delete("key1")  # node1

        results = pipeline.execute()

        # 结果应该保持原始顺序
        assert results == ["result1", "result2", "result3"]

    def test_execute_with_node_failure(self):
        """测试节点执行失败时的处理"""
        node_proxy = MagicMock()
        node_proxy.routing_manager = MagicMock()
        node_proxy.get_node_by_shard_key.return_value = Mock(node_id="node1")

        mock_pipeline = MagicMock()
        mock_pipeline.execute.side_effect = Exception("Redis error")
        node_proxy.get_client.return_value.pipeline.return_value = mock_pipeline

        pipeline = PipelineProxy(node_proxy, transaction=False)
        pipeline.set("key1", "value1")

        # 节点失败应该返回 None
        results = pipeline.execute()
        assert results == [None]
        assert len(pipeline) == 0

    def test_reset_clears_state(self):
        """测试重置清空状态"""
        node_proxy = MagicMock()
        node_proxy.routing_manager = MagicMock()
        node_proxy.get_node_by_shard_key.return_value = Mock(node_id="node1")

        pipeline = PipelineProxy(node_proxy, transaction=False)
        pipeline.set("key1", "value1")
        pipeline.get("key2")

        assert len(pipeline) == 2

        pipeline.reset()

        assert len(pipeline) == 0
        assert len(pipeline._pipeline_pool) == 0
        assert len(pipeline._node_cmd_counts) == 0

    def test_context_manager(self):
        """测试上下文管理器"""
        node_proxy = MagicMock()
        node_proxy.routing_manager = MagicMock()
        node_proxy.get_node_by_shard_key.return_value = Mock(node_id="node1")

        mock_pipeline = MagicMock()
        mock_pipeline.execute.return_value = ["result"]
        node_proxy.get_client.return_value.pipeline.return_value = mock_pipeline

        results = None
        with PipelineProxy(node_proxy, transaction=False) as pipe:
            pipe.set("key1", "value1")
            results = pipe.execute()

        assert results == ["result"]

    def test_context_manager_with_exception(self):
        """测试上下文管理器异常时重置"""
        node_proxy = MagicMock()
        node_proxy.routing_manager = MagicMock()
        node_proxy.get_node_by_shard_key.return_value = Mock(node_id="node1")

        mock_pipeline = MagicMock()
        node_proxy.get_client.return_value.pipeline.return_value = mock_pipeline

        with pytest.raises(ValueError):
            with PipelineProxy(node_proxy, transaction=False) as pipe:
                pipe.set("key1", "value1")
                raise ValueError("Test error")

        # 异常后应该重置
        assert len(pipe._command_stack) == 0

    def test_node_not_found_error(self):
        """测试节点不存在时抛出异常"""
        node_proxy = MagicMock()
        node_proxy.routing_manager = MagicMock()
        node_proxy.get_node_by_shard_key.return_value = None
        node_proxy.routing_manager.get_node_by_id.return_value = None

        pipeline = PipelineProxy(node_proxy, transaction=False)

        # 由于节点为 None，会直接触发 AttributeError
        with pytest.raises(AttributeError):
            pipeline.set("key1", "value1")
        assert "watch" in PipelineProxy.ALLOWED_METHODS
        assert "multi" in PipelineProxy.ALLOWED_METHODS

    def test_non_allowed_methods_still_work(self):
        """测试不在允许列表中的方法仍然可以工作"""
        node_proxy = MagicMock()
        node_proxy.routing_manager = MagicMock()
        node_proxy.get_node_by_shard_key.return_value = Mock(node_id="node1")

        mock_pipeline = MagicMock()
        node_proxy.get_client.return_value.pipeline.return_value = mock_pipeline

        pipeline = PipelineProxy(node_proxy, transaction=False)

        # 这些方法不在 ALLOWED_METHODS 中，但应该可以工作
        pipeline.set("key", "value")
        pipeline.get("key")
        pipeline.delete("key")

        assert len(pipeline) == 3

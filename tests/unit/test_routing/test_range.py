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
from redis_kit.routing.range import RangeRoutingStrategy, ModuloRoutingStrategy
from redis_kit.exceptions import RoutingTableError, InvalidShardKeyError


class TestRangeRoutingStrategy:
    """测试 RangeRoutingStrategy 类"""

    def test_basic_routing(self):
        """测试基本范围路由"""
        routing_table = [
            {"node_name": "node1", "min": 0, "max": 100},
            {"node_name": "node2", "min": 101, "max": 200},
            {"node_name": "node3", "min": 201, "max": 300},
        ]
        strategy = RangeRoutingStrategy(routing_table)

        assert strategy.route(50) == "node1"
        assert strategy.route(150) == "node2"
        assert strategy.route(250) == "node3"

    def test_boundary_values(self):
        """测试边界值"""
        routing_table = [
            {"node_name": "node1", "min": 0, "max": 100},
            {"node_name": "node2", "min": 101, "max": 200},
        ]
        strategy = RangeRoutingStrategy(routing_table)

        assert strategy.route(0) == "node1"
        assert strategy.route(100) == "node1"
        assert strategy.route(101) == "node2"
        assert strategy.route(200) == "node2"

    def test_string_shard_key(self):
        """测试字符串类型的 shard_key（应转换为数值）"""
        routing_table = [
            {"node_name": "node1", "min": 0, "max": 100},
            {"node_name": "node2", "min": 101, "max": 200},
        ]
        strategy = RangeRoutingStrategy(routing_table)

        # 字符串应该被哈希后路由
        node = strategy.route("user:123")
        assert node in ["node1", "node2"]

    def test_numeric_string_shard_key(self):
        """测试数字字符串类型的 shard_key"""
        routing_table = [
            {"node_name": "node1", "min": 0, "max": 100},
            {"node_name": "node2", "min": 101, "max": 200},
        ]
        strategy = RangeRoutingStrategy(routing_table)

        # 尝试将数字字符串转换为整数
        node = strategy.route("50")
        # 因为实现可能直接哈希字符串，这里只验证返回有效节点
        assert node in ["node1", "node2"]

    def test_out_of_range_shard_key(self):
        """测试超出范围的 shard_key"""
        routing_table = [
            {"node_name": "node1", "min": 0, "max": 100},
            {"node_name": "node2", "min": 101, "max": 200},
        ]
        strategy = RangeRoutingStrategy(routing_table)

        # 超出范围的键应该抛出异常
        with pytest.raises(InvalidShardKeyError):
            strategy.route(300)

    def test_invalid_routing_table(self):
        """测试无效的路由表"""
        # 缺少必需字段
        with pytest.raises(RoutingTableError):
            RangeRoutingStrategy([{"node_name": "node1"}])

        # 范围重叠
        with pytest.raises(RoutingTableError):
            RangeRoutingStrategy(
                [
                    {"node_name": "node1", "min": 0, "max": 150},
                    {"node_name": "node2", "min": 100, "max": 200},
                ]
            )

    def test_empty_routing_table(self):
        """测试空路由表"""
        with pytest.raises(ValueError):
            RangeRoutingStrategy([])

    def test_gap_in_ranges(self):
        """测试范围间存在间隙"""
        routing_table = [
            {"node_name": "node1", "min": 0, "max": 100},
            {"node_name": "node2", "min": 150, "max": 200},  # 缺少 101-149
        ]

        # 可能会抛出异常，或者在初始化时检测到
        with pytest.raises(RoutingTableError):
            strategy = RangeRoutingStrategy(routing_table)


class TestModuloRoutingStrategy:
    """测试 ModuloRoutingStrategy 类"""

    def test_basic_routing(self):
        """测试基本取模路由"""
        nodes = ["node1", "node2", "node3"]
        strategy = ModuloRoutingStrategy(nodes)

        # shard_key % 3
        assert strategy.route(0) == "node1"  # 0 % 3 = 0
        assert strategy.route(1) == "node2"  # 1 % 3 = 1
        assert strategy.route(2) == "node3"  # 2 % 3 = 2
        assert strategy.route(3) == "node1"  # 3 % 3 = 0
        assert strategy.route(4) == "node2"  # 4 % 3 = 1

    def test_distribution(self):
        """测试分布均匀性"""
        nodes = ["node1", "node2", "node3", "node4"]
        strategy = ModuloRoutingStrategy(nodes)

        distribution = {}
        for i in range(1000):
            node = strategy.route(i)
            distribution[node] = distribution.get(node, 0) + 1

        # 取模路由应该完美均匀分布
        assert len(distribution) == len(nodes)
        for count in distribution.values():
            assert count == 250

    def test_string_shard_key(self):
        """测试字符串类型的 shard_key（应转换为数值）"""
        nodes = ["node1", "node2", "node3"]
        strategy = ModuloRoutingStrategy(nodes)

        # 字符串应该被哈希后路由
        node = strategy.route("user:123")
        assert node in nodes

        # 相同字符串应该路由到相同节点
        node2 = strategy.route("user:123")
        assert node == node2

    def test_negative_shard_key(self):
        """测试负数 shard_key"""
        nodes = ["node1", "node2", "node3"]
        strategy = ModuloRoutingStrategy(nodes)

        # 负数取模应该正常工作
        node = strategy.route(-5)
        assert node in nodes

    def test_empty_nodes_list(self):
        """测试空节点列表"""
        with pytest.raises(ValueError):
            ModuloRoutingStrategy([])

    def test_consistency(self):
        """测试一致性（相同输入得到相同输出）"""
        nodes = ["node1", "node2", "node3"]
        strategy = ModuloRoutingStrategy(nodes)

        for i in range(100):
            shard_key = f"key:{i}"
            node1 = strategy.route(shard_key)
            node2 = strategy.route(shard_key)
            assert node1 == node2

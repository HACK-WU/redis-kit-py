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
from redis_kit.routing.hash import HashRoutingStrategy, ConsistentHashRoutingStrategy


class TestHashRoutingStrategy:
    """测试 HashRoutingStrategy 类"""

    def test_basic_routing(self):
        """测试基本路由"""
        nodes = ["node1", "node2", "node3"]
        strategy = HashRoutingStrategy(nodes)

        # 相同的 shard_key 应该路由到相同的节点
        node1 = strategy.route("user:123")
        node2 = strategy.route("user:123")
        assert node1 == node2

    def test_distribution(self):
        """测试分布均匀性"""
        nodes = ["node1", "node2", "node3", "node4"]
        strategy = HashRoutingStrategy(nodes)

        distribution = {}
        for i in range(1000):
            node = strategy.route(f"key:{i}")
            distribution[node] = distribution.get(node, 0) + 1

        # 每个节点至少应该有一些键
        assert len(distribution) == len(nodes)

        # 分布应该相对均匀（每个节点分配100-400个键）
        for count in distribution.values():
            assert 100 < count < 400

    def test_string_shard_key(self):
        """测试字符串类型的 shard_key"""
        nodes = ["node1", "node2", "node3"]
        strategy = HashRoutingStrategy(nodes)

        node = strategy.route("abc123")
        assert node in nodes

    def test_numeric_shard_key(self):
        """测试数值类型的 shard_key"""
        nodes = ["node1", "node2"]
        strategy = HashRoutingStrategy(nodes)

        node = strategy.route(12345)
        assert node in nodes

    def test_empty_nodes_list(self):
        """测试空节点列表"""
        with pytest.raises(ValueError):
            HashRoutingStrategy([])

    def test_node_addition(self):
        """测试添加节点后的影响"""
        nodes_before = ["node1", "node2"]
        strategy_before = HashRoutingStrategy(nodes_before)

        nodes_after = ["node1", "node2", "node3"]
        strategy_after = HashRoutingStrategy(nodes_after)

        # 添加节点后，部分键的路由会发生变化
        changed = 0
        total = 100
        for i in range(total):
            shard_key = f"key:{i}"
            if strategy_before.route(shard_key) != strategy_after.route(shard_key):
                changed += 1

        # 应该有一定比例的键发生了迁移
        assert changed > 0


class TestConsistentHashRoutingStrategy:
    """测试 ConsistentHashRoutingStrategy 类"""

    def test_basic_routing(self):
        """测试基本路由"""
        nodes = ["node1", "node2", "node3"]
        strategy = ConsistentHashRoutingStrategy(nodes)

        # 相同的 shard_key 应该路由到相同的节点
        node1 = strategy.route("user:123")
        node2 = strategy.route("user:123")
        assert node1 == node2

    def test_virtual_nodes(self):
        """测试虚拟节点设置"""
        nodes = ["node1", "node2"]
        strategy = ConsistentHashRoutingStrategy(nodes, virtual_nodes=100)

        node = strategy.route("test_key")
        assert node in nodes

    def test_distribution(self):
        """测试分布均匀性"""
        nodes = ["node1", "node2", "node3", "node4"]
        strategy = ConsistentHashRoutingStrategy(nodes, virtual_nodes=150)

        distribution = {}
        for i in range(1000):
            node = strategy.route(f"key:{i}")
            distribution[node] = distribution.get(node, 0) + 1

        # 每个节点至少应该有一些键
        assert len(distribution) == len(nodes)

        # 一致性哈希的分布应该相对均匀
        for count in distribution.values():
            assert 150 < count < 350

    def test_node_removal_stability(self):
        """测试删除节点时的稳定性"""
        nodes_before = ["node1", "node2", "node3"]
        strategy_before = ConsistentHashRoutingStrategy(nodes_before, virtual_nodes=100)

        # 删除一个节点
        nodes_after = ["node1", "node2"]
        strategy_after = ConsistentHashRoutingStrategy(nodes_after, virtual_nodes=100)

        # 大部分键应该保持在原节点（除了 node3 的键需要迁移）
        changed = 0
        total = 300
        for i in range(total):
            shard_key = f"key:{i}"
            node_before = strategy_before.route(shard_key)
            node_after = strategy_after.route(shard_key)

            # 只有原本在 node3 的键需要迁移
            if node_before == "node3":
                assert node_after in ["node1", "node2"]
            else:
                # node1 和 node2 的键应该大部分保持不变
                if node_before != node_after:
                    changed += 1

        # 变更率应该较低（一致性哈希的优势）
        change_rate = changed / total
        assert change_rate < 0.15  # 变更率应小于15%

    def test_node_addition_stability(self):
        """测试添加节点时的稳定性"""
        nodes_before = ["node1", "node2"]
        strategy_before = ConsistentHashRoutingStrategy(nodes_before, virtual_nodes=100)

        # 添加一个节点
        nodes_after = ["node1", "node2", "node3"]
        strategy_after = ConsistentHashRoutingStrategy(nodes_after, virtual_nodes=100)

        # 统计变化的键
        changed = 0
        total = 300
        for i in range(total):
            shard_key = f"key:{i}"
            node_before = strategy_before.route(shard_key)
            node_after = strategy_after.route(shard_key)

            if node_before != node_after:
                changed += 1
                # 变化的键应该迁移到新节点
                assert node_after == "node3"

        # 变更率应该接近 1/3（理论值）
        change_rate = changed / total
        assert 0.20 < change_rate < 0.45  # 允许一定的偏差

    def test_empty_nodes_list(self):
        """测试空节点列表"""
        with pytest.raises(ValueError):
            ConsistentHashRoutingStrategy([])

    def test_custom_hash_function(self):
        """测试自定义哈希函数"""
        nodes = ["node1", "node2"]

        def custom_hash(key):
            # 简单的自定义哈希函数
            return sum(ord(c) for c in str(key))

        strategy = ConsistentHashRoutingStrategy(nodes, hash_func=custom_hash)
        node = strategy.route("test")
        assert node in nodes

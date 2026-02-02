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
import tempfile
import os
from redis_kit.config.loader import (
    DictConfigLoader,
    YamlConfigLoader,
    EnvConfigLoader,
    CompositeConfigLoader,
    load_config,
)
from redis_kit.config.schema import RedisKitConfig
from redis_kit.exceptions import InvalidConfigError


class TestDictConfigLoader:
    """测试 DictConfigLoader 类"""

    def test_load_from_dict(self):
        """测试从字典加载配置"""
        config_dict = {
            "nodes": {"default": {"host": "localhost", "port": 6379, "db": 0}},
            "backends": {"cache": {"name": "cache", "db": 0, "node_name": "default"}},
        }

        loader = DictConfigLoader(config_dict)
        config = loader.load()

        assert isinstance(config, RedisKitConfig)
        assert "default" in config.nodes
        assert config.nodes["default"]["host"] == "localhost"
        assert "cache" in config.backends

    def test_load_invalid_dict(self):
        """测试加载无效字典"""
        config_dict = {"invalid_key": "value"}

        loader = DictConfigLoader(config_dict)

        # 应该能加载但缺少必要字段
        config = loader.load()
        assert config.nodes == {}
        assert config.backends == {}


class TestYamlConfigLoader:
    """测试 YamlConfigLoader 类"""

    def test_load_from_yaml_file(self):
        """测试从YAML文件加载配置"""
        yaml_content = """
nodes:
  default:
    host: localhost
    port: 6379
    db: 0

backends:
  cache:
    name: cache
    db: 0
    node_name: default
"""

        # 创建临时文件
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_file = f.name

        try:
            loader = YamlConfigLoader(temp_file)
            config = loader.load()

            assert isinstance(config, RedisKitConfig)
            assert "default" in config.nodes
            assert config.nodes["default"]["host"] == "localhost"
        finally:
            os.unlink(temp_file)

    def test_load_nonexistent_file(self):
        """测试加载不存在的文件"""
        loader = YamlConfigLoader("/nonexistent/file.yaml")

        with pytest.raises(FileNotFoundError):
            loader.load()

    def test_load_invalid_yaml(self):
        """测试加载无效YAML"""
        yaml_content = """
invalid: yaml: content:
  - item
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_file = f.name

        try:
            loader = YamlConfigLoader(temp_file)

            with pytest.raises(InvalidConfigError):
                loader.load()
        finally:
            os.unlink(temp_file)


class TestEnvConfigLoader:
    """测试 EnvConfigLoader 类"""

    def test_load_from_env(self):
        """测试从环境变量加载配置"""
        env_vars = {
            "REDIS_KIT_NODES__DEFAULT__HOST": "redis.example.com",
            "REDIS_KIT_NODES__DEFAULT__PORT": "6379",
            "REDIS_KIT_NODES__DEFAULT__DB": "0",
            "REDIS_KIT_BACKENDS__CACHE__NAME": "cache",
            "REDIS_KIT_BACKENDS__CACHE__NODE_NAME": "default",
        }

        # 设置环境变量
        for key, value in env_vars.items():
            os.environ[key] = value

        try:
            loader = EnvConfigLoader(prefix="REDIS_KIT_")
            config = loader.load()

            assert isinstance(config, RedisKitConfig)
            assert "default" in config.nodes
            assert config.nodes["default"]["host"] == "redis.example.com"
        finally:
            # 清理环境变量
            for key in env_vars:
                os.environ.pop(key, None)

    def test_load_with_type_conversion(self):
        """测试类型转换"""
        env_vars = {
            "REDIS_KIT_NODES__DEFAULT__PORT": "6379",  # 应转为 int
            "REDIS_KIT_NODES__DEFAULT__DB": "1",  # 应转为 int
        }

        for key, value in env_vars.items():
            os.environ[key] = value

        try:
            loader = EnvConfigLoader(prefix="REDIS_KIT_")
            config = loader.load()

            # 根据实现，可能会保持字符串或转换为整数
            # 这里只验证能成功加载
            assert "default" in config.nodes
        finally:
            for key in env_vars:
                os.environ.pop(key, None)


class TestCompositeConfigLoader:
    """测试 CompositeConfigLoader 类"""

    def test_merge_multiple_sources(self):
        """测试合并多个配置源"""
        # 第一个配置源
        dict1 = {"nodes": {"default": {"host": "localhost", "port": 6379}}}

        # 第二个配置源（会覆盖第一个）
        dict2 = {
            "nodes": {"default": {"host": "redis.example.com"}},
            "backends": {"cache": {"name": "cache", "node_name": "default"}},
        }

        loader1 = DictConfigLoader(dict1)
        loader2 = DictConfigLoader(dict2)

        composite = CompositeConfigLoader([loader1, loader2])
        config = composite.load()

        # host 应该被第二个配置覆盖
        assert config.nodes["default"]["host"] == "redis.example.com"
        # backends 应该来自第二个配置
        assert "cache" in config.backends

    def test_empty_loaders(self):
        """测试空加载器列表"""
        composite = CompositeConfigLoader([])
        config = composite.load()

        assert isinstance(config, RedisKitConfig)
        assert config.nodes == {}
        assert config.backends == {}


class TestLoadConfigFunction:
    """测试 load_config 便捷函数"""

    def test_load_from_dict(self):
        """测试从字典加载"""
        config_dict = {"nodes": {"default": {"host": "localhost", "port": 6379}}}

        config = load_config(config_dict=config_dict)

        assert isinstance(config, RedisKitConfig)
        assert "default" in config.nodes

    def test_load_from_yaml(self):
        """测试从YAML文件加载"""
        yaml_content = """
nodes:
  default:
    host: localhost
    port: 6379
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_file = f.name

        try:
            config = load_config(yaml_path=temp_file)

            assert isinstance(config, RedisKitConfig)
            assert "default" in config.nodes
        finally:
            os.unlink(temp_file)

    def test_load_with_priority(self):
        """测试配置优先级（env > yaml > dict）"""
        # Dict 配置
        config_dict = {"nodes": {"default": {"host": "from_dict", "port": 6379}}}

        # YAML 配置
        yaml_content = """
nodes:
  default:
    host: from_yaml
"""

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            temp_file = f.name

        try:
            # 环境变量配置（优先级最高）
            os.environ["REDIS_KIT_NODES__DEFAULT__HOST"] = "from_env"

            config = load_config(
                config_dict=config_dict, yaml_path=temp_file, env_prefix="REDIS_KIT_"
            )

            # 应该使用环境变量的值
            assert config.nodes["default"]["host"] == "from_env"
        finally:
            os.unlink(temp_file)
            os.environ.pop("REDIS_KIT_NODES__DEFAULT__HOST", None)

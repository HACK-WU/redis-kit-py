# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

redis-kit 配置加载器模块

提供多种配置加载方式
"""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict

from redis_kit.config.schema import RedisKitConfig
from redis_kit.exceptions import ConfigurationError, MissingConfigError


class ConfigLoader(ABC):
    """
    配置加载器基类

    所有配置加载器必须继承此类。
    """

    @abstractmethod
    def load(self) -> RedisKitConfig:
        """
        加载配置

        Returns:
            RedisKitConfig 实例

        Raises:
            ConfigurationError: 加载失败时抛出
        """
        pass


class DictConfigLoader(ConfigLoader):
    """
    字典配置加载器

    从 Python 字典加载配置。

    Example:
        >>> loader = DictConfigLoader({
        ...     "nodes": {
        ...         "default": {"host": "localhost", "port": 6379}
        ...     }
        ... })
        >>> config = loader.load()
    """

    def __init__(self, config: Dict[str, Any]):
        """
        初始化加载器

        Args:
            config: 配置字典
        """
        self._config = config

    def load(self) -> RedisKitConfig:
        """加载配置"""
        return RedisKitConfig.from_dict(self._config)


class YamlConfigLoader(ConfigLoader):
    """
    YAML 配置加载器

    从 YAML 文件加载配置。

    Example:
        >>> loader = YamlConfigLoader("/path/to/config.yaml")
        >>> config = loader.load()
    """

    def __init__(self, path: str):
        """
        初始化加载器

        Args:
            path: YAML 文件路径
        """
        self._path = Path(path)

    def load(self) -> RedisKitConfig:
        """加载配置"""
        try:
            import yaml
        except ImportError:
            raise ConfigurationError(
                "pyyaml is required for YAML config. Install with: pip install pyyaml"
            )

        if not self._path.exists():
            raise MissingConfigError(str(self._path))

        try:
            with open(self._path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
        except Exception as e:
            raise ConfigurationError(f"Failed to load YAML config: {e}")

        if not isinstance(config, dict):
            raise ConfigurationError("YAML config must be a dictionary")

        return RedisKitConfig.from_dict(config)


class EnvConfigLoader(ConfigLoader):
    """
    环境变量配置加载器

    从环境变量加载配置。

    环境变量命名规则：
    - REDIS_KIT_NODE_{NODE_ID}_{FIELD}: 节点配置
    - REDIS_KIT_{FIELD}: 其他配置

    Example:
        # 设置环境变量
        export REDIS_KIT_NODE_DEFAULT_HOST=localhost
        export REDIS_KIT_NODE_DEFAULT_PORT=6379
        export REDIS_KIT_KEY_PREFIX=myapp

        >>> loader = EnvConfigLoader()
        >>> config = loader.load()
    """

    DEFAULT_PREFIX = "REDIS_KIT_"

    def __init__(self, prefix: str = None):
        """
        初始化加载器

        Args:
            prefix: 环境变量前缀，默认 REDIS_KIT_
        """
        self._prefix = prefix or self.DEFAULT_PREFIX

    def load(self) -> RedisKitConfig:
        """加载配置"""
        config: Dict[str, Any] = {"nodes": {}, "backends": {}}

        for key, value in os.environ.items():
            if not key.startswith(self._prefix):
                continue

            # 移除前缀
            field_path = key[len(self._prefix) :]

            # 解析节点配置
            if field_path.startswith("NODE_"):
                self._parse_node_config(field_path[5:], value, config)
            # 解析后端配置
            elif field_path.startswith("BACKEND_"):
                self._parse_backend_config(field_path[8:], value, config)
            # 解析其他配置
            else:
                self._parse_general_config(field_path, value, config)

        return RedisKitConfig.from_dict(config)

    def _parse_node_config(
        self, field_path: str, value: str, config: Dict[str, Any]
    ) -> None:
        """解析节点配置"""
        parts = field_path.split("_", 1)
        if len(parts) != 2:
            return

        node_id = parts[0].lower()
        field = parts[1].lower()

        if node_id not in config["nodes"]:
            config["nodes"][node_id] = {}

        config["nodes"][node_id][field] = self._parse_value(value)

    def _parse_backend_config(
        self, field_path: str, value: str, config: Dict[str, Any]
    ) -> None:
        """解析后端配置"""
        parts = field_path.split("_", 1)
        if len(parts) != 2:
            return

        backend_name = parts[0].lower()
        field = parts[1].lower()

        if backend_name not in config["backends"]:
            config["backends"][backend_name] = {}

        config["backends"][backend_name][field] = self._parse_value(value)

    def _parse_general_config(
        self, field_path: str, value: str, config: Dict[str, Any]
    ) -> None:
        """解析通用配置"""
        field = field_path.lower()
        config[field] = self._parse_value(value)

    def _parse_value(self, value: str) -> Any:
        """
        解析环境变量值

        支持自动类型转换：布尔值、整数、浮点数
        """
        # 布尔值
        if value.lower() in ("true", "yes", "1", "on"):
            return True
        if value.lower() in ("false", "no", "0", "off"):
            return False

        # 整数
        try:
            return int(value)
        except ValueError:
            pass

        # 浮点数
        try:
            return float(value)
        except ValueError:
            pass

        # 字符串
        return value


class CompositeConfigLoader(ConfigLoader):
    """
    组合配置加载器

    按优先级从多个来源加载配置，后面的覆盖前面的。

    Example:
        >>> loader = CompositeConfigLoader([
        ...     YamlConfigLoader("default.yaml"),
        ...     YamlConfigLoader("local.yaml"),
        ...     EnvConfigLoader(),
        ... ])
        >>> config = loader.load()
    """

    def __init__(self, loaders: list):
        """
        初始化加载器

        Args:
            loaders: 配置加载器列表，按优先级从低到高排列
        """
        self._loaders = loaders

    def load(self) -> RedisKitConfig:
        """加载配置"""
        merged_config: Dict[str, Any] = {}

        for loader in self._loaders:
            try:
                config = loader.load()
                config_dict = config.to_dict()
                merged_config = self._deep_merge(merged_config, config_dict)
            except MissingConfigError:
                # 配置文件不存在时跳过
                continue
            except Exception as e:
                raise ConfigurationError(f"Failed to load config from {loader}: {e}")

        return RedisKitConfig.from_dict(merged_config)

    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """深度合并字典"""
        result = base.copy()
        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result


def load_config(
    config: Dict[str, Any] = None,
    yaml_path: str = None,
    env_prefix: str = None,
) -> RedisKitConfig:
    """
    便捷配置加载函数

    按以下优先级加载配置（后面的覆盖前面的）：
    1. YAML 文件
    2. 字典配置
    3. 环境变量

    Args:
        config: 字典配置
        yaml_path: YAML 文件路径
        env_prefix: 环境变量前缀

    Returns:
        RedisKitConfig 实例

    Example:
        >>> config = load_config(
        ...     yaml_path="config.yaml",
        ...     config={"key_prefix": "myapp"},
        ... )
    """
    loaders = []

    if yaml_path:
        loaders.append(YamlConfigLoader(yaml_path))

    if config:
        loaders.append(DictConfigLoader(config))

    if env_prefix is not None or not loaders:
        loaders.append(EnvConfigLoader(env_prefix))

    if not loaders:
        return RedisKitConfig()

    return CompositeConfigLoader(loaders).load()

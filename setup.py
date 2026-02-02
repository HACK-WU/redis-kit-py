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

from setuptools import find_packages, setup

setup(
    name="redis-kit",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "redis>=4.0.0",
    ],
    extras_require={
        "yaml": ["pyyaml>=5.0"],
        "metrics": ["prometheus-client>=0.14.0"],
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "fakeredis>=2.0.0",
        ],
    },
    python_requires=">=3.8",
    author="BlueKing Monitor Team",
    author_email="contactus_bk@tencent.com",
    description="A reusable Redis caching framework for Python",
    long_description=open("README.md").read()
    if __import__("os").path.exists("README.md")
    else "",
    long_description_content_type="text/markdown",
    url="https://github.com/TencentBlueKing/bk-monitor",
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)

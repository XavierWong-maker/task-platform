# 任务调度与数据处理平台

任务调度与数据处理项目，所有数据均为模拟数据。

## 环境要求
- Python 3.12+
- conda

## 快速开始

创建 conda 环境

    conda create -n task-platform python=3.12 -y
    conda activate task-platform

安装依赖

    pip install pytest ruff mypy pre-commit

初始化仓库

    git init
    pre-commit install

## 开发命令

运行测试

    python -m pytest

代码检查

    ruff check .

类型检查

    mypy src

## 项目状态
当前阶段：阶段 0 - 工程准备

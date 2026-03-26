# BioFlow-CLI

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9%2B-green.svg)](https://python.org)

**[English Documentation](README.md)**

专业的生物信息学 TUI（终端用户界面）工作流工具，支持完整的中英文国际化。

BioFlow-CLI 为常见的生物信息学任务提供实用的序列处理和环境管理功能，专为交互式使用和自动化脚本设计。

## 开源声明

BioFlow-CLI 是一个基于 **MIT 许可证** 发布的 **开源项目**。

- 您可以在商业或非商业场景中使用、修改和分发此项目。
- 您必须保留原始版权和许可证声明。
- 欢迎通过 Issue 和 Pull Request 提交贡献。

许可证全文：[MIT License](LICENSE)

## 功能特性

- **双模式运行** — 提供交互式 TUI (`bioflow`) 和脚本友好的 CLI (`bioflow ...`)
- **国际化支持** — 完整的中英文本地化；首次运行时选择语言，偏好保存至用户配置目录
- **环境管理器** — 一键检测和安装常用生物工具（FastQC、SAMtools、BWA、BLAST+、Trimmomatic），通过 Conda 管理
- **序列格式化** — 标准化 FASTA/FASTQ 文件，支持自定义行宽，并使用流式读写降低大文件内存占用
- **批量处理** — 支持目录递归扫描、多进程加速、多文件处理、进度跟踪及统计表格
- **序列比对** — 集成 BWA + SAMtools 完整流程，支持建索引、比对、排序、BAM 索引与比对统计
- **QC 流程** — 集成 FastQC + Trimmomatic 的质量控制流水线

## 快速开始

### 安全安装（推荐）

```bash
# 下载安装脚本及其校验和
curl -LO https://github.com/BioCael-Dev/BioFlow-CLI/releases/latest/download/install.sh
curl -LO https://github.com/BioCael-Dev/BioFlow-CLI/releases/latest/download/install.sh.sha256

# 运行前验证完整性（平台特定）
# Linux：
sha256sum -c install.sh.sha256

# macOS：
shasum -a 256 -c install.sh.sha256

# Windows（PowerShell）：
# (Get-FileHash install.sh -Algorithm SHA256).Hash -eq (Get-Content install.sh.sha256).Split()[0]

# 如果验证失败，请勿继续 — 重新下载文件

# 运行安装程序（Linux/macOS）
bash install.sh
```

### 其他安装方式

```bash
# 克隆仓库
git clone https://github.com/BioCael-Dev/BioFlow-CLI.git
cd BioFlow-CLI

# 方式 A：使用安装脚本（不验证）
chmod +x install.sh && ./install.sh

# 方式 B：本地开发安装
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

## 项目结构

```
BioFlow-CLI/
├── bioflow/
│   ├── __init__.py        # 包元数据 (版本号等)
│   ├── main.py            # TUI 入口与主菜单
│   ├── cli.py             # 非交互式 CLI 接口
│   ├── i18n.py            # 国际化核心模块
│   ├── env_manager.py     # 生物工具检测与安装
│   ├── bio_tasks.py       # 序列格式化任务逻辑
│   ├── alignment.py       # 序列比对流程
│   ├── pipeline.py        # QC 流程管理
│   ├── preflight.py       # 环境预检
│   └── locales/
│       ├── __init__.py    # 语言包注册
│       ├── en.py          # 英文字符串
│       └── zh.py          # 中文字符串
├── install.sh             # 双语安装脚本
├── pyproject.toml         # 构建配置
├── requirements.txt       # 依赖列表
├── LICENSE                # MIT 许可证
├── README.md              # 英文文档
└── README_CN.md           # 中文文档
```

## 使用说明

BioFlow-CLI 支持 **交互式 TUI 模式** 和 **非交互式 CLI 模式**。

### TUI 模式（交互式）

不带参数启动以进入交互式菜单：

```bash
bioflow
```

首次启动时，会提示您选择语言。选择结果会持久化保存，可随时通过设置菜单更改。

### CLI 模式（非交互式）

使用命令行参数进行自动化和脚本编写：

```bash
# 格式化单文件 FASTA/FASTQ（自动识别）
bioflow seq --input input.fasta --output output.fasta --width 80

# 批量格式化多个文件
bioflow batch --input-dir ./data --output-dir ./formatted --pattern "*.fasta" --width 80

# 带递归扫描的批量处理
bioflow batch -i ./data -o ./formatted -p "*.fastq" -r -w 60

# 使用 4 个工作进程加速批量处理
bioflow batch -i ./data -o ./formatted -p "*.fastq" -r --workers 4

# 运行 QC 流程
bioflow qc --input reads.fastq --output qc_results/ --adapter adapters.fa --minlen 36

# 运行序列比对流程
bioflow align --ref ref.fa --input reads.fastq --output aligned.bam --threads 4

# 列出生物工具安装状态
bioflow env --list

# 安装指定工具
bioflow env --install fastqc

# JSON 输出（便于自动化集成）
bioflow --json seq --input reads.fastq
bioflow --json batch -i ./data -o ./formatted
```

#### 退出码

| 代码 | 含义 |
|---|---|
| `0` | 成功 |
| `1` | 运行时错误（如：解析失败、遇错中断） |
| `2` | 参数错误（如：文件/目录不存在、无效宽度） |
| `3` | 依赖缺失（如：Conda、BWA、SAMtools 等未安装） |

#### 输出流

- **stdout**：结果数据和 JSON 输出（用于管道传输）
- **stderr**：进度消息、警告和错误信息

#### 批量并发

- `bioflow batch --workers N` 可启用多进程批量格式化
- 默认值为 `1`
- 在多核机器上处理大量文件时可适当提高并发数

### 配置文件位置

| 操作系统 | 路径 |
|---|---|
| macOS | `~/Library/Application Support/bioflow/config.json` |
| Linux | `~/.config/bioflow/config.json`（或 `$XDG_CONFIG_HOME/bioflow/`） |
| Windows | `%APPDATA%\bioflow\config.json` |

## 开发

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## 项目状态

当前开发版本：**v0.4.0**

## 许可证

本项目基于 MIT 许可证开源。详见 [LICENSE](LICENSE)。

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
- **BLAST 检索** — 集成 `makeblastdb` + `blastn` 基础核酸检索流程，输出标准 tabular 结果
- **QC 流程** — 集成 FastQC + Trimmomatic 的质量控制流水线
- **运行检查** — 提供 `bioflow inspect`，可汇总运行状态、关键输出、失败步骤与日志位置
- **HTML 运行报告** — 可将单次或多次工作流运行导出为单文件 HTML 汇总报告
- **失败诊断** — 为失败的 workflow 提供统一 CLI 诊断输出，包括失败步骤、失败命令、stderr 摘要和日志路径
- **YAML 工作流配置** — 可通过配置文件复用 QC / 比对 / 检索参数
- **结构化输出** — 支持 `--json` 输出，便于自动化集成和脚本调用
- **标准退出码** — 提供统一的成功 / 参数错误 / 运行时错误 / 依赖缺失状态码

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
│   ├── search.py          # BLAST 检索流程
│   ├── pipeline.py        # QC 流程管理
│   ├── inspect.py         # 运行检查与诊断摘要
│   ├── report.py          # HTML 运行报告导出
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

# 运行 QC 流程，并指定统一运行目录
bioflow qc --input reads.fastq --outdir runs/qc-001 --adapter adapters.fa --minlen 36

# 从配置文件运行 QC 流程
bioflow qc --config examples/qc.yml

# 恢复中断的 QC 流程
bioflow qc --input reads.fastq --outdir runs/qc-001 --resume

# 运行序列比对流程
bioflow align --ref ref.fa --input reads.fastq --outdir runs/align-001 --output aligned.bam --threads 4

# 从配置文件运行序列比对流程
bioflow align --config examples/align.yml

# 恢复中断的比对流程
bioflow align --ref ref.fa --input reads.fastq --outdir runs/align-001 --resume

# 运行 BLAST 核酸检索
bioflow search --db ref.fa --query query.fa --outdir runs/search-001 --output hits.tsv --evalue 1e-5 --max-target-seqs 20

# 仅展示前 3 个摘要命中
bioflow search --db ref.fa --query query.fa --output hits.tsv --top 3

# 从配置文件运行 BLAST 检索
bioflow search --config examples/search.yml

# 恢复中断的 BLAST 检索
bioflow search --db ref.fa --query query.fa --outdir runs/search-001 --resume

# 为单次运行导出 HTML 报告
bioflow report --input runs/qc-001 --output qc-report.html

# 为目录下多次运行导出汇总 HTML 报告
bioflow report --input runs --output runs-report.html --title "BioFlow 运行汇总"

# 检查运行目录状态、关键输出和日志路径
bioflow inspect --input runs/qc-001

# JSON 模式运行检查
bioflow --json inspect --input runs/qc-001

# 检查时同时显示最新 stderr 摘要
bioflow inspect --input runs/qc-001 --show-log tail

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

#### 检索结果摘要

- `bioflow search --top N` 可控制摘要展示的 Top hits 数量
- JSON 模式现包含 `summary.best_hit`、`summary.top_hits` 和聚合统计字段
- 原始 BLAST tabular 结果仍会写入 TSV 输出文件

#### YAML 工作流配置

- `bioflow qc --config qc.yml`
- `bioflow align --config align.yml`
- `bioflow search --config search.yml`
- 参数优先级为：CLI 显式参数 > YAML 配置 > 内置默认值
- 示例模板已放在 `examples/`

#### 工作流输出目录规范

- `qc`、`align`、`search` 现在统一使用标准运行目录布局
- 可通过 `--outdir` 指定运行根目录；未指定时会在输入文件旁自动创建 `qc_run`、`align_run` 或 `search_run`
- 每次运行都会生成 `logs/`、`results/`、`tmp/` 和 `metadata.json`
- `metadata.json` 现在额外记录输入文件大小 / 修改时间 / sha256、运行环境、工具版本和失败摘要
- 若运行失败，诊断日志会保留在 `logs/` 目录中，便于排错

#### 恢复执行与检查点

- `bioflow qc --resume`、`bioflow align --resume`、`bioflow search --resume` 可从最近一次有效检查点恢复执行
- 已完成且输出有效的步骤会自动复用
- resume 判断除了检查产物是否存在，也会校验 metadata 步骤状态和关键输出描述字段
- 缺失或损坏的中间结果会被识别并重新计算
- TUI 模式下检测到可恢复运行目录时会给出恢复提示

#### 运行检查

- `bioflow inspect --input <run_dir>` 可输出运行状态、关键输出、失败步骤和日志路径
- `bioflow inspect --input <run_dir> --show-log tail` 可同时输出 stderr 最后若干行，便于快速排障
- `bioflow --json inspect --input <run_dir>` 可输出结构化诊断结果，便于脚本集成
- 旧版运行目录仍可读取，不要求必须包含所有新字段

#### 失败诊断

- 失败的 `qc`、`align`、`search` 现在会输出统一 CLI 诊断块
- 诊断块包含失败步骤、失败命令、stdout/stderr 日志路径和 stderr 摘要
- 同一份诊断信息也会写入 `metadata.json` 的 `failure_details`

#### HTML 报告导出

- `bioflow report --input <run_dir>` 可基于 `metadata.json` 导出单次运行报告
- `bioflow report --input <parent_dir>` 会扫描其一级子目录并合并多次运行结果
- 生成的报告包含运行摘要、输入详情、运行环境、工具版本、日志路径、失败摘要、失败详情以及步骤状态表
- TUI 主菜单也已提供报告导出入口

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
pip install -e .[dev]
```

## 项目状态

当前开发版本：**v0.6.1**

## 许可证

本项目基于 MIT 许可证开源。详见 [LICENSE](LICENSE)。

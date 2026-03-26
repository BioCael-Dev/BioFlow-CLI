# BioFlow-CLI

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9%2B-green.svg)](https://python.org)

**A bioinformatics workflow toolkit for terminal users (TUI + CLI), with English/Chinese i18n.**

BioFlow-CLI provides practical sequence processing and environment management for common bioinformatics tasks, designed for both interactive use and automation scripts.

## Open Source Statement

BioFlow-CLI is an **open-source project** released under the **MIT License**.

- You may use, modify, and redistribute this project in commercial or non-commercial scenarios.
- You must keep the original copyright and license notice.
- Contributions are welcome via Issues and Pull Requests.

License text: [MIT License](LICENSE)

## Key Features

- **Dual Mode**: Interactive TUI (`bioflow`) and script-friendly CLI (`bioflow ...`)
- **i18n**: Full English/Chinese localization with persisted language preference
- **Environment Manager**: Detect/install FastQC, SAMtools, BWA, BLAST+, Trimmomatic via Conda
- **Sequence Formatting**:
  - FASTA formatting with configurable line width
  - FASTQ formatting with auto-detection and quality summary (Avg Q / Q20 / Q30)
  - Streaming read/write path for large files with lower memory usage
  - Batch processing with optional multi-process acceleration, progress tracking, and result tables
- **Sequence Alignment**:
  - BWA index + BWA mem + SAMtools sort/index + `samtools flagstat`
  - Mapping statistics summary for terminal workflows
- **QC Pipeline**: Integrated FastQC + Trimmomatic workflow
- **Structured Output**: `--json` output for automation pipelines
- **Stable Exit Codes**: standardized success/error/dependency signaling

## Installation

### Option A: Secure installer (recommended)

```bash
curl -LO https://github.com/BioCael-Dev/BioFlow-CLI/releases/latest/download/install.sh
curl -LO https://github.com/BioCael-Dev/BioFlow-CLI/releases/latest/download/install.sh.sha256

# Linux
sha256sum -c install.sh.sha256

# macOS
shasum -a 256 -c install.sh.sha256

bash install.sh
```

### Option B: Local development install

```bash
git clone https://github.com/BioCael-Dev/BioFlow-CLI.git
cd BioFlow-CLI
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

## Quick Start

### TUI mode

```bash
bioflow
```

### CLI mode

```bash
# Format single FASTA file
bioflow seq --input input.fasta --output output.fasta --width 80

# Format FASTQ (auto-detected)
bioflow seq --input reads.fastq --output reads.formatted.fastq --width 80

# Batch format multiple files
bioflow batch --input-dir ./data --output-dir ./formatted --pattern "*.fasta" --width 80

# Batch format with recursive scan
bioflow batch -i ./data -o ./formatted -p "*.fastq" -r -w 60

# Batch format with 4 worker processes
bioflow batch -i ./data -o ./formatted -p "*.fastq" -r --workers 4

# Run QC pipeline
bioflow qc --input reads.fastq --output qc_results/ --adapter adapters.fa --minlen 36

# Run alignment pipeline
bioflow align --ref ref.fa --input reads.fastq --output aligned.bam --threads 4

# List tool status
bioflow env --list

# Install a tool
bioflow env --install fastqc

# JSON output for automation
bioflow --json seq --input reads.fastq
bioflow --json batch -i ./data -o ./formatted
```

## CLI Behavior Contract

### Exit Codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Runtime error |
| `2` | Argument error |
| `3` | Dependency missing |

### Output Streams

- `stdout`: normal result data (including JSON)
- `stderr`: progress, warnings, and errors

### Batch Concurrency

- `bioflow batch --workers N` enables multi-process batch formatting
- default `--workers` value is `1`
- use a larger worker count for large batch jobs on multi-core machines

## Configuration

Language config is saved per OS:

- macOS: `~/Library/Application Support/bioflow/config.json`
- Linux: `~/.config/bioflow/config.json` (or `$XDG_CONFIG_HOME/bioflow/`)
- Windows: `%APPDATA%\bioflow\config.json`

## Development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Project Status

Current development version: **v0.4.0**

Release history and notes: [GitHub Releases](https://github.com/BioCael-Dev/BioFlow-CLI/releases)

## License

This project is licensed under the [MIT License](LICENSE).

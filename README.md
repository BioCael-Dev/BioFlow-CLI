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
- **BLAST Search**:
  - `makeblastdb` + `blastn` nucleotide search workflow
  - Tabular result output (`outfmt 6`) for downstream analysis
- **QC Pipeline**: Integrated FastQC + Trimmomatic workflow
- **YAML Workflow Config**: run QC / alignment / search from reusable config files
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

# Run QC pipeline with a managed run directory
bioflow qc --input reads.fastq --outdir runs/qc-001 --adapter adapters.fa --minlen 36

# Run QC pipeline from config
bioflow qc --config examples/qc.yml

# Run alignment pipeline
bioflow align --ref ref.fa --input reads.fastq --outdir runs/align-001 --output aligned.bam --threads 4

# Run alignment pipeline from config
bioflow align --config examples/align.yml

# Run BLAST nucleotide search
bioflow search --db ref.fa --query query.fa --outdir runs/search-001 --output hits.tsv --evalue 1e-5 --max-target-seqs 20

# Show only top 3 summarized hits
bioflow search --db ref.fa --query query.fa --output hits.tsv --top 3

# Run BLAST search from config
bioflow search --config examples/search.yml

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

### Search Summary

- `bioflow search --top N` controls how many top hits are summarized
- JSON mode now includes `summary.best_hit`, `summary.top_hits`, and aggregate hit statistics
- Raw BLAST tabular output is still written to the TSV file

### YAML Workflow Config

- `bioflow qc --config qc.yml`
- `bioflow align --config align.yml`
- `bioflow search --config search.yml`
- parameter precedence is: explicit CLI argument > YAML config > built-in default
- example templates are available in `examples/`

### Workflow Output Layout

- `qc`, `align`, and `search` now share a standard run directory layout
- set `--outdir` to control the run root; if omitted, BioFlow-CLI creates `qc_run`, `align_run`, or `search_run` beside the input file
- each run contains `logs/`, `results/`, `tmp/`, and `metadata.json`
- on failure, diagnostic stdout/stderr logs are retained under `logs/`

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

Current development version: **v0.5.0**

Release history and notes: [GitHub Releases](https://github.com/BioCael-Dev/BioFlow-CLI/releases)

## License

This project is licensed under the [MIT License](LICENSE).

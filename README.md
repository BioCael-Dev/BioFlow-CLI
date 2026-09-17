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
- **Environment Manager**: Detect/install FastQC, SAMtools, BCFtools, BWA, minimap2, BLAST+, Trimmomatic, and Salmon via Conda
- **Sequence Formatting**:
  - FASTA formatting with configurable line width
  - FASTQ formatting with auto-detection and quality summary (Avg Q / Q20 / Q30)
  - Streaming read/write path for large files with lower memory usage
  - Batch processing with optional multi-process acceleration, progress tracking, and result tables
- **Sequence Alignment**:
  - BWA index + BWA mem + SAMtools sort/index + `samtools flagstat`
  - Single-end and paired-end read support with mapping statistics summary
- **Long-read QC and Alignment**:
  - Streaming FASTA/FASTQ length statistics including read count, total bases, mean length, and N50
  - FASTQ quality summaries (Avg Q / Q20 / Q30), including gzip-compressed input support
  - minimap2 presets for Oxford Nanopore (`map-ont`), PacBio HiFi (`map-hifi`), and PacBio CLR (`map-pb`)
  - Sorted/indexed BAM, flagstat, QC JSON, and combined long-read summary JSON outputs
- **Variant Calling**:
  - BCFtools mpileup/call workflow from a reference FASTA and coordinate-sorted BAM
  - Automatic FASTA and BAM indexing with SAMtools
  - Configurable QUAL/depth soft filtering with raw BCF, compressed VCF, Tabix index, BCFtools statistics, and structured summary outputs
  - Record-level total, PASS, filtered, SNP, INDEL, MNP, multiallelic, and mean-QUAL metrics
- **BLAST Search**:
  - `makeblastdb` + `blastn` nucleotide search workflow
  - Tabular result output (`outfmt 6`) for downstream analysis
- **QC Pipeline**: Integrated FastQC + Trimmomatic workflow for single-end and paired-end reads
- **RNA-seq Quantification**:
  - FastQC + Salmon mapping-based transcript quantification
  - Single-end/paired-end reads, transcriptome index building, and prebuilt Salmon index reuse
  - `quant.sf`, run summary JSON, mapping statistics, and sample design metadata including lane/replicate
  - Project-level counts/TPM matrices and a sample metadata table for downstream R/Python analysis
- **Run Inspection**: `bioflow inspect` summarizes run status, critical outputs, failed steps, and log locations
- **HTML Run Reports**: Export one or more workflow runs into a portable single-file HTML summary with success-rate cards, failure summaries, sample search/sort, workflow metrics, and structured JSON/TSV summaries
- **Project Batch**: `bioflow project` executes mixed QC / alignment / long-read / variant / search / RNA-seq samples from one YAML file and emits project summaries plus a combined HTML report
- **Failure Diagnostics**: Unified failure output across workflows with failed step, failed command, stderr tail, and direct log paths
- **YAML Workflow Config**: run QC / alignment / long-read / variant / search / RNA-seq from reusable config files
- **Workflow Manifest**: built-in manifests describe workflow inputs, key outputs, supported profiles, and config schema validation
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

# Run QC pipeline in single-end mode
bioflow qc --input reads.fastq --outdir runs/qc-001 --adapter adapters.fa --minlen 36

# Run QC pipeline in paired-end mode
bioflow qc --input-r1 reads_1.fastq --input-r2 reads_2.fastq --outdir runs/qc-pe-001 --adapter adapters.fa --minlen 36

# Run QC pipeline from config
bioflow qc --config examples/qc.yml

# Run QC with the conda execution backend
bioflow qc --input reads.fastq --profile workstation --backend conda --conda-env bioflow-env --threads 4 --memory 8G --queue short --time-limit 02:00:00

# Resume an interrupted QC run
bioflow qc --input reads.fastq --outdir runs/qc-001 --resume

# Run alignment pipeline in single-end mode
bioflow align --ref ref.fa --input reads.fastq --outdir runs/align-001 --output aligned.bam --threads 4

# Run alignment pipeline in paired-end mode
bioflow align --ref ref.fa --input-r1 reads_1.fastq --input-r2 reads_2.fastq --outdir runs/align-pe-001 --output aligned.bam --threads 4

# Run alignment pipeline from config
bioflow align --config examples/align.yml

# Run alignment with the conda execution backend
bioflow align --ref ref.fa --input reads.fastq --threads 4 --profile workstation --backend conda --conda-env bioflow-env --memory 16G --queue short --time-limit 04:00:00

# Resume an interrupted alignment run
bioflow align --ref ref.fa --input reads.fastq --outdir runs/align-001 --resume

# Run Oxford Nanopore long-read QC and alignment
bioflow longread --ref ref.fa --input nanopore_reads.fastq.gz --outdir runs/longread-001 --preset map-ont --threads 8 --sample-id sample-long

# Run PacBio HiFi long-read alignment from config
bioflow longread --config examples/longread.yml --preset map-hifi

# Resume a long-read run after validating input hashes, preset, outputs, and execution environment
bioflow longread --ref ref.fa --input nanopore_reads.fastq.gz --outdir runs/longread-001 --resume

# Call and filter variants from a coordinate-sorted BAM
bioflow variant --ref ref.fa --bam aligned.sorted.bam --outdir runs/variant-001 --output sample.vcf.gz --min-qual 20 --min-depth 5 --threads 4 --sample-id sample-a

# Run variant calling from config
bioflow variant --config examples/variant.yml

# Resume variant calling after validating reference/BAM hashes, filters, outputs, and execution environment
bioflow variant --ref ref.fa --bam aligned.sorted.bam --outdir runs/variant-001 --resume

# Run BLAST nucleotide search
bioflow search --db ref.fa --query query.fa --outdir runs/search-001 --output hits.tsv --evalue 1e-5 --max-target-seqs 20

# Show only top 3 summarized hits
bioflow search --db ref.fa --query query.fa --output hits.tsv --top 3

# Run BLAST search from config
bioflow search --config examples/search.yml

# Run BLAST search with the container execution backend
bioflow search --db ref.fa --query query.fa --profile local --backend container --container-image ghcr.io/biocael-dev/bioflow-cli:latest --threads 2 --memory 4G

# Run paired-end RNA-seq and build a Salmon index from the transcriptome
bioflow rnaseq --transcriptome transcripts.fa --input-r1 reads_1.fastq --input-r2 reads_2.fastq --outdir runs/rnaseq-001 --threads 8 --sample-id sample-a --group control --condition untreated --lane L001 --replicate 1

# Run single-end RNA-seq with an existing Salmon index
bioflow rnaseq --index salmon-index --input reads.fastq --outdir runs/rnaseq-002 --library-type A

# Run RNA-seq from config
bioflow rnaseq --config examples/rnaseq.yml

# Resume an interrupted RNA-seq run
bioflow rnaseq --transcriptome transcripts.fa --input reads.fastq --outdir runs/rnaseq-001 --resume

# Run a mixed project batch from one YAML config
bioflow project --config examples/project.yml

# Continue other samples even if one sample fails
bioflow project --config examples/project.yml --continue-on-error

# Override project-level execution defaults from CLI
bioflow project --config examples/project.yml --profile workstation --backend conda --conda-env bioflow-env --threads 8 --memory 32G --queue short --time-limit 08:00:00

# Resume an interrupted BLAST search
bioflow search --db ref.fa --query query.fa --outdir runs/search-001 --resume

# Export an HTML report for one run
bioflow report --input runs/qc-001 --output qc-report.html

# Export a MultiQC-style combined HTML report plus structured summaries
bioflow report --input runs --output runs-report.html --summary-json summary.json --summary-tsv summary.tsv --title "BioFlow Run Summary"

# Inspect run metadata, outputs, and diagnostics
bioflow inspect --input runs/qc-001

# Inspect in JSON mode for automation
bioflow --json inspect --input runs/qc-001

# Show the latest stderr tail during inspection
bioflow inspect --input runs/qc-001 --show-log tail

# List tool status
bioflow env --list

# Install a tool
bioflow env --install fastqc
bioflow env --install salmon
bioflow env --install minimap2
bioflow env --install bcftools

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
- `bioflow rnaseq --config rnaseq.yml`
- `bioflow longread --config longread.yml`
- `bioflow variant --config variant.yml`
- `bioflow project --config project.yml`
- parameter precedence is: explicit CLI argument > YAML config > built-in default
- `qc`, `align`, and `rnaseq` support either `input` or the `input_r1` + `input_r2` pair
- `rnaseq` requires exactly one Salmon reference source: `transcriptome` or `index`
- RNA-seq design metadata accepts optional `sample_id`, `group`, `condition`, `lane`, and positive integer `replicate`
- `group` and `condition` must be provided together when either is used
- when a project uses RNA-seq group/condition design, every RNA-seq sample must provide both fields
- `longread` requires one reference FASTA and one FASTA/FASTQ reads file; `preset` accepts `map-ont`, `map-hifi`, or `map-pb`
- `variant` requires one reference FASTA and one coordinate-sorted BAM; `caller` is `bcftools`, while `min_qual` and `min_depth` define the `LowQual` soft-filter thresholds
- `input` cannot be combined with `input_r1` / `input_r2`
- workflow and project YAML files are validated against built-in workflow manifests before execution
- manifest-based schema checks catch unknown fields, invalid types, non-positive numeric values, and workflow-specific missing fields early
- `qc`, `align`, `longread`, `variant`, `search`, and `rnaseq` configs also support execution metadata fields: `profile`, `threads`, `memory`, `queue`, `time_limit`, `backend`, `conda_env`, and `container_image`
- `project` config uses a top-level `project:` section optionally, and supports `outdir`, `continue_on_error`, `report_title`, `profile`, `threads`, `memory`, `queue`, `time_limit`, `backend`, `conda_env`, `container_image`, and `samples`
- each `samples` item requires `sample_id`, `workflow`, and that workflow's normal required fields
- project-level execution fields are inherited by samples unless a sample overrides them
- project samples now execute through the same backend wrapper as direct workflow runs
- example templates are available in `examples/`

### Workflow Output Layout

- `qc`, `align`, `longread`, `variant`, `search`, and `rnaseq` share a standard run directory layout
- set `--outdir` to control the run root; if omitted, BioFlow-CLI creates `qc_run`, `align_run`, `longread_run`, `variant_run`, `search_run`, or `rnaseq_run` beside the input file
- each run contains `logs/`, `results/`, `tmp/`, and `metadata.json`
- `bioflow project` creates one project root with per-sample run directories such as `001-sample-qc-qc`
- each project run also writes `project_summary.json`, `summary.json`, `summary.tsv`, and `project_report.html`
- projects containing RNA-seq samples additionally write `counts_matrix.tsv` (Salmon `NumReads`), `tpm_matrix.tsv` (Salmon `TPM`), and `sample_metadata.tsv`; matrices include successful parseable Salmon runs while metadata retains failed, missing, and not-run samples
- metadata now records `metadata_schema_version`, input file size / mtime / sha256, runtime environment, tool versions, and failure summary
- metadata now also writes an `execution` block with `profile`, `backend`, `conda_env`, `container_image`, requested resources, and parameter source
- each workflow step now records its backend, raw command, resolved command, and environment fingerprint
- paired-end `qc` metadata also records `trimmed_r1`, `trimmed_r2`, `unpaired_r1`, and `unpaired_r2`
- paired-end `align` metadata records `input_r1`, `input_r2`, `bam`, `bai`, and paired flagstat metrics
- `rnaseq` metadata records sample design fields (including lane/replicate), FastQC/Salmon steps, `quant.sf`, Salmon meta information, mapping metrics, and transcript abundance summaries
- `longread` metadata records input format, length/N50 and FASTQ quality metrics separately from alignment metrics, plus minimap2 preset, BAM/BAI/flagstat, QC summary, and combined summary paths
- `variant` metadata records reference/BAM checksums, caller and filter settings, raw/resolved commands, raw BCF, filtered VCF, Tabix index, BCFtools stats, and structured variant metrics
- on failure, diagnostic stdout/stderr logs are retained under `logs/`

### Resume And Checkpoints

- `bioflow qc --resume`, `bioflow align --resume`, `bioflow longread --resume`, `bioflow variant --resume`, `bioflow search --resume`, and `bioflow rnaseq --resume` resume from the latest valid workflow checkpoint
- completed steps are reused automatically when their key outputs remain valid
- resume validation also checks metadata step status and required output descriptors before reusing a checkpoint
- resume also compares the execution fingerprint; changing profile, backend, environment, or requested resources forces recomputation
- long-read resume additionally compares reference/read hashes, minimap2 preset, and sample id before reusing outputs
- variant resume compares reference/BAM hashes, caller, filtering thresholds, sample id, outputs, and execution environment before reusing each step
- incomplete or corrupted intermediate outputs are detected and recomputed
- TUI mode now prompts when an existing run directory contains resumable metadata

### Execution Backends

- `system` runs tools directly on the host
- `conda` wraps tool calls with `conda run -n <env>`
- `container` wraps tool calls with `docker run` or `apptainer exec`
- `metadata.json` stores both the raw tool command and the resolved backend-specific command

### Run Inspection

- `bioflow inspect --input <run_dir>` prints workflow status, critical outputs, failed steps, and log paths
- `bioflow inspect --input <run_dir> --show-log tail` includes the latest stderr tail for faster triage
- `bioflow --json inspect --input <run_dir>` emits structured diagnostics for scripts
- old run directories remain readable even if they predate the enhanced metadata schema

### Failure Diagnostics

- failed `qc`, `align`, `longread`, `variant`, `search`, and `rnaseq` runs print a unified CLI diagnostic block
- the block includes failed step, failed command, stdout log path, stderr log path, and stderr tail
- backend-aware preflight now distinguishes missing tools from missing conda runtime, missing conda env, or missing container runtime/image
- the same diagnostics are persisted in `metadata.json` under `failure_details`

### HTML Reports

- `bioflow report --input <run_dir>` exports a single-run HTML report from `metadata.json`
- `bioflow report --input <parent_dir>` scans immediate subdirectories and combines multiple runs into one report
- `bioflow report --summary-json summary.json --summary-tsv summary.tsv` exports reusable aggregate data for downstream scripts
- the generated report now includes success-rate cards, failure summaries, sample search, run sorting, workflow/status filters, run navigation, and workflow-specific metric summaries
- QC reports summarize trimmed outputs and FastQC result directories
- alignment reports summarize BAM / BAI / flagstat outputs and key mapping metrics
- search reports summarize TSV / summary outputs, hit count, and best hit
- RNA-seq reports summarize `quant.sf`, Salmon metadata, mapping rate, mapped fragments, and expressed transcripts
- long-read reports summarize BAM / BAI / flagstat, read count, total bases, mean length, N50, FASTQ quality, and mapping rate independently from short-read alignment metrics
- variant reports summarize raw BCF, filtered/indexed VCF, BCFtools statistics, total/PASS records, and SNP/INDEL counts
- project RNA-seq reports also show sample-design groups, counts/TPM matrix links, sample metadata, and failed or missing quantification warnings
- TUI mode also exposes report export from the main menu

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
pip install -e .[dev]
```

## Project Status

Current development version: **v1.1.0**

Release history and notes: [GitHub Releases](https://github.com/BioCael-Dev/BioFlow-CLI/releases)

## License

This project is licensed under the [MIT License](LICENSE).

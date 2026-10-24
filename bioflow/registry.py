"""Built-in workflow manifest registry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FieldSpec:
    """Configuration field description used for lightweight schema checks."""

    name: str
    kind: str
    required_for_project: bool = False
    positive: bool = False


@dataclass(frozen=True)
class WorkflowManifest:
    """Static manifest for one built-in workflow."""

    workflow_id: str
    display_name: str
    supported_inputs: tuple[str, ...]
    supported_profiles: tuple[str, ...]
    key_outputs: tuple[str, ...]
    fields: dict[str, FieldSpec]
    project_fields: dict[str, FieldSpec]
    required_tools: tuple[str, ...] = ()
    cli_help: str = ""
    display_name_zh: str = ""
    supported_backends: tuple[str, ...] = ("system", "conda", "container")

    @property
    def menu_key(self) -> str:
        return f"menu_{self.workflow_id}"

    @property
    def example_config(self) -> str:
        return f"examples/{self.workflow_id}.yml"

    @property
    def allowed_keys(self) -> set[str]:
        return set(self.fields)

    @property
    def project_allowed_keys(self) -> set[str]:
        return set(self.project_fields)


EXECUTION_FIELDS: dict[str, FieldSpec] = {
    "profile": FieldSpec("profile", "str"),
    "threads": FieldSpec("threads", "int", positive=True),
    "memory": FieldSpec("memory", "str"),
    "queue": FieldSpec("queue", "str"),
    "time_limit": FieldSpec("time_limit", "str"),
    "backend": FieldSpec("backend", "backend"),
    "conda_env": FieldSpec("conda_env", "str"),
    "container_image": FieldSpec("container_image", "str"),
}


def _fields(*specs: FieldSpec) -> dict[str, FieldSpec]:
    fields = {spec.name: spec for spec in specs}
    fields.update(EXECUTION_FIELDS)
    return fields


def _project_fields(*specs: FieldSpec) -> dict[str, FieldSpec]:
    fields = {
        "sample_id": FieldSpec("sample_id", "str", required_for_project=True),
        "workflow": FieldSpec("workflow", "workflow", required_for_project=True),
    }
    fields.update(_fields(*specs))
    return fields


WORKFLOW_REGISTRY: dict[str, WorkflowManifest] = {
    "qc": WorkflowManifest(
        workflow_id="qc",
        display_name_zh='质量控制',
        required_tools=('fastqc', 'trimmomatic'),
        cli_help='Run QC pipeline (FastQC + Trimmomatic)',
        display_name="Quality Control",
        supported_inputs=("single-end", "paired-end"),
        supported_profiles=("local", "workstation", "hpc-slurm"),
        key_outputs=("trimmed", "trimmed_r1", "trimmed_r2", "unpaired_r1", "unpaired_r2", "fastqc_pre", "fastqc_post"),
        fields=_fields(
            FieldSpec("input", "str"),
            FieldSpec("input_r1", "str"),
            FieldSpec("input_r2", "str"),
            FieldSpec("output", "str"),
            FieldSpec("outdir", "str"),
            FieldSpec("adapter", "str"),
            FieldSpec("minlen", "int", positive=True),
            FieldSpec("resume", "bool"),
        ),
        project_fields=_project_fields(
            FieldSpec("input", "str"),
            FieldSpec("input_r1", "str"),
            FieldSpec("input_r2", "str"),
            FieldSpec("adapter", "str"),
            FieldSpec("minlen", "int", positive=True),
            FieldSpec("resume", "bool"),
        ),
    ),
    "align": WorkflowManifest(
        workflow_id="align",
        display_name_zh='序列比对',
        required_tools=('bwa', 'samtools'),
        cli_help='Run alignment pipeline (BWA + SAMtools)',
        display_name="Sequence Alignment",
        supported_inputs=("single-end", "paired-end"),
        supported_profiles=("local", "workstation", "hpc-slurm"),
        key_outputs=("bam", "bai", "flagstat"),
        fields=_fields(
            FieldSpec("ref", "str"),
            FieldSpec("input", "str"),
            FieldSpec("input_r1", "str"),
            FieldSpec("input_r2", "str"),
            FieldSpec("output", "str"),
            FieldSpec("outdir", "str"),
            FieldSpec("threads", "int", positive=True),
            FieldSpec("resume", "bool"),
        ),
        project_fields=_project_fields(
            FieldSpec("ref", "str", required_for_project=True),
            FieldSpec("input", "str"),
            FieldSpec("input_r1", "str"),
            FieldSpec("input_r2", "str"),
            FieldSpec("output", "str"),
            FieldSpec("threads", "int", positive=True),
            FieldSpec("resume", "bool"),
        ),
    ),
    "longread": WorkflowManifest(
        workflow_id="longread",
        display_name_zh='长读长质控与比对',
        required_tools=('minimap2', 'samtools'),
        cli_help='Run long-read QC and alignment (minimap2 + SAMtools)',
        display_name="Long-read Alignment",
        supported_inputs=("long-read-fasta", "long-read-fastq"),
        supported_profiles=("local", "workstation", "hpc-slurm"),
        key_outputs=("bam", "bai", "flagstat", "qc_summary", "summary"),
        fields=_fields(
            FieldSpec("ref", "str"),
            FieldSpec("input", "str"),
            FieldSpec("output", "str"),
            FieldSpec("outdir", "str"),
            FieldSpec("preset", "str"),
            FieldSpec("threads", "int", positive=True),
            FieldSpec("sample_id", "str"),
            FieldSpec("resume", "bool"),
        ),
        project_fields=_project_fields(
            FieldSpec("ref", "str", required_for_project=True),
            FieldSpec("input", "str", required_for_project=True),
            FieldSpec("output", "str"),
            FieldSpec("preset", "str"),
            FieldSpec("threads", "int", positive=True),
            FieldSpec("resume", "bool"),
        ),
    ),
    "variant": WorkflowManifest(
        workflow_id="variant",
        display_name_zh='变异检测',
        required_tools=('samtools', 'bcftools'),
        cli_help='Run variant calling from a sorted BAM (SAMtools + BCFtools)',
        display_name="Variant Calling",
        supported_inputs=("reference-fasta", "sorted-bam"),
        supported_profiles=("local", "workstation", "hpc-slurm"),
        key_outputs=("bcf", "vcf", "vcf_index", "bcftools_stats", "summary"),
        fields=_fields(
            FieldSpec("ref", "str"),
            FieldSpec("bam", "str"),
            FieldSpec("output", "str"),
            FieldSpec("outdir", "str"),
            FieldSpec("caller", "str"),
            FieldSpec("min_qual", "number", positive=True),
            FieldSpec("min_depth", "int", positive=True),
            FieldSpec("threads", "int", positive=True),
            FieldSpec("sample_id", "str"),
            FieldSpec("resume", "bool"),
        ),
        project_fields=_project_fields(
            FieldSpec("ref", "str", required_for_project=True),
            FieldSpec("bam", "str", required_for_project=True),
            FieldSpec("output", "str"),
            FieldSpec("caller", "str"),
            FieldSpec("min_qual", "number", positive=True),
            FieldSpec("min_depth", "int", positive=True),
            FieldSpec("threads", "int", positive=True),
            FieldSpec("resume", "bool"),
        ),
    ),
    "search": WorkflowManifest(
        workflow_id="search",
        display_name_zh='BLAST 检索',
        required_tools=('makeblastdb', 'blastn'),
        cli_help='Run BLAST nucleotide search',
        display_name="BLAST Search",
        supported_inputs=("database-query",),
        supported_profiles=("local", "workstation", "hpc-slurm"),
        key_outputs=("tsv", "summary"),
        fields=_fields(
            FieldSpec("db", "str"),
            FieldSpec("query", "str"),
            FieldSpec("output", "str"),
            FieldSpec("outdir", "str"),
            FieldSpec("evalue", "number", positive=True),
            FieldSpec("max_target_seqs", "int", positive=True),
            FieldSpec("top", "int", positive=True),
            FieldSpec("resume", "bool"),
        ),
        project_fields=_project_fields(
            FieldSpec("db", "str", required_for_project=True),
            FieldSpec("query", "str", required_for_project=True),
            FieldSpec("output", "str"),
            FieldSpec("evalue", "number", positive=True),
            FieldSpec("max_target_seqs", "int", positive=True),
            FieldSpec("top", "int", positive=True),
            FieldSpec("resume", "bool"),
        ),
    ),
    "rnaseq": WorkflowManifest(
        workflow_id="rnaseq",
        display_name_zh='RNA-seq 转录本定量',
        required_tools=('fastqc', 'salmon'),
        cli_help='Run RNA-seq quantification (FastQC + Salmon)',
        display_name="RNA-seq Quantification",
        supported_inputs=("single-end", "paired-end"),
        supported_profiles=("local", "workstation", "hpc-slurm"),
        key_outputs=("fastqc", "salmon_index", "quant_sf", "meta_info", "summary"),
        fields=_fields(
            FieldSpec("transcriptome", "str"),
            FieldSpec("index", "str"),
            FieldSpec("input", "str"),
            FieldSpec("input_r1", "str"),
            FieldSpec("input_r2", "str"),
            FieldSpec("outdir", "str"),
            FieldSpec("threads", "int", positive=True),
            FieldSpec("library_type", "str"),
            FieldSpec("sample_id", "str"),
            FieldSpec("group", "str"),
            FieldSpec("condition", "str"),
            FieldSpec("lane", "str"),
            FieldSpec("replicate", "int", positive=True),
            FieldSpec("resume", "bool"),
        ),
        project_fields=_project_fields(
            FieldSpec("transcriptome", "str"),
            FieldSpec("index", "str"),
            FieldSpec("input", "str"),
            FieldSpec("input_r1", "str"),
            FieldSpec("input_r2", "str"),
            FieldSpec("threads", "int", positive=True),
            FieldSpec("library_type", "str"),
            FieldSpec("group", "str"),
            FieldSpec("condition", "str"),
            FieldSpec("lane", "str"),
            FieldSpec("replicate", "int", positive=True),
            FieldSpec("resume", "bool"),
        ),
    ),
}


def list_workflow_manifests() -> list[WorkflowManifest]:
    """Return all built-in workflow manifests sorted by workflow id."""
    return [WORKFLOW_REGISTRY[key] for key in sorted(WORKFLOW_REGISTRY)]


def get_workflow_manifest(workflow: str) -> WorkflowManifest:
    """Return one workflow manifest, raising KeyError if unsupported."""
    return WORKFLOW_REGISTRY[workflow]


def workflow_allowed_keys() -> dict[str, set[str]]:
    """Return allowed workflow config keys for compatibility callers."""
    return {key: manifest.allowed_keys for key, manifest in WORKFLOW_REGISTRY.items()}


def project_sample_allowed_keys() -> dict[str, set[str]]:
    """Return allowed project sample keys for compatibility callers."""
    return {key: manifest.project_allowed_keys for key, manifest in WORKFLOW_REGISTRY.items()}


def validate_field_value(spec: FieldSpec, value: Any, *, context: str) -> str | None:
    """Return an error message when value does not match the field spec."""
    if value is None:
        return None

    if spec.kind in {"str", "workflow", "backend"}:
        if not isinstance(value, str) or not value.strip():
            return f"{context} '{spec.name}' must be a non-empty string"
    elif spec.kind == "int":
        if not isinstance(value, int) or isinstance(value, bool):
            return f"{context} '{spec.name}' must be an integer"
        if spec.positive and value <= 0:
            return f"{context} '{spec.name}' must be positive"
    elif spec.kind == "number":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return f"{context} '{spec.name}' must be a number"
        if spec.positive and value <= 0:
            return f"{context} '{spec.name}' must be positive"
    elif spec.kind == "bool":
        if not isinstance(value, bool):
            return f"{context} '{spec.name}' must be a boolean"

    return None


def workflow_catalog(workflow: str | None = None) -> list[dict[str, Any]]:
    """Describe configured capabilities; this does not probe tool availability."""
    manifests = [get_workflow_manifest(workflow)] if workflow else list_workflow_manifests()
    return [
        {
            "workflow_id": manifest.workflow_id,
            "display_name": manifest.display_name,
            "display_name_zh": manifest.display_name_zh,
            "supported_inputs": list(manifest.supported_inputs),
            "key_outputs": list(manifest.key_outputs),
            "required_tools": list(manifest.required_tools),
            "supported_profiles": list(manifest.supported_profiles),
            "supported_backends": list(manifest.supported_backends),
            "cli_command": f"bioflow {manifest.workflow_id}",
            "tui_menu_key": manifest.menu_key,
            "example_config": manifest.example_config,
            "project_supported": True,
        }
        for manifest in manifests
    ]

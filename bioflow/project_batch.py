"""BioFlow-CLI 项目级 batch workflow 运行器。"""

from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rich.console import Console

from bioflow import __version__
from bioflow.alignment import run_alignment_pipeline
from bioflow.config import merge_project_sample_defaults
from bioflow.execution import build_execution_context
from bioflow.longread import run_longread_pipeline
from bioflow.pipeline import run_qc_pipeline
from bioflow.preflight import PreflightError
from bioflow.report import (
    collect_summary_data_from_runs,
    generate_report,
    parse_metadata,
    write_summary_json,
    write_summary_tsv,
)
from bioflow.run_layout import read_metadata, utc_now_iso
from bioflow.rnaseq import run_rnaseq_pipeline
from bioflow.search import run_blast_search
from bioflow.variant import run_variant_pipeline

console = Console(stderr=True)

RNASEQ_SAMPLE_METADATA_COLUMNS = (
    "sample_id",
    "group",
    "condition",
    "lane",
    "replicate",
    "status",
    "run_dir",
    "quant_sf",
    "included_in_matrix",
    "error",
)


@dataclass
class ProjectJobResult:
    """单个样本任务的项目级汇总结果。"""

    sample_id: str
    workflow: str
    run_dir: Path
    metadata_path: Path
    status: str
    outputs: dict[str, Any]
    error: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "sample_id": self.sample_id,
            "workflow": self.workflow,
            "run_dir": str(self.run_dir),
            "metadata": str(self.metadata_path),
            "status": self.status,
            "outputs": self.outputs,
            "error": self.error,
        }


@dataclass
class _MetadataHolder:
    """适配 read_metadata 所需的 metadata_path 属性。"""

    metadata_path: Path


def _slugify(value: str) -> str:
    """将 sample_id 转换为稳定目录名。"""
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-") or "sample"


def _job_run_dir(project_root: Path, sample_id: str, workflow: str, index: int) -> Path:
    """返回项目中单个样本任务的运行目录。"""
    slug = _slugify(sample_id)
    return project_root / f"{index:03d}-{slug}-{workflow}"


def _read_run_metadata(run_dir: Path) -> dict[str, Any]:
    """读取单次 workflow 的 metadata。"""
    return read_metadata(_MetadataHolder(metadata_path=run_dir / "metadata.json"))


def _job_failure(sample_id: str, workflow: str, run_dir: Path, error: str) -> ProjectJobResult:
    """构造失败结果。"""
    return ProjectJobResult(
        sample_id=sample_id,
        workflow=workflow,
        run_dir=run_dir,
        metadata_path=run_dir / "metadata.json",
        status="failed",
        outputs={},
        error=error,
    )


def _run_project_job(run_dir: Path, sample: dict[str, Any]) -> ProjectJobResult:
    """执行单个项目样本任务。"""
    workflow = str(sample["workflow"])
    sample_id = str(sample["sample_id"])
    execution = build_execution_context(sample, source="project_config")

    try:
        if workflow == "qc":
            success = run_qc_pipeline(
                Path(sample["input"]) if sample.get("input") else None,
                input_r1=Path(sample["input_r1"]) if sample.get("input_r1") else None,
                input_r2=Path(sample["input_r2"]) if sample.get("input_r2") else None,
                outdir=run_dir,
                adapter=str(sample["adapter"]) if sample.get("adapter") else None,
                minlen=int(sample.get("minlen", 36)),
                resume=bool(sample.get("resume", False)),
                execution=execution,
                cli_mode=True,
            )
            if not success:
                metadata = _read_run_metadata(run_dir)
                return _job_failure(
                    sample_id,
                    workflow,
                    run_dir,
                    str(metadata.get("failure_summary", "qc failed")),
                )

        elif workflow == "align":
            stats = run_alignment_pipeline(
                Path(sample["ref"]),
                Path(sample["input"]) if sample.get("input") else None,
                input_r1=Path(sample["input_r1"]) if sample.get("input_r1") else None,
                input_r2=Path(sample["input_r2"]) if sample.get("input_r2") else None,
                output=Path(sample["output"]) if sample.get("output") else None,
                outdir=run_dir,
                threads=int(sample.get("threads", 1)),
                resume=bool(sample.get("resume", False)),
                execution=execution,
                cli_mode=True,
            )
            if stats is None:
                metadata = _read_run_metadata(run_dir)
                return _job_failure(
                    sample_id,
                    workflow,
                    run_dir,
                    str(metadata.get("failure_summary", "align failed")),
                )

        elif workflow == "search":
            result = run_blast_search(
                Path(sample["db"]),
                Path(sample["query"]),
                output=Path(sample["output"]) if sample.get("output") else None,
                outdir=run_dir,
                evalue=float(sample.get("evalue", 10.0)),
                max_target_seqs=int(sample.get("max_target_seqs", 10)),
                top_n=int(sample.get("top", 5)),
                resume=bool(sample.get("resume", False)),
                execution=execution,
                cli_mode=True,
            )
            if result is None:
                metadata = _read_run_metadata(run_dir)
                return _job_failure(
                    sample_id,
                    workflow,
                    run_dir,
                    str(metadata.get("failure_summary", "search failed")),
                )
        elif workflow == "rnaseq":
            result = run_rnaseq_pipeline(
                Path(sample["transcriptome"]) if sample.get("transcriptome") else None,
                Path(sample["input"]) if sample.get("input") else None,
                index=Path(sample["index"]) if sample.get("index") else None,
                input_r1=Path(sample["input_r1"]) if sample.get("input_r1") else None,
                input_r2=Path(sample["input_r2"]) if sample.get("input_r2") else None,
                outdir=run_dir,
                threads=int(sample.get("threads", 1)),
                library_type=str(sample.get("library_type", "A")),
                sample_id=sample_id,
                group=str(sample["group"]) if sample.get("group") else None,
                condition=str(sample["condition"]) if sample.get("condition") else None,
                lane=str(sample["lane"]) if sample.get("lane") else None,
                replicate=int(sample["replicate"]) if sample.get("replicate") is not None else None,
                resume=bool(sample.get("resume", False)),
                execution=execution,
                cli_mode=True,
            )
            if result is None:
                metadata = _read_run_metadata(run_dir)
                return _job_failure(
                    sample_id,
                    workflow,
                    run_dir,
                    str(metadata.get("failure_summary", "rnaseq failed")),
                )
        elif workflow == "longread":
            result = run_longread_pipeline(
                Path(sample["ref"]),
                Path(sample["input"]),
                output=Path(sample["output"]) if sample.get("output") else None,
                outdir=run_dir,
                preset=str(sample.get("preset", "map-ont")),
                threads=int(sample.get("threads", 1)),
                sample_id=sample_id,
                resume=bool(sample.get("resume", False)),
                execution=execution,
                cli_mode=True,
            )
            if result is None:
                metadata = _read_run_metadata(run_dir)
                return _job_failure(
                    sample_id,
                    workflow,
                    run_dir,
                    str(metadata.get("failure_summary", "longread failed")),
                )
        elif workflow == "variant":
            result = run_variant_pipeline(
                Path(sample["ref"]),
                Path(sample["bam"]),
                output=Path(sample["output"]) if sample.get("output") else None,
                outdir=run_dir,
                caller=str(sample.get("caller", "bcftools")),
                min_qual=float(sample.get("min_qual", 20.0)),
                min_depth=int(sample.get("min_depth", 1)),
                threads=int(sample.get("threads", 1)),
                sample_id=sample_id,
                resume=bool(sample.get("resume", False)),
                execution=execution,
                cli_mode=True,
            )
            if result is None:
                metadata = _read_run_metadata(run_dir)
                return _job_failure(
                    sample_id,
                    workflow,
                    run_dir,
                    str(metadata.get("failure_summary", "variant failed")),
                )
        else:  # pragma: no cover
            return _job_failure(sample_id, workflow, run_dir, f"unsupported workflow: {workflow}")

    except PreflightError as exc:
        return _job_failure(sample_id, workflow, run_dir, str(exc))
    except Exception as exc:
        return _job_failure(sample_id, workflow, run_dir, str(exc))

    metadata = _read_run_metadata(run_dir)
    return ProjectJobResult(
        sample_id=sample_id,
        workflow=workflow,
        run_dir=run_dir,
        metadata_path=run_dir / "metadata.json",
        status=str(metadata.get("status", "unknown")),
        outputs=metadata.get("outputs", {}) if isinstance(metadata.get("outputs"), dict) else {},
        error=str(metadata.get("failure_summary", "")),
    )


def _read_salmon_quant(quant_path: Path) -> dict[str, tuple[float, float]]:
    """Read transcript NumReads and TPM values from one Salmon quant.sf."""
    values: dict[str, tuple[float, float]] = {}
    with quant_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        required = {"Name", "TPM", "NumReads"}
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            raise ValueError("invalid Salmon quant.sf header")
        for row in reader:
            transcript_id = str(row["Name"]).strip()
            if not transcript_id:
                raise ValueError("Salmon quant.sf contains an empty transcript id")
            if transcript_id in values:
                raise ValueError(f"duplicate transcript id in Salmon quant.sf: {transcript_id}")
            num_reads = float(row["NumReads"])
            tpm = float(row["TPM"])
            if not math.isfinite(num_reads) or not math.isfinite(tpm) or num_reads < 0 or tpm < 0:
                raise ValueError(f"invalid Salmon abundance value for transcript: {transcript_id}")
            values[transcript_id] = (num_reads, tpm)
    if not values:
        raise ValueError("Salmon quant.sf contains no transcript rows")
    return values


def _matrix_number(value: float) -> str:
    """Format matrix numbers compactly without losing useful precision."""
    if value.is_integer():
        return str(int(value))
    return format(value, ".10g")


def _write_rnaseq_matrix(
    output_path: Path,
    *,
    sample_ids: list[str],
    transcript_ids: list[str],
    quant_values: dict[str, dict[str, tuple[float, float]]],
    value_index: int,
) -> Path:
    """Write one transcript-by-sample RNA-seq matrix."""
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, dialect="excel-tab", lineterminator="\n")
        writer.writerow(["transcript_id", *sample_ids])
        for transcript_id in transcript_ids:
            writer.writerow(
                [
                    transcript_id,
                    *[
                        _matrix_number(quant_values[sample_id].get(transcript_id, (0.0, 0.0))[value_index])
                        for sample_id in sample_ids
                    ],
                ]
            )
    return output_path


def _write_rnaseq_project_exports(
    project_root: Path,
    *,
    planned_samples: list[dict[str, Any]],
    results: list[ProjectJobResult],
) -> dict[str, Any]:
    """Create project-level RNA-seq matrices, design metadata, and diagnostics."""
    rnaseq_samples = [sample for sample in planned_samples if sample.get("workflow") == "rnaseq"]
    if not rnaseq_samples:
        return {}

    counts_matrix_target = project_root / "counts_matrix.tsv"
    tpm_matrix_target = project_root / "tpm_matrix.tsv"
    for stale_matrix in (counts_matrix_target, tpm_matrix_target):
        stale_matrix.unlink(missing_ok=True)

    results_by_sample = {result.sample_id: result for result in results if result.workflow == "rnaseq"}
    quant_values: dict[str, dict[str, tuple[float, float]]] = {}
    quant_paths: dict[str, Path] = {}
    failed_samples: list[str] = []
    missing_quant_samples: list[str] = []
    not_run_samples: list[str] = []
    sample_errors: dict[str, str] = {}

    for sample in rnaseq_samples:
        sample_id = str(sample["sample_id"])
        result = results_by_sample.get(sample_id)
        if result is None:
            not_run_samples.append(sample_id)
            sample_errors[sample_id] = "sample was not run"
            continue
        if result.status != "success":
            failed_samples.append(sample_id)
            sample_errors[sample_id] = result.error or f"run status: {result.status}"
            continue

        quant_raw = result.outputs.get("quant_sf")
        quant_path = Path(str(quant_raw)) if quant_raw else result.run_dir / "results" / "salmon_quant" / "quant.sf"
        quant_paths[sample_id] = quant_path
        if not quant_path.is_file():
            missing_quant_samples.append(sample_id)
            sample_errors[sample_id] = f"quant.sf not found: {quant_path}"
            continue
        try:
            quant_values[sample_id] = _read_salmon_quant(quant_path)
        except (OSError, TypeError, ValueError) as exc:
            missing_quant_samples.append(sample_id)
            sample_errors[sample_id] = str(exc)

    matrix_sample_ids = [
        str(sample["sample_id"])
        for sample in rnaseq_samples
        if str(sample["sample_id"]) in quant_values
    ]
    transcript_ids = sorted(
        {transcript_id for sample_values in quant_values.values() for transcript_id in sample_values}
    )

    counts_matrix_path: Path | None = None
    tpm_matrix_path: Path | None = None
    if matrix_sample_ids:
        counts_matrix_path = _write_rnaseq_matrix(
            counts_matrix_target,
            sample_ids=matrix_sample_ids,
            transcript_ids=transcript_ids,
            quant_values=quant_values,
            value_index=0,
        )
        tpm_matrix_path = _write_rnaseq_matrix(
            tpm_matrix_target,
            sample_ids=matrix_sample_ids,
            transcript_ids=transcript_ids,
            quant_values=quant_values,
            value_index=1,
        )

    sample_metadata_path = project_root / "sample_metadata.tsv"
    with sample_metadata_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=RNASEQ_SAMPLE_METADATA_COLUMNS,
            dialect="excel-tab",
            lineterminator="\n",
        )
        writer.writeheader()
        for sample in rnaseq_samples:
            sample_id = str(sample["sample_id"])
            result = results_by_sample.get(sample_id)
            writer.writerow(
                {
                    "sample_id": sample_id,
                    "group": sample.get("group", ""),
                    "condition": sample.get("condition", ""),
                    "lane": sample.get("lane", ""),
                    "replicate": sample.get("replicate", ""),
                    "status": result.status if result is not None else "not_run",
                    "run_dir": str(result.run_dir) if result is not None else "",
                    "quant_sf": str(quant_paths.get(sample_id, "")),
                    "included_in_matrix": sample_id in quant_values,
                    "error": sample_errors.get(sample_id, ""),
                }
            )

    group_counts = Counter(str(sample["group"]) for sample in rnaseq_samples if sample.get("group"))
    condition_counts = Counter(
        str(sample["condition"]) for sample in rnaseq_samples if sample.get("condition")
    )
    design_counts: Counter[tuple[str, str]] = Counter(
        (str(sample.get("group", "")), str(sample.get("condition", "")))
        for sample in rnaseq_samples
    )
    return {
        "planned_sample_count": len(rnaseq_samples),
        "successful_sample_count": sum(
            1
            for sample in rnaseq_samples
            if (result := results_by_sample.get(str(sample["sample_id"]))) is not None
            and result.status == "success"
        ),
        "matrix_sample_count": len(matrix_sample_ids),
        "transcript_count": len(transcript_ids),
        "matrix_samples": matrix_sample_ids,
        "failed_samples": failed_samples,
        "missing_quant_samples": missing_quant_samples,
        "not_run_samples": not_run_samples,
        "group_counts": dict(sorted(group_counts.items())),
        "condition_counts": dict(sorted(condition_counts.items())),
        "design_counts": [
            {"group": group, "condition": condition, "sample_count": count}
            for (group, condition), count in sorted(design_counts.items())
        ],
        "counts_matrix": str(counts_matrix_path) if counts_matrix_path is not None else "",
        "tpm_matrix": str(tpm_matrix_path) if tpm_matrix_path is not None else "",
        "sample_metadata": str(sample_metadata_path),
    }


def _write_project_summary(
    project_root: Path,
    *,
    samples: list[ProjectJobResult],
    started_at: str,
    completed_at: str | None = None,
    continue_on_error: bool,
    report_path: Path | None = None,
    planned_sample_count: int,
    summary_json_path: Path | None = None,
    summary_tsv_path: Path | None = None,
    rnaseq: dict[str, Any] | None = None,
) -> Path:
    """写入项目级汇总 JSON。"""
    success_count = sum(1 for item in samples if item.status == "success")
    failed_count = sum(1 for item in samples if item.status != "success")
    payload = {
        "workflow": "project_batch",
        "version": __version__,
        "status": (
            "success"
            if failed_count == 0
            else ("failed" if success_count == 0 else "partial_failed")
        ),
        "started_at": started_at,
        "completed_at": completed_at,
        "project_root": str(project_root),
        "sample_count": len(samples),
        "planned_sample_count": planned_sample_count,
        "success_count": success_count,
        "failed_count": failed_count,
        "continue_on_error": continue_on_error,
        "report": str(report_path) if report_path is not None and report_path.exists() else "",
        "summary_json": str(summary_json_path) if summary_json_path is not None and summary_json_path.exists() else "",
        "summary_tsv": str(summary_tsv_path) if summary_tsv_path is not None and summary_tsv_path.exists() else "",
        "samples": [item.as_dict() for item in samples],
        "workflow_counts": {
            workflow: sum(1 for item in samples if item.workflow == workflow)
            for workflow in sorted({item.workflow for item in samples})
        },
        "rnaseq": rnaseq or {},
    }
    summary_path = project_root / "project_summary.json"
    summary_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary_path


def _resolve_project_root(
    *,
    config_path: Path,
    project_config: dict[str, Any],
    outdir: Path | None,
) -> Path:
    """解析项目输出根目录。"""
    if outdir is not None:
        return outdir
    if project_config.get("outdir"):
        return Path(str(project_config["outdir"]))
    return config_path.parent / "project_run"


def run_project_batch(
    *,
    config_path: Path,
    project_config: dict[str, Any],
    outdir: Path | None = None,
    quiet: bool = False,
) -> dict[str, Any]:
    """执行项目级 batch workflow。"""
    project_root = _resolve_project_root(
        config_path=config_path,
        project_config=project_config,
        outdir=outdir,
    )
    project_root.mkdir(parents=True, exist_ok=True)
    started_at = utc_now_iso()
    continue_on_error = bool(project_config.get("continue_on_error", False))
    report_title = str(project_config.get("report_title") or "BioFlow Project Batch Report")
    planned_sample_count = len(project_config["samples"])

    results: list[ProjectJobResult] = []

    for index, sample in enumerate(project_config["samples"], start=1):
        sample_config = merge_project_sample_defaults(project_config, sample)
        run_dir = _job_run_dir(project_root, str(sample_config["sample_id"]), str(sample_config["workflow"]), index)
        if not quiet:
            console.print(
                f"[bold cyan][Project {index}/{planned_sample_count}][/bold cyan] "
                f"{sample_config['sample_id']} -> {sample_config['workflow']}"
            )

        result = _run_project_job(run_dir, sample_config)
        results.append(result)
        _write_project_summary(
            project_root,
            samples=results,
            started_at=started_at,
            continue_on_error=continue_on_error,
            planned_sample_count=planned_sample_count,
        )

        if result.status != "success" and not continue_on_error:
            break

    rnaseq_exports = _write_rnaseq_project_exports(
        project_root,
        planned_samples=project_config["samples"],
        results=results,
    )
    _write_project_summary(
        project_root,
        samples=results,
        started_at=started_at,
        completed_at=utc_now_iso(),
        continue_on_error=continue_on_error,
        planned_sample_count=planned_sample_count,
        rnaseq=rnaseq_exports,
    )

    report_path: Path | None = None
    if any(item.run_dir.is_dir() and item.metadata_path.exists() for item in results):
        report_path = generate_report(
            project_root,
            project_root / "project_report.html",
            title=report_title,
        )

    completed_at = utc_now_iso()
    summary_json_path: Path | None = None
    summary_tsv_path: Path | None = None
    parsed_runs = []
    for item in results:
        if not (item.run_dir.is_dir() and item.metadata_path.exists()):
            continue
        try:
            parsed_runs.append(parse_metadata(item.run_dir))
        except (FileNotFoundError, ValueError):
            continue
    if parsed_runs:
        aggregate_data = collect_summary_data_from_runs(
            parsed_runs,
            source=project_root,
            project={
                "project_root": str(project_root),
                "config": str(config_path),
                "planned_sample_count": planned_sample_count,
                "continue_on_error": continue_on_error,
            },
        )
        summary_json_path = write_summary_json(aggregate_data, project_root / "summary.json")
        summary_tsv_path = write_summary_tsv(aggregate_data, project_root / "summary.tsv")

    summary_path = _write_project_summary(
        project_root,
        samples=results,
        started_at=started_at,
        completed_at=completed_at,
        continue_on_error=continue_on_error,
        report_path=report_path,
        planned_sample_count=planned_sample_count,
        summary_json_path=summary_json_path,
        summary_tsv_path=summary_tsv_path,
        rnaseq=rnaseq_exports,
    )

    success_count = sum(1 for item in results if item.status == "success")
    failed_count = sum(1 for item in results if item.status != "success")
    status = "success" if failed_count == 0 else ("failed" if success_count == 0 else "partial_failed")

    return {
        "status": status,
        "project_root": str(project_root),
        "config": str(config_path),
        "summary": str(summary_path),
        "summary_json": str(summary_json_path) if summary_json_path is not None else "",
        "summary_tsv": str(summary_tsv_path) if summary_tsv_path is not None else "",
        "report": str(report_path) if report_path is not None else "",
        "sample_count": len(results),
        "planned_sample_count": planned_sample_count,
        "success_count": success_count,
        "failed_count": failed_count,
        "continue_on_error": continue_on_error,
        "rnaseq": rnaseq_exports,
        "samples": [item.as_dict() for item in results],
    }

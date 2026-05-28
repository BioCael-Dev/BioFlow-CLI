"""BioFlow-CLI 项目级 batch workflow 运行器。"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rich.console import Console

from bioflow import __version__
from bioflow.alignment import run_alignment_pipeline
from bioflow.config import merge_project_sample_defaults
from bioflow.pipeline import run_qc_pipeline
from bioflow.preflight import PreflightError
from bioflow.report import generate_report
from bioflow.run_layout import read_metadata, utc_now_iso
from bioflow.search import run_blast_search

console = Console(stderr=True)


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
        else:  # pragma: no cover
            return _job_failure(sample_id, workflow, run_dir, f"unsupported workflow: {workflow}")

    except PreflightError as exc:
        return _job_failure(sample_id, workflow, run_dir, f"missing tools: {', '.join(exc.missing_tools)}")
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


def _write_project_summary(
    project_root: Path,
    *,
    samples: list[ProjectJobResult],
    started_at: str,
    completed_at: str | None = None,
    continue_on_error: bool,
    report_path: Path | None = None,
    planned_sample_count: int,
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
        "samples": [item.as_dict() for item in samples],
        "workflow_counts": {
            workflow: sum(1 for item in samples if item.workflow == workflow)
            for workflow in sorted({item.workflow for item in samples})
        },
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

    report_path: Path | None = None
    if any(item.run_dir.is_dir() and item.metadata_path.exists() for item in results):
        report_path = generate_report(
            project_root,
            project_root / "project_report.html",
            title=report_title,
        )

    completed_at = utc_now_iso()
    summary_path = _write_project_summary(
        project_root,
        samples=results,
        started_at=started_at,
        completed_at=completed_at,
        continue_on_error=continue_on_error,
        report_path=report_path,
        planned_sample_count=planned_sample_count,
    )

    success_count = sum(1 for item in results if item.status == "success")
    failed_count = sum(1 for item in results if item.status != "success")
    status = "success" if failed_count == 0 else ("failed" if success_count == 0 else "partial_failed")

    return {
        "status": status,
        "project_root": str(project_root),
        "config": str(config_path),
        "summary": str(summary_path),
        "report": str(report_path) if report_path is not None else "",
        "sample_count": len(results),
        "planned_sample_count": planned_sample_count,
        "success_count": success_count,
        "failed_count": failed_count,
        "continue_on_error": continue_on_error,
        "samples": [item.as_dict() for item in results],
    }

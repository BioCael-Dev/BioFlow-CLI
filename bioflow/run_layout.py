"""BioFlow-CLI 统一运行目录布局模块。"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bioflow import __version__


@dataclass
class RunLayout:
    workflow: str
    root: Path
    logs_dir: Path
    results_dir: Path
    tmp_dir: Path
    metadata_path: Path
    stderr_log: Path
    stdout_log: Path


def utc_now_iso() -> str:
    """返回 UTC ISO8601 时间字符串。"""
    return datetime.now(timezone.utc).isoformat()


def default_run_root(workflow: str, anchor: Path) -> Path:
    """返回 workflow 的默认运行目录。"""
    return anchor.parent / f"{workflow}_run"


def create_run_layout(workflow: str, anchor: Path, outdir: Path | None = None) -> RunLayout:
    """创建统一运行目录结构。"""
    root = outdir if outdir is not None else default_run_root(workflow, anchor)
    root.mkdir(parents=True, exist_ok=True)
    logs_dir = root / "logs"
    results_dir = root / "results"
    tmp_dir = root / "tmp"
    logs_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    return RunLayout(
        workflow=workflow,
        root=root,
        logs_dir=logs_dir,
        results_dir=results_dir,
        tmp_dir=tmp_dir,
        metadata_path=root / "metadata.json",
        stderr_log=logs_dir / f"{workflow}.stderr.log",
        stdout_log=logs_dir / f"{workflow}.stdout.log",
    )


def resolve_result_path(
    layout: RunLayout,
    output: Path | None,
    default_name: str,
) -> Path:
    """将主要结果文件路径解析到统一布局中。"""
    if output is None:
        return layout.results_dir / default_name
    if output.is_absolute():
        output.parent.mkdir(parents=True, exist_ok=True)
        return output
    return layout.results_dir / output.name


def append_log(path: Path | None, text: str) -> None:
    """向日志文件追加文本。"""
    if path is None or not text:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(text)
        if not text.endswith("\n"):
            handle.write("\n")


def write_metadata(
    layout: RunLayout,
    *,
    status: str,
    command: str,
    parameters: dict[str, Any],
    inputs: dict[str, Any],
    outputs: dict[str, Any],
    started_at: str,
    completed_at: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    """写入统一 metadata.json。"""
    payload: dict[str, Any] = {
        "workflow": layout.workflow,
        "version": __version__,
        "status": status,
        "started_at": started_at,
        "completed_at": completed_at,
        "command": command,
        "parameters": parameters,
        "inputs": inputs,
        "outputs": outputs,
    }
    if extra:
        payload.update(extra)

    layout.metadata_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

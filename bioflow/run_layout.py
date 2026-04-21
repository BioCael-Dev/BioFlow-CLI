"""BioFlow-CLI 统一运行目录布局模块。"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

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


STEP_PENDING = "pending"
STEP_RUNNING = "running"
STEP_SUCCESS = "success"
STEP_FAILED = "failed"
STEP_SKIPPED = "skipped"
STEP_STATUSES = {STEP_PENDING, STEP_RUNNING, STEP_SUCCESS, STEP_FAILED, STEP_SKIPPED}


def _safe_stat(path: Path) -> os.stat_result | None:
    """返回文件 stat，失败时返回 None。"""
    try:
        return path.stat()
    except OSError:
        return None


def utc_now_iso() -> str:
    """返回 UTC ISO8601 时间字符串。"""
    return datetime.now(timezone.utc).isoformat()


def _first_nonempty_line(text: str) -> str:
    """返回文本中的第一条非空行。"""
    for line in text.splitlines():
        line = line.strip()
        if line:
            return line
    return ""


def sha256_file(path: Path) -> str:
    """计算文件 sha256。"""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def describe_path(path: str | Path) -> dict[str, Any]:
    """返回文件或目录的基础描述信息。"""
    resolved = Path(path)
    stat_result = _safe_stat(resolved)
    payload: dict[str, Any] = {
        "path": str(resolved),
        "exists": resolved.exists(),
        "type": "missing",
    }
    if stat_result is None:
        return payload

    if resolved.is_file():
        payload["type"] = "file"
        payload["size_bytes"] = stat_result.st_size
        payload["modified_at"] = datetime.fromtimestamp(
            stat_result.st_mtime,
            tz=timezone.utc,
        ).isoformat()
        try:
            payload["sha256"] = sha256_file(resolved)
        except OSError:
            payload["sha256"] = ""
    elif resolved.is_dir():
        payload["type"] = "directory"
        payload["modified_at"] = datetime.fromtimestamp(
            stat_result.st_mtime,
            tz=timezone.utc,
        ).isoformat()
    return payload


def collect_input_details(paths: dict[str, str | Path]) -> dict[str, dict[str, Any]]:
    """收集输入文件的大小、mtime 和 sha256 等信息。"""
    return {name: describe_path(value) for name, value in paths.items()}


def _capture_command_output(cmd: list[str]) -> str:
    """执行命令并返回首条非空输出，失败时返回空字符串。"""
    try:
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return ""

    combined = "\n".join(part for part in (result.stdout, result.stderr) if part)
    return _first_nonempty_line(combined)


def detect_tool_version(tool: str) -> str:
    """尽力获取工具版本字符串。"""
    executable = shutil.which(tool)
    if not executable:
        return ""

    for candidate in ([executable, "--version"], [executable, "-version"], [executable, "version"]):
        output = _capture_command_output(candidate)
        if output:
            return output
    return ""


def collect_tool_versions(tools: list[str] | tuple[str, ...]) -> dict[str, str]:
    """收集工作流依赖工具版本。"""
    return {tool: detect_tool_version(tool) for tool in tools}


def build_runtime_context() -> dict[str, str]:
    """构建运行环境信息。"""
    return {
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "bioflow_version": __version__,
        "executable": sys.executable,
    }


def read_log_tail(path: Path | None, *, lines: int = 20) -> str:
    """读取日志文件末尾若干行。"""
    if path is None or not path.exists():
        return ""
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return ""
    selected = content.splitlines()[-lines:]
    return "\n".join(selected).strip()


def build_failure_summary(
    step_name: str,
    *,
    stderr_log: Path | None = None,
    fallback: str = "",
) -> str:
    """根据步骤和日志构建失败摘要。"""
    tail = read_log_tail(stderr_log, lines=8)
    if tail:
        return f"{step_name}: {tail}"
    if fallback:
        return f"{step_name}: {fallback}"
    return step_name


def metadata_supports_resume(metadata: dict[str, Any], step_name: str) -> bool:
    """判断 metadata 是否足够支撑 resume 判断。"""
    if not isinstance(metadata, dict):
        return False
    steps = metadata.get("steps")
    if not isinstance(steps, dict):
        return False
    step = steps.get(step_name)
    return isinstance(step, dict) and step.get("status") in {STEP_SUCCESS, STEP_SKIPPED}


def step_resume_ready(
    metadata: dict[str, Any],
    step_name: str,
    *,
    validator: Callable[[], bool],
    required_outputs: tuple[str, ...] = (),
) -> bool:
    """更严格地判断某一步是否可安全 resume。"""
    if not metadata_supports_resume(metadata, step_name):
        return False

    step = metadata["steps"][step_name]
    outputs = step.get("outputs")
    if required_outputs:
        if not isinstance(outputs, dict):
            return False
        for key in required_outputs:
            value = outputs.get(key)
            if value in (None, "", [], {}):
                return False

    return validator()


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


def read_metadata(layout: RunLayout) -> dict[str, Any]:
    """读取 metadata.json，不存在或损坏时返回空字典。"""
    if not layout.metadata_path.exists():
        return {}
    try:
        return json.loads(layout.metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def init_steps(step_names: list[str], existing: dict[str, Any] | None = None) -> dict[str, Any]:
    """初始化步骤状态字典。"""
    steps: dict[str, Any] = {}
    existing_steps = existing if isinstance(existing, dict) else {}
    for step_name in step_names:
        step_payload = existing_steps.get(step_name)
        if isinstance(step_payload, dict):
            step = dict(step_payload)
            if step.get("status") not in STEP_STATUSES:
                step["status"] = STEP_PENDING
            steps[step_name] = step
        else:
            steps[step_name] = {"status": STEP_PENDING}
    return steps


def set_step_state(
    steps: dict[str, Any],
    step_name: str,
    status: str,
    *,
    outputs: dict[str, Any] | None = None,
    note: str | None = None,
    error: str | None = None,
) -> None:
    """更新单个步骤状态。"""
    now = utc_now_iso()
    step = dict(steps.get(step_name, {}))
    step["status"] = status
    step.setdefault("started_at", now)
    if status == STEP_RUNNING:
        step["started_at"] = now
        step.pop("completed_at", None)
        step.pop("error", None)
    else:
        step["completed_at"] = now
    if outputs:
        step["outputs"] = outputs
    if note is not None:
        step["note"] = note
    if error is not None:
        step["error"] = error
    elif status != STEP_FAILED:
        step.pop("error", None)
    steps[step_name] = step


def step_succeeded(steps: dict[str, Any], step_name: str) -> bool:
    """判断步骤在 metadata 中是否标记为成功。"""
    step = steps.get(step_name)
    return isinstance(step, dict) and step.get("status") in {STEP_SUCCESS, STEP_SKIPPED}


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
        "logs": {
            "stdout": str(layout.stdout_log),
            "stderr": str(layout.stderr_log),
        },
        "runtime": build_runtime_context(),
    }
    if extra:
        payload.update(extra)

    layout.metadata_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

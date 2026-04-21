"""BioFlow-CLI run inspection helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _load_metadata(run_dir: Path) -> dict[str, Any]:
    """Load metadata.json from a run directory."""
    meta_path = run_dir / "metadata.json"
    if not meta_path.exists():
        raise FileNotFoundError(f"metadata.json not found in {run_dir}")
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid metadata.json in {run_dir}: {exc}") from exc


def _path_status(path_value: str) -> dict[str, Any]:
    """Return existence info for a path-like value."""
    path = Path(path_value)
    return {
        "path": str(path),
        "exists": path.exists(),
        "type": "directory" if path.is_dir() else "file" if path.is_file() else "missing",
    }


def _collect_output_paths(value: Any, prefix: str = "") -> list[dict[str, Any]]:
    """Flatten output structures into label/path records."""
    items: list[dict[str, Any]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_prefix = f"{prefix}.{key}" if prefix else key
            items.extend(_collect_output_paths(child, child_prefix))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            child_prefix = f"{prefix}[{index}]"
            items.extend(_collect_output_paths(child, child_prefix))
    elif isinstance(value, str) and value:
        entry = {"label": prefix or "output", **_path_status(value)}
        items.append(entry)
    return items


def inspect_run(run_dir: Path) -> dict[str, Any]:
    """Inspect a single BioFlow run directory."""
    metadata = _load_metadata(run_dir)
    steps = metadata.get("steps", {})
    failed_steps: list[dict[str, Any]] = []
    if isinstance(steps, dict):
        for name, info in steps.items():
            if isinstance(info, dict) and info.get("status") == "failed":
                failed_steps.append(
                    {
                        "name": name,
                        "error": info.get("error", ""),
                        "note": info.get("note", ""),
                    }
                )

    logs = metadata.get("logs", {})
    outputs = metadata.get("outputs", {})
    return {
        "run_dir": str(run_dir),
        "metadata": str(run_dir / "metadata.json"),
        "workflow": metadata.get("workflow", "unknown"),
        "version": metadata.get("version", ""),
        "command": metadata.get("command", ""),
        "status": metadata.get("status", "unknown"),
        "started_at": metadata.get("started_at", ""),
        "completed_at": metadata.get("completed_at"),
        "resume_used": bool(metadata.get("resume_used", False)),
        "failure_summary": metadata.get("failure_summary", ""),
        "runtime": metadata.get("runtime", {}),
        "tool_versions": metadata.get("tool_versions", {}),
        "input_details": metadata.get("input_details", {}),
        "logs": {
            "stdout": _path_status(logs["stdout"]) if isinstance(logs, dict) and logs.get("stdout") else None,
            "stderr": _path_status(logs["stderr"]) if isinstance(logs, dict) and logs.get("stderr") else None,
        },
        "critical_outputs": _collect_output_paths(outputs),
        "failed_steps": failed_steps,
        "steps": steps if isinstance(steps, dict) else {},
    }


def render_inspection_text(payload: dict[str, Any]) -> str:
    """Render inspection payload as plain text."""
    lines = [
        f"Run Directory: {payload['run_dir']}",
        f"Workflow: {payload['workflow']}",
        f"Status: {payload['status']}",
        f"Command: {payload['command'] or '-'}",
        f"Version: {payload['version'] or '-'}",
        f"Started: {payload['started_at'] or '-'}",
        f"Completed: {payload['completed_at'] or '-'}",
    ]

    failure_summary = payload.get("failure_summary") or ""
    if failure_summary:
        lines.append(f"Failure Summary: {failure_summary}")

    lines.append("Logs:")
    logs = payload.get("logs", {})
    for key in ("stdout", "stderr"):
        info = logs.get(key)
        if info is None:
            lines.append(f"  {key}: -")
            continue
        status = "ok" if info["exists"] else "missing"
        lines.append(f"  {key}: {info['path']} [{status}]")

    lines.append("Critical Outputs:")
    outputs = payload.get("critical_outputs", [])
    if not outputs:
        lines.append("  -")
    else:
        for item in outputs:
            status = "ok" if item["exists"] else "missing"
            lines.append(f"  {item['label']}: {item['path']} [{status}]")

    lines.append("Failed Steps:")
    failed_steps = payload.get("failed_steps", [])
    if not failed_steps:
        lines.append("  -")
    else:
        for item in failed_steps:
            detail = item.get("error") or item.get("note") or "-"
            lines.append(f"  {item['name']}: {detail}")

    return "\n".join(lines)

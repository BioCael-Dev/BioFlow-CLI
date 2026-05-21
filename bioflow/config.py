"""BioFlow-CLI 工作流配置模块。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


WORKFLOW_ALLOWED_KEYS: dict[str, set[str]] = {
    "qc": {"input", "input_r1", "input_r2", "output", "outdir", "adapter", "minlen", "resume"},
    "align": {"ref", "input", "input_r1", "input_r2", "output", "outdir", "threads", "resume"},
    "search": {"db", "query", "output", "outdir", "evalue", "max_target_seqs", "top", "resume"},
}

PROJECT_ALLOWED_KEYS: set[str] = {"outdir", "continue_on_error", "report_title", "samples"}
PROJECT_SAMPLE_ALLOWED_KEYS: dict[str, set[str]] = {
    "qc": {"sample_id", "workflow", "input", "input_r1", "input_r2", "adapter", "minlen", "resume"},
    "align": {"sample_id", "workflow", "ref", "input", "input_r1", "input_r2", "output", "threads", "resume"},
    "search": {"sample_id", "workflow", "db", "query", "output", "evalue", "max_target_seqs", "top", "resume"},
}


class ConfigError(Exception):
    """配置文件加载或校验失败。"""


def _validate_single_or_paired_inputs(
    data: dict[str, Any],
    workflow: str,
    *,
    context: str,
    require_input: bool = False,
) -> None:
    """校验 workflow 输入组合。"""
    has_single = bool(data.get("input"))
    has_r1 = bool(data.get("input_r1"))
    has_r2 = bool(data.get("input_r2"))
    if has_single and (has_r1 or has_r2):
        raise ConfigError(
            f"{context} cannot mix 'input' with 'input_r1/input_r2'"
        )
    if has_r1 != has_r2:
        raise ConfigError(
            f"{context} paired-end config requires both 'input_r1' and 'input_r2'"
        )
    if require_input and workflow in {"qc", "align"} and not has_single and not (has_r1 and has_r2):
        raise ConfigError(
            f"{context} requires 'input' or both 'input_r1' and 'input_r2'"
        )


def _read_yaml_mapping(config_path: Path) -> dict[str, Any]:
    """读取 YAML 并保证顶层是 mapping。"""
    if not config_path.exists():
        raise ConfigError(f"Config file not found: {config_path}")

    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {config_path}: {exc}") from exc
    except OSError as exc:
        raise ConfigError(f"Failed to read config file {config_path}: {exc}") from exc

    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"Config file must contain a YAML mapping: {config_path}")
    return raw


def load_workflow_config(config_path: Path, workflow: str) -> dict[str, Any]:
    """读取并校验工作流 YAML 配置。

    支持两种格式：
    1. 顶层直接为工作流参数映射
    2. 顶层包含 `qc` / `align` / `search` 分组
    """
    if workflow not in WORKFLOW_ALLOWED_KEYS:
        raise ConfigError(f"Unsupported workflow: {workflow}")

    raw = _read_yaml_mapping(config_path)
    if raw == {}:
        return {}

    if workflow in raw and isinstance(raw[workflow], dict):
        data = raw[workflow]
    else:
        data = raw

    if not isinstance(data, dict):
        raise ConfigError(f"Workflow section '{workflow}' must be a mapping")

    allowed = WORKFLOW_ALLOWED_KEYS[workflow]
    unknown = sorted(key for key in data if key not in allowed)
    if unknown:
        raise ConfigError(
            f"Unknown config keys for {workflow}: {', '.join(unknown)}"
        )

    if workflow in {"qc", "align"}:
        _validate_single_or_paired_inputs(data, workflow, context=f"{workflow} config")

    return dict(data)


def load_project_config(config_path: Path) -> dict[str, Any]:
    """读取并校验项目级 batch YAML 配置。"""
    raw = _read_yaml_mapping(config_path)
    if "project" in raw and isinstance(raw["project"], dict):
        data = raw["project"]
    else:
        data = raw

    if not isinstance(data, dict):
        raise ConfigError("Project config must be a mapping")

    unknown = sorted(key for key in data if key not in PROJECT_ALLOWED_KEYS)
    if unknown:
        raise ConfigError(f"Unknown project config keys: {', '.join(unknown)}")

    if data.get("outdir") is not None and not isinstance(data.get("outdir"), str):
        raise ConfigError("Project config 'outdir' must be a string path")
    if data.get("report_title") is not None and not isinstance(data.get("report_title"), str):
        raise ConfigError("Project config 'report_title' must be a string")
    if data.get("continue_on_error") is not None and not isinstance(data.get("continue_on_error"), bool):
        raise ConfigError("Project config 'continue_on_error' must be a boolean")

    samples = data.get("samples")
    if not isinstance(samples, list) or not samples:
        raise ConfigError("Project config requires a non-empty 'samples' list")

    validated_samples: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, item in enumerate(samples, start=1):
        if not isinstance(item, dict):
            raise ConfigError(f"Project sample #{index} must be a mapping")

        workflow = item.get("workflow")
        sample_id = item.get("sample_id")
        if not isinstance(workflow, str) or workflow not in PROJECT_SAMPLE_ALLOWED_KEYS:
            raise ConfigError(f"Project sample #{index} has unsupported workflow: {workflow}")
        if not isinstance(sample_id, str) or not sample_id.strip():
            raise ConfigError(f"Project sample #{index} requires non-empty sample_id")
        if sample_id in seen_ids:
            raise ConfigError(f"Duplicate sample_id in project config: {sample_id}")
        seen_ids.add(sample_id)

        allowed = PROJECT_SAMPLE_ALLOWED_KEYS[workflow]
        unknown_sample = sorted(key for key in item if key not in allowed)
        if unknown_sample:
            raise ConfigError(
                f"Unknown config keys for project sample '{sample_id}': {', '.join(unknown_sample)}"
            )

        if workflow in {"qc", "align"}:
            _validate_single_or_paired_inputs(
                item,
                workflow,
                context=f"Project sample '{sample_id}'",
                require_input=True,
            )
        if workflow == "align":
            ref = item.get("ref")
            if not isinstance(ref, str) or not ref.strip():
                raise ConfigError(f"Project sample '{sample_id}' requires non-empty ref")
        if workflow == "search":
            db = item.get("db")
            query = item.get("query")
            if not isinstance(db, str) or not db.strip():
                raise ConfigError(f"Project sample '{sample_id}' requires non-empty db")
            if not isinstance(query, str) or not query.strip():
                raise ConfigError(f"Project sample '{sample_id}' requires non-empty query")

        validated_samples.append(dict(item))

    return {
        "outdir": data.get("outdir"),
        "continue_on_error": bool(data.get("continue_on_error", False)),
        "report_title": data.get("report_title"),
        "samples": validated_samples,
    }

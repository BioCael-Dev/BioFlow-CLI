"""BioFlow-CLI 工作流配置模块。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from bioflow.registry import (
    get_workflow_manifest,
    project_sample_allowed_keys,
    validate_field_value,
    workflow_allowed_keys,
)

WORKFLOW_ALLOWED_KEYS: dict[str, set[str]] = workflow_allowed_keys()

PROJECT_ALLOWED_KEYS: set[str] = {
    "outdir", "continue_on_error", "report_title", "profile", "threads", "memory",
    "queue", "time_limit", "backend", "conda_env", "container_image", "samples",
}
PROJECT_SAMPLE_ALLOWED_KEYS: dict[str, set[str]] = project_sample_allowed_keys()

EXECUTION_OPTION_KEYS: tuple[str, ...] = (
    "profile", "threads", "memory", "queue", "time_limit", "backend", "conda_env", "container_image",
)


class ConfigError(Exception):
    """配置文件加载或校验失败。"""


def _validate_manifest_fields(
    data: dict[str, Any],
    workflow: str,
    *,
    context: str,
    project_sample: bool = False,
) -> None:
    """Validate config values using workflow manifest field specs."""
    manifest = get_workflow_manifest(workflow)
    specs = manifest.project_fields if project_sample else manifest.fields
    for key, value in data.items():
        spec = specs.get(key)
        if spec is None:
            continue
        error = validate_field_value(spec, value, context=context)
        if error:
            raise ConfigError(error)


def _validate_execution_options(data: dict[str, Any], *, context: str) -> None:
    """校验运行 profile 与资源参数。"""
    backend = data.get("backend")
    if backend is not None:
        if not isinstance(backend, str) or backend not in {"system", "conda", "container"}:
            raise ConfigError(f"{context} 'backend' must be one of: system, conda, container")

    profile = data.get("profile")
    if profile is not None and (not isinstance(profile, str) or not profile.strip()):
        raise ConfigError(f"{context} 'profile' must be a non-empty string")

    memory = data.get("memory")
    if memory is not None and (not isinstance(memory, str) or not memory.strip()):
        raise ConfigError(f"{context} 'memory' must be a non-empty string")

    queue = data.get("queue")
    if queue is not None and (not isinstance(queue, str) or not queue.strip()):
        raise ConfigError(f"{context} 'queue' must be a non-empty string")

    time_limit = data.get("time_limit")
    if time_limit is not None and (not isinstance(time_limit, str) or not time_limit.strip()):
        raise ConfigError(f"{context} 'time_limit' must be a non-empty string")

    conda_env = data.get("conda_env")
    if conda_env is not None and (not isinstance(conda_env, str) or not conda_env.strip()):
        raise ConfigError(f"{context} 'conda_env' must be a non-empty string")

    container_image = data.get("container_image")
    if container_image is not None and (not isinstance(container_image, str) or not container_image.strip()):
        raise ConfigError(f"{context} 'container_image' must be a non-empty string")

    threads = data.get("threads")
    if threads is not None:
        if not isinstance(threads, int):
            raise ConfigError(f"{context} 'threads' must be an integer")
        if threads <= 0:
            raise ConfigError(f"{context} 'threads' must be positive")

    if backend == "conda" and conda_env is None:
        raise ConfigError(f"{context} backend 'conda' requires 'conda_env'")
    if backend == "container" and container_image is None:
        raise ConfigError(f"{context} backend 'container' requires 'container_image'")


def merge_project_sample_defaults(
    project_config: dict[str, Any],
    sample: dict[str, Any],
) -> dict[str, Any]:
    """将项目级默认执行参数合并到样本级配置。"""
    merged = dict(sample)
    for key in EXECUTION_OPTION_KEYS:
        if merged.get(key) is None and project_config.get(key) is not None:
            merged[key] = project_config[key]
    return merged


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
    _validate_manifest_fields(data, workflow, context=f"{workflow} config")
    _validate_execution_options(data, context=f"{workflow} config")

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
    _validate_execution_options(data, context="Project config")

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

        _validate_execution_options(item, context=f"Project sample '{sample_id}'")
        _validate_manifest_fields(
            item,
            workflow,
            context=f"Project sample '{sample_id}'",
            project_sample=True,
        )

        if workflow in {"qc", "align"}:
            _validate_single_or_paired_inputs(
                item,
                workflow,
                context=f"Project sample '{sample_id}'",
                require_input=True,
            )
        manifest = get_workflow_manifest(workflow)
        required_fields = tuple(
            key
            for key, spec in manifest.project_fields.items()
            if spec.required_for_project and key not in {"sample_id", "workflow"}
        )
        for field in required_fields:
            value = item.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ConfigError(f"Project sample '{sample_id}' requires non-empty {field}")

        validated_samples.append(dict(item))

    return {
        "outdir": data.get("outdir"),
        "continue_on_error": bool(data.get("continue_on_error", False)),
        "report_title": data.get("report_title"),
        "profile": data.get("profile"),
        "threads": data.get("threads"),
        "memory": data.get("memory"),
        "queue": data.get("queue"),
        "time_limit": data.get("time_limit"),
        "backend": data.get("backend"),
        "conda_env": data.get("conda_env"),
        "container_image": data.get("container_image"),
        "samples": validated_samples,
    }

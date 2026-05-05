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


class ConfigError(Exception):
    """配置文件加载或校验失败。"""


def load_workflow_config(config_path: Path, workflow: str) -> dict[str, Any]:
    """读取并校验工作流 YAML 配置。

    支持两种格式：
    1. 顶层直接为工作流参数映射
    2. 顶层包含 `qc` / `align` / `search` 分组
    """
    if workflow not in WORKFLOW_ALLOWED_KEYS:
        raise ConfigError(f"Unsupported workflow: {workflow}")

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
        has_single = bool(data.get("input"))
        has_r1 = bool(data.get("input_r1"))
        has_r2 = bool(data.get("input_r2"))
        if has_single and (has_r1 or has_r2):
            raise ConfigError(
                f"{workflow} config cannot mix 'input' with 'input_r1/input_r2'"
            )
        if has_r1 != has_r2:
            raise ConfigError(
                f"{workflow} paired-end config requires both 'input_r1' and 'input_r2'"
            )

    return dict(data)

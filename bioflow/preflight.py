"""BioFlow-CLI 工具预检模块 — 统一的外部工具依赖检查策略。"""

from __future__ import annotations

import shutil
import sys
from typing import Sequence

from bioflow.env_manager import _check_conda, _check_conda_env, _check_container_runtime
from bioflow.i18n import t


# 工具名称 → (可执行文件名, conda 安装命令)
TOOL_REGISTRY: dict[str, tuple[str, str]] = {
    "fastqc": ("fastqc", "conda install -y -c bioconda fastqc"),
    "trimmomatic": ("trimmomatic", "conda install -y -c bioconda trimmomatic"),
    "samtools": ("samtools", "conda install -y -c bioconda samtools"),
    "bwa": ("bwa", "conda install -y -c bioconda bwa"),
    "makeblastdb": ("makeblastdb", "conda install -y -c bioconda blast"),
    "blastn": ("blastn", "conda install -y -c bioconda blast"),
}


class PreflightError(Exception):
    """工具预检失败时抛出的异常。"""

    def __init__(
        self,
        missing_tools: list[str],
        *,
        backend: str = "system",
        missing_runtime: str | None = None,
        conda_env: str | None = None,
        container_image: str | None = None,
        reason: str = "missing_tools",
    ) -> None:
        self.missing_tools = missing_tools
        self.backend = backend
        self.missing_runtime = missing_runtime
        self.conda_env = conda_env
        self.container_image = container_image
        self.reason = reason
        details: list[str] = []
        if missing_tools:
            details.append(f"tools={', '.join(missing_tools)}")
        if missing_runtime:
            details.append(f"runtime={missing_runtime}")
        if conda_env:
            details.append(f"conda_env={conda_env}")
        if container_image:
            details.append(f"container_image={container_image}")
        suffix = ", ".join(details) if details else "no details"
        super().__init__(f"Preflight failed ({reason}, backend={backend}): {suffix}")


def check_tool(name: str) -> bool:
    """检查单个工具是否可用。"""
    entry = TOOL_REGISTRY.get(name)
    if entry is None:
        return False
    executable, _cmd = entry
    return shutil.which(executable) is not None


def preflight_check(
    tools: Sequence[str],
    *,
    backend: str = "system",
    conda_env: str | None = None,
    container_image: str | None = None,
    cli_mode: bool = False,
) -> bool:
    """检查一组工具是否全部可用。

    Args:
        tools: 需要检查的工具名称列表。
        backend: 执行后端。支持 system / conda / container。
        conda_env: conda 后端目标环境名。
        container_image: container 后端目标镜像名。
        cli_mode: 是否为 CLI 模式。CLI 模式下输出到 stderr 并抛出异常；
                  TUI 模式下使用 rich 打印友好提示。

    Returns:
        True 表示全部工具可用。

    Raises:
        PreflightError: CLI 模式下有缺失工具时抛出。
    """
    missing: list[str] = []
    missing_runtime: str | None = None
    reason = "missing_tools"

    for tool_name in tools:
        if not check_tool(tool_name):
            missing.append(tool_name)

    if backend == "conda":
        if not _check_conda():
            missing_runtime = "conda"
            reason = "missing_runtime"
        elif conda_env and not _check_conda_env(conda_env):
            reason = "missing_conda_env"
    elif backend == "container":
        if _check_container_runtime("docker"):
            missing_runtime = None
        elif _check_container_runtime("apptainer"):
            missing_runtime = None
        else:
            missing_runtime = "docker/apptainer"
            reason = "missing_runtime"
        if reason == "missing_tools" and not container_image:
            reason = "missing_container_image"

    if not missing and missing_runtime is None and reason not in {"missing_conda_env", "missing_container_image"}:
        return True

    if cli_mode:
        # CLI 模式：输出到 stderr + 抛出异常
        for tool_name in missing:
            entry = TOOL_REGISTRY.get(tool_name)
            if entry:
                _exe, install_cmd = entry
                print(
                    t("preflight_missing_cli", tool=tool_name, cmd=install_cmd),
                    file=sys.stderr,
                )
            else:
                print(
                    t("preflight_unknown_tool", tool=tool_name),
                    file=sys.stderr,
                )
        if missing_runtime is not None:
            print(f"Missing runtime for backend '{backend}': {missing_runtime}", file=sys.stderr)
        if reason == "missing_conda_env" and conda_env:
            print(f"Missing conda environment for backend '{backend}': {conda_env}", file=sys.stderr)
        if reason == "missing_container_image":
            print(f"Missing container image for backend '{backend}'", file=sys.stderr)
        raise PreflightError(
            missing,
            backend=backend,
            missing_runtime=missing_runtime,
            conda_env=conda_env,
            container_image=container_image,
            reason=reason,
        )
    else:
        # TUI 模式：使用 rich 打印友好提示
        from rich.console import Console

        console = Console()
        for tool_name in missing:
            entry = TOOL_REGISTRY.get(tool_name)
            if entry:
                _exe, install_cmd = entry
                console.print(
                    t("preflight_missing_tui", tool=tool_name, cmd=install_cmd),
                    style="bold yellow",
                )
            else:
                console.print(
                    t("preflight_unknown_tool", tool=tool_name),
                    style="bold red",
                )
        if missing_runtime is not None:
            console.print(
                f"Missing runtime for backend '{backend}': {missing_runtime}",
                style="bold yellow",
            )
        if reason == "missing_conda_env" and conda_env:
            console.print(
                f"Missing conda environment for backend '{backend}': {conda_env}",
                style="bold yellow",
            )
        if reason == "missing_container_image":
            console.print(
                f"Missing container image for backend '{backend}'",
                style="bold yellow",
            )
        console.print(t("preflight_hint_env_manager"), style="cyan")
        return False

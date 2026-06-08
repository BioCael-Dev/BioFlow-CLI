"""BioFlow-CLI 环境管理模块 — 检测与安装常用生物信息学工具。"""

from __future__ import annotations

import shutil
import subprocess
import sys
from typing import Any

import questionary
from rich.console import Console
from rich.panel import Panel

from bioflow.i18n import t

console = Console()

# Unicode 符号缓存（模块加载时一次性计算）
try:
    "✓".encode(sys.stdout.encoding or "utf-8")
    _SYM_OK, _SYM_FAIL = "✓", "✗"
except (UnicodeEncodeError, LookupError):
    _SYM_OK, _SYM_FAIL = "+", "-"

# 支持的工具列表：(显示名, 可执行文件名, 安装命令)
BIO_TOOLS: list[tuple[str, str, list[str]]] = [
    ("FastQC", "fastqc", ["conda", "install", "-y", "-c", "bioconda", "fastqc"]),
    ("SAMtools", "samtools", ["conda", "install", "-y", "-c", "bioconda", "samtools"]),
    ("BWA", "bwa", ["conda", "install", "-y", "-c", "bioconda", "bwa"]),
    ("BLAST+", "blastn", ["conda", "install", "-y", "-c", "bioconda", "blast"]),
    ("Trimmomatic", "trimmomatic", ["conda", "install", "-y", "-c", "bioconda", "trimmomatic"]),
]


def _check_installed(executable: str) -> bool:
    """检查工具是否已安装。"""
    return shutil.which(executable) is not None


def _check_conda() -> bool:
    """检查 conda 是否可用。"""
    return shutil.which("conda") is not None


def _check_conda_env(env_name: str) -> bool:
    """检查指定 conda 环境是否存在。"""
    if not env_name or not _check_conda():
        return False
    try:
        result = subprocess.run(
            ["conda", "env", "list", "--json"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

    try:
        import json

        payload: dict[str, Any] = json.loads(result.stdout)
    except Exception:
        return False

    envs = payload.get("envs", [])
    if not isinstance(envs, list):
        return False

    for entry in envs:
        if not isinstance(entry, str):
            continue
        if entry.rstrip("/").endswith(f"/{env_name}") or entry.rstrip("\\").endswith(f"\\{env_name}"):
            return True
    return False


def _check_container_runtime(runtime: str) -> bool:
    """检查容器运行时是否可用。"""
    return shutil.which(runtime) is not None


def _run_install(name: str, cmd: list[str]) -> bool:
    """执行安装命令，返回是否成功。"""
    console.print(t("env_installing", tool=name), style="bold cyan")
    try:
        subprocess.run(cmd, check=True)
        console.print(t("env_install_ok", tool=name), style="bold green")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        console.print(t("env_install_fail", tool=name, err=str(exc)), style="bold red")
        return False


def env_menu() -> None:
    """环境管理交互菜单。"""
    console.print(Panel(t("env_title"), style="bold blue"))

    # Conda 预检
    if not _check_conda():
        console.print(t("env_conda_missing"), style="bold red")
        input(t("press_enter"))
        return

    # 使用 questionary.Choice 绑定稳定值，避免字符串解析
    choices = []
    for name, exe, _ in BIO_TOOLS:
        sym = _SYM_OK if _check_installed(exe) else _SYM_FAIL
        choices.append(questionary.Choice(title=f"{sym} {name}", value=name))
    choices.append(questionary.Choice(title=t("env_back"), value="__back__"))

    try:
        answer = questionary.select(t("env_select_tool"), choices=choices).ask()
    except KeyboardInterrupt:
        return

    if answer is None or answer == "__back__":
        return

    # answer 直接是 Choice.value，无需解析
    for name, exe, cmd in BIO_TOOLS:
        if name == answer:
            if _check_installed(exe):
                console.print(t("env_already", tool=name), style="yellow")
            else:
                _run_install(name, cmd)
            break

    input(t("press_enter"))

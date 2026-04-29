"""BioFlow-CLI 质控流程模块 — FastQC → Trimmomatic → FastQC 串联 QC Pipeline。"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Sequence

import questionary
from rich.console import Console
from rich.panel import Panel

from bioflow.i18n import t
from bioflow.preflight import PreflightError, preflight_check
from bioflow.run_layout import (
    STEP_FAILED,
    STEP_RUNNING,
    STEP_SKIPPED,
    STEP_SUCCESS,
    append_log,
    build_failure_details,
    build_failure_summary,
    collect_input_details,
    collect_tool_versions,
    create_run_layout,
    init_steps,
    read_metadata,
    set_step_state,
    step_resume_ready,
    utc_now_iso,
    write_metadata,
)

console = Console()

# QC 流程依赖的工具
QC_REQUIRED_TOOLS = ("fastqc", "trimmomatic")
QC_STEP_FASTQC_PRE = "fastqc_pre"
QC_STEP_TRIM = "trimmomatic"
QC_STEP_FASTQC_POST = "fastqc_post"


def _run_cmd(
    cmd: list[str],
    *,
    description: str = "",
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
) -> bool:
    """执行外部命令，返回是否成功。"""
    if description:
        console.print(f"  → {description}", style="cyan")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        append_log(stdout_log, result.stdout)
        append_log(stderr_log, result.stderr)
        return True
    except subprocess.CalledProcessError as exc:
        append_log(stdout_log, exc.stdout or "")
        append_log(stderr_log, exc.stderr or "")
        console.print(
            t("qc_step_failed", step=description, err=exc.stderr.strip()),
            style="bold red",
        )
        return False
    except FileNotFoundError as exc:
        append_log(stderr_log, str(exc))
        console.print(
            t("qc_step_failed", step=description, err=str(exc)),
            style="bold red",
        )
        return False


def _run_fastqc(
    input_file: Path,
    output_dir: Path,
    *,
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
) -> bool:
    """运行 FastQC 质量检测。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    return _run_cmd(
        ["fastqc", str(input_file), "-o", str(output_dir), "--quiet"],
        description=t("qc_running_fastqc", file=input_file.name),
        stdout_log=stdout_log,
        stderr_log=stderr_log,
    )


def _run_trimmomatic(
    input_file: Path,
    output_file: Path,
    *,
    adapter: str | None = None,
    minlen: int = 36,
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
) -> bool:
    """运行 Trimmomatic 质控修剪。"""
    cmd = [
        "trimmomatic",
        "SE",           # Single-End 模式
        "-phred33",
        str(input_file),
        str(output_file),
    ]

    # 添加修剪步骤
    if adapter:
        cmd.append(f"ILLUMINACLIP:{adapter}:2:30:10")
    cmd.append("LEADING:3")
    cmd.append("TRAILING:3")
    cmd.append("SLIDINGWINDOW:4:15")
    cmd.append(f"MINLEN:{minlen}")

    return _run_cmd(
        cmd,
        description=t("qc_running_trimmomatic", file=input_file.name),
        stdout_log=stdout_log,
        stderr_log=stderr_log,
    )


def _dir_has_outputs(path: Path) -> bool:
    """目录存在且包含至少一个文件。"""
    return path.is_dir() and any(child.is_file() for child in path.iterdir())


def _is_nonempty_file(path: Path) -> bool:
    """文件存在且非空。"""
    return path.is_file() and path.stat().st_size > 0


def run_qc_pipeline(
    input_file: Path,
    *,
    output_dir: Path | None = None,
    outdir: Path | None = None,
    adapter: str | None = None,
    minlen: int = 36,
    resume: bool = False,
    cli_mode: bool = False,
    skip_preflight: bool = False,
) -> bool:
    """执行完整的 QC 串联流程：FastQC → Trimmomatic → FastQC。

    Args:
        input_file: 输入 FASTQ 文件路径。
        output_dir: 运行输出根目录，默认在输入文件同目录下创建 qc_run/。
        adapter: Trimmomatic adapter 文件路径（可选）。
        minlen: Trimmomatic 最短读长阈值，默认 36。
        cli_mode: 是否为 CLI 模式。
        skip_preflight: 是否跳过预检（TUI 模式下菜单入口已做过预检时可跳过）。

    Returns:
        True 表示全部步骤成功。
    """
    # 1. Preflight 检查
    if not skip_preflight:
        if not preflight_check(QC_REQUIRED_TOOLS, cli_mode=cli_mode):
            return False

    # 2. 准备运行目录
    layout = create_run_layout("qc", input_file, outdir=outdir or output_dir)
    started_at = utc_now_iso()
    fastqc_pre_dir = layout.results_dir / "fastqc_pre"
    fastqc_post_dir = layout.results_dir / "fastqc_post"
    trimmed_file = layout.results_dir / f"{input_file.stem}.trimmed{input_file.suffix}"
    existing_metadata = read_metadata(layout)
    tool_versions = collect_tool_versions(QC_REQUIRED_TOOLS)
    input_details = collect_input_details({"input": input_file})
    failure_summary = str(existing_metadata.get("failure_summary", ""))
    failure_details = existing_metadata.get("failure_details", {})
    steps = init_steps(
        [QC_STEP_FASTQC_PRE, QC_STEP_TRIM, QC_STEP_FASTQC_POST],
        existing_metadata.get("steps"),
    )

    def persist(status: str, *, completed_at: str | None = None) -> None:
        write_metadata(
            layout,
            status=status,
            command="qc",
            parameters={"adapter": adapter, "minlen": minlen, "resume": resume},
            inputs={"input": str(input_file)},
            outputs={
                "root": str(layout.root),
                "fastqc_pre": str(fastqc_pre_dir),
                "fastqc_post": str(fastqc_post_dir),
                "trimmed": str(trimmed_file),
            },
            started_at=started_at,
            completed_at=completed_at,
            extra={
                "steps": steps,
                "resume_used": resume,
                "input_details": input_details,
                "tool_versions": tool_versions,
                "failure_summary": failure_summary,
                "failure_details": failure_details,
            },
        )

    persist("running")

    console.print(
        Panel(t("qc_pipeline_start", file=str(input_file)), style="bold magenta")
    )

    # 3. 步骤 1：初始 FastQC
    console.print(t("qc_step_label", step="1/3", name="FastQC"), style="bold blue")
    if resume and step_resume_ready(
        existing_metadata,
        QC_STEP_FASTQC_PRE,
        validator=lambda: _dir_has_outputs(fastqc_pre_dir),
        required_outputs=("dir",),
    ):
        set_step_state(steps, QC_STEP_FASTQC_PRE, STEP_SKIPPED, outputs={"dir": str(fastqc_pre_dir)}, note="reused existing output")
        persist("running")
    else:
        set_step_state(steps, QC_STEP_FASTQC_PRE, STEP_RUNNING)
        persist("running")
        if not _run_fastqc(input_file, fastqc_pre_dir, stdout_log=layout.stdout_log, stderr_log=layout.stderr_log):
            failure_summary = build_failure_summary(QC_STEP_FASTQC_PRE, stderr_log=layout.stderr_log, fallback="FastQC failed")
            failure_details = build_failure_details(
                step_name=QC_STEP_FASTQC_PRE,
                command=f"fastqc {input_file} -o {fastqc_pre_dir} --quiet",
                layout=layout,
                error=failure_summary,
            )
            set_step_state(steps, QC_STEP_FASTQC_PRE, STEP_FAILED, outputs={"dir": str(fastqc_pre_dir)}, error=failure_summary)
            persist("failed", completed_at=utc_now_iso())
            return False
        set_step_state(steps, QC_STEP_FASTQC_PRE, STEP_SUCCESS, outputs={"dir": str(fastqc_pre_dir)})
        persist("running")

    # 4. 步骤 2：Trimmomatic 修剪
    console.print(
        t("qc_step_label", step="2/3", name="Trimmomatic"), style="bold blue"
    )
    if resume and step_resume_ready(
        existing_metadata,
        QC_STEP_TRIM,
        validator=lambda: _is_nonempty_file(trimmed_file),
        required_outputs=("trimmed",),
    ):
        set_step_state(steps, QC_STEP_TRIM, STEP_SKIPPED, outputs={"trimmed": str(trimmed_file)}, note="reused existing output")
        persist("running")
    else:
        set_step_state(steps, QC_STEP_TRIM, STEP_RUNNING)
        persist("running")
        if not _run_trimmomatic(
            input_file,
            trimmed_file,
            adapter=adapter,
            minlen=minlen,
            stdout_log=layout.stdout_log,
            stderr_log=layout.stderr_log,
        ):
            failure_summary = build_failure_summary(QC_STEP_TRIM, stderr_log=layout.stderr_log, fallback="Trimmomatic failed")
            trim_parts = [
                "trimmomatic SE -phred33",
                str(input_file),
                str(trimmed_file),
            ]
            if adapter:
                trim_parts.append(f"ILLUMINACLIP:{adapter}:2:30:10")
            trim_parts.extend(["LEADING:3", "TRAILING:3", "SLIDINGWINDOW:4:15", f"MINLEN:{minlen}"])
            failure_details = build_failure_details(
                step_name=QC_STEP_TRIM,
                command=" ".join(trim_parts),
                layout=layout,
                error=failure_summary,
            )
            set_step_state(steps, QC_STEP_TRIM, STEP_FAILED, outputs={"trimmed": str(trimmed_file)}, error=failure_summary)
            persist("failed", completed_at=utc_now_iso())
            return False
        set_step_state(steps, QC_STEP_TRIM, STEP_SUCCESS, outputs={"trimmed": str(trimmed_file)})
        persist("running")

    # 5. 步骤 3：修剪后 FastQC
    console.print(t("qc_step_label", step="3/3", name="FastQC"), style="bold blue")
    if resume and step_resume_ready(
        existing_metadata,
        QC_STEP_FASTQC_POST,
        validator=lambda: _dir_has_outputs(fastqc_post_dir),
        required_outputs=("dir",),
    ):
        set_step_state(steps, QC_STEP_FASTQC_POST, STEP_SKIPPED, outputs={"dir": str(fastqc_post_dir)}, note="reused existing output")
        persist("running")
    else:
        set_step_state(steps, QC_STEP_FASTQC_POST, STEP_RUNNING)
        persist("running")
        if not _run_fastqc(trimmed_file, fastqc_post_dir, stdout_log=layout.stdout_log, stderr_log=layout.stderr_log):
            failure_summary = build_failure_summary(QC_STEP_FASTQC_POST, stderr_log=layout.stderr_log, fallback="FastQC failed")
            failure_details = build_failure_details(
                step_name=QC_STEP_FASTQC_POST,
                command=f"fastqc {trimmed_file} -o {fastqc_post_dir} --quiet",
                layout=layout,
                error=failure_summary,
            )
            set_step_state(steps, QC_STEP_FASTQC_POST, STEP_FAILED, outputs={"dir": str(fastqc_post_dir)}, error=failure_summary)
            persist("failed", completed_at=utc_now_iso())
            return False
        set_step_state(steps, QC_STEP_FASTQC_POST, STEP_SUCCESS, outputs={"dir": str(fastqc_post_dir)})

    failure_summary = ""
    failure_details = {}
    persist("success", completed_at=utc_now_iso())
    console.print(
        t("qc_pipeline_done", output=str(layout.root)), style="bold green"
    )
    return True


def qc_menu() -> None:
    """质控流程交互菜单（TUI 模式）。"""
    console.print(Panel(t("qc_title"), style="bold magenta"))

    # Preflight 检查
    if not preflight_check(QC_REQUIRED_TOOLS, cli_mode=False):
        input(t("press_enter"))
        return

    # 输入文件
    try:
        input_path = questionary.path(t("qc_input_prompt")).ask()
    except KeyboardInterrupt:
        return
    if not input_path:
        return

    src = Path(input_path)
    if not src.exists():
        console.print(t("seq_file_not_found", path=str(src)), style="bold red")
        input(t("press_enter"))
        return

    # 输出目录
    default_output = src.parent / "qc_run"
    try:
        output_path = questionary.path(
            t("qc_output_prompt"), default=str(default_output)
        ).ask()
    except KeyboardInterrupt:
        return
    if not output_path:
        return
    resume = False
    if (Path(output_path) / "metadata.json").exists():
        try:
            resume = bool(
                questionary.confirm(
                    t("resume_detected_prompt", path=str(output_path)),
                    default=True,
                ).ask()
            )
        except KeyboardInterrupt:
            return

    # Adapter 文件（可选）
    try:
        adapter_path = questionary.text(
            t("qc_adapter_prompt"), default=""
        ).ask()
    except KeyboardInterrupt:
        return

    adapter = adapter_path if adapter_path and Path(adapter_path).exists() else None

    # 最短读长
    minlen_str = questionary.text(t("qc_minlen_prompt"), default="36").ask()
    minlen = int(minlen_str) if minlen_str and minlen_str.isdigit() else 36
    minlen = max(1, minlen)

    # 执行流程
    run_qc_pipeline(
        src,
        output_dir=Path(output_path),
        adapter=adapter,
        minlen=minlen,
        resume=resume,
        cli_mode=False,
        skip_preflight=True,
    )
    input(t("press_enter"))

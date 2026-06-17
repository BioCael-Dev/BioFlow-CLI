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
from bioflow.execution import ResolvedCommand, resolve_command, summarize_commands
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
    command: ResolvedCommand,
    *,
    description: str = "",
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
) -> bool:
    """执行外部命令，返回是否成功。"""
    if description:
        console.print(f"  → {description}", style="cyan")
    try:
        result = subprocess.run(list(command.resolved_command), check=True, capture_output=True, text=True)
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
    execution: dict[str, object] | None = None,
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
) -> bool:
    """运行 FastQC 质量检测。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    command = resolve_command(
        ["fastqc", str(input_file), "-o", str(output_dir), "--quiet"],
        execution,
        path_hints=(input_file, output_dir),
        workdir=output_dir,
    )
    return _run_cmd(
        command,
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
    execution: dict[str, object] | None = None,
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

    command = resolve_command(
        cmd,
        execution,
        path_hints=(input_file, output_file, adapter) if adapter else (input_file, output_file),
        workdir=output_file.parent,
    )
    return _run_cmd(
        command,
        description=t("qc_running_trimmomatic", file=input_file.name),
        stdout_log=stdout_log,
        stderr_log=stderr_log,
    )


def _run_trimmomatic_pe(
    input_r1: Path,
    input_r2: Path,
    output_r1_paired: Path,
    output_r1_unpaired: Path,
    output_r2_paired: Path,
    output_r2_unpaired: Path,
    *,
    adapter: str | None = None,
    minlen: int = 36,
    execution: dict[str, object] | None = None,
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
) -> bool:
    """运行 Trimmomatic 双端修剪。"""
    cmd = [
        "trimmomatic",
        "PE",
        "-phred33",
        str(input_r1),
        str(input_r2),
        str(output_r1_paired),
        str(output_r1_unpaired),
        str(output_r2_paired),
        str(output_r2_unpaired),
    ]
    if adapter:
        cmd.append(f"ILLUMINACLIP:{adapter}:2:30:10")
    cmd.append("LEADING:3")
    cmd.append("TRAILING:3")
    cmd.append("SLIDINGWINDOW:4:15")
    cmd.append(f"MINLEN:{minlen}")
    command = resolve_command(
        cmd,
        execution,
        path_hints=(
            input_r1,
            input_r2,
            output_r1_paired,
            output_r1_unpaired,
            output_r2_paired,
            output_r2_unpaired,
            adapter,
        ) if adapter else (
            input_r1,
            input_r2,
            output_r1_paired,
            output_r1_unpaired,
            output_r2_paired,
            output_r2_unpaired,
        ),
        workdir=output_r1_paired.parent,
    )
    return _run_cmd(
        command,
        description=t("qc_running_trimmomatic", file=f"{input_r1.name} / {input_r2.name}"),
        stdout_log=stdout_log,
        stderr_log=stderr_log,
    )


def _dir_has_outputs(path: Path) -> bool:
    """目录存在且包含至少一个文件。"""
    return path.is_dir() and any(child.is_file() for child in path.iterdir())


def _is_nonempty_file(path: Path) -> bool:
    """文件存在且非空。"""
    return path.is_file() and path.stat().st_size > 0


def _validate_qc_inputs(
    input_file: Path | None,
    input_r1: Path | None,
    input_r2: Path | None,
) -> tuple[bool, Path]:
    """校验 QC 单端/双端输入组合并返回 anchor。"""
    has_single = input_file is not None
    has_r1 = input_r1 is not None
    has_r2 = input_r2 is not None
    if has_single and (has_r1 or has_r2):
        raise ValueError("qc cannot mix input with input_r1/input_r2")
    if has_r1 != has_r2:
        raise ValueError("qc paired-end mode requires both input_r1 and input_r2")
    if not has_single and not (has_r1 and has_r2):
        raise ValueError("qc requires input or input_r1/input_r2")
    anchor = input_file or input_r1
    assert anchor is not None
    return has_r1 and has_r2, anchor


def _fastqc_report_exists(input_file: Path, output_dir: Path) -> bool:
    """FastQC HTML 报告是否存在。"""
    return (output_dir / f"{input_file.stem}_fastqc.html").is_file()


def run_qc_pipeline(
    input_file: Path | None,
    *,
    input_r1: Path | None = None,
    input_r2: Path | None = None,
    output_dir: Path | None = None,
    outdir: Path | None = None,
    adapter: str | None = None,
    minlen: int = 36,
    resume: bool = False,
    execution: dict[str, object] | None = None,
    cli_mode: bool = False,
    skip_preflight: bool = False,
) -> bool:
    """执行完整的 QC 串联流程：FastQC → Trimmomatic → FastQC。

    Args:
        input_file: 单端 FASTQ 输入文件路径。
        output_dir: 运行输出根目录，默认在输入文件同目录下创建 qc_run/。
        adapter: Trimmomatic adapter 文件路径（可选）。
        minlen: Trimmomatic 最短读长阈值，默认 36。
        cli_mode: 是否为 CLI 模式。
        skip_preflight: 是否跳过预检（TUI 模式下菜单入口已做过预检时可跳过）。

    Returns:
        True 表示全部步骤成功。
    """
    execution_payload = execution or {"profile": "local", "backend": "system", "resources": {}, "source": "default"}

    # 1. Preflight 检查
    if not skip_preflight:
        if not preflight_check(
            QC_REQUIRED_TOOLS,
            backend=str(execution_payload.get("backend", "system")),
            conda_env=(
                str(execution_payload["conda_env"])
                if execution_payload.get("conda_env") is not None
                else None
            ),
            container_image=(
                str(execution_payload["container_image"])
                if execution_payload.get("container_image") is not None
                else None
            ),
            cli_mode=cli_mode,
        ):
            return False

    paired_mode, anchor = _validate_qc_inputs(input_file, input_r1, input_r2)

    # 2. 准备运行目录
    layout = create_run_layout("qc", anchor, outdir=outdir or output_dir)
    started_at = utc_now_iso()
    fastqc_pre_dir = layout.results_dir / "fastqc_pre"
    fastqc_post_dir = layout.results_dir / "fastqc_post"
    if paired_mode:
        assert input_r1 is not None and input_r2 is not None
        trimmed_r1 = layout.results_dir / f"{input_r1.stem}.paired{input_r1.suffix}"
        trimmed_r2 = layout.results_dir / f"{input_r2.stem}.paired{input_r2.suffix}"
        unpaired_r1 = layout.results_dir / f"{input_r1.stem}.unpaired{input_r1.suffix}"
        unpaired_r2 = layout.results_dir / f"{input_r2.stem}.unpaired{input_r2.suffix}"
        trimmed_file = trimmed_r1
    else:
        trimmed_file = layout.results_dir / f"{input_file.stem}.trimmed{input_file.suffix}"
    existing_metadata = read_metadata(layout)
    tool_versions = collect_tool_versions(QC_REQUIRED_TOOLS)
    if paired_mode:
        input_details = collect_input_details({"input_r1": input_r1, "input_r2": input_r2})
    else:
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
            parameters={
                "adapter": adapter,
                "minlen": minlen,
                "resume": resume,
                "paired": paired_mode,
                "execution": execution_payload,
            },
            inputs=(
                {"input_r1": str(input_r1), "input_r2": str(input_r2)}
                if paired_mode
                else {"input": str(input_file)}
            ),
            outputs=(
                {
                    "root": str(layout.root),
                    "fastqc_pre": str(fastqc_pre_dir),
                    "fastqc_post": str(fastqc_post_dir),
                    "trimmed_r1": str(trimmed_r1),
                    "trimmed_r2": str(trimmed_r2),
                    "unpaired_r1": str(unpaired_r1),
                    "unpaired_r2": str(unpaired_r2),
                }
                if paired_mode
                else {
                    "root": str(layout.root),
                    "fastqc_pre": str(fastqc_pre_dir),
                    "fastqc_post": str(fastqc_post_dir),
                    "trimmed": str(trimmed_file),
                }
            ),
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
        Panel(t("qc_pipeline_start", file=str(input_r1 if paired_mode else input_file)), style="bold magenta")
    )

    # 3. 步骤 1：初始 FastQC
    console.print(t("qc_step_label", step="1/3", name="FastQC"), style="bold blue")
    if resume and step_resume_ready(
        existing_metadata,
        QC_STEP_FASTQC_PRE,
        validator=(
            (lambda: _fastqc_report_exists(input_r1, fastqc_pre_dir) and _fastqc_report_exists(input_r2, fastqc_pre_dir))
            if paired_mode
            else (lambda: _fastqc_report_exists(input_file, fastqc_pre_dir))
        ),
        required_outputs=("dir",),
        current_execution=execution_payload,
    ):
        set_step_state(steps, QC_STEP_FASTQC_PRE, STEP_SKIPPED, outputs={"dir": str(fastqc_pre_dir)}, note="reused existing output")
        persist("running")
    else:
        pre_fastqc_commands = [
            resolve_command(
                ["fastqc", str(candidate), "-o", str(fastqc_pre_dir), "--quiet"],
                execution_payload,
                path_hints=(candidate, fastqc_pre_dir),
                workdir=fastqc_pre_dir,
            )
            for candidate in ((input_r1, input_r2) if paired_mode else (input_file,))
        ]
        raw_command, resolved_command = summarize_commands(pre_fastqc_commands, separator=" && ")
        set_step_state(steps, QC_STEP_FASTQC_PRE, STEP_RUNNING)
        set_step_state(
            steps,
            QC_STEP_FASTQC_PRE,
            STEP_RUNNING,
            backend=pre_fastqc_commands[0].backend,
            raw_command=raw_command,
            resolved_command=resolved_command,
            environment_fingerprint=pre_fastqc_commands[0].environment_fingerprint,
        )
        persist("running")
        fastqc_ok = (
            _run_fastqc(input_r1, fastqc_pre_dir, execution=execution_payload, stdout_log=layout.stdout_log, stderr_log=layout.stderr_log)
            and _run_fastqc(input_r2, fastqc_pre_dir, execution=execution_payload, stdout_log=layout.stdout_log, stderr_log=layout.stderr_log)
            if paired_mode
            else _run_fastqc(input_file, fastqc_pre_dir, execution=execution_payload, stdout_log=layout.stdout_log, stderr_log=layout.stderr_log)
        )
        if not fastqc_ok:
            failure_summary = build_failure_summary(QC_STEP_FASTQC_PRE, stderr_log=layout.stderr_log, fallback="FastQC failed")
            failure_details = build_failure_details(
                step_name=QC_STEP_FASTQC_PRE,
                command=resolved_command,
                layout=layout,
                error=failure_summary,
            )
            set_step_state(steps, QC_STEP_FASTQC_PRE, STEP_FAILED, outputs={"dir": str(fastqc_pre_dir)}, error=failure_summary)
            persist("failed", completed_at=utc_now_iso())
            return False
        set_step_state(
            steps,
            QC_STEP_FASTQC_PRE,
            STEP_SUCCESS,
            outputs={"dir": str(fastqc_pre_dir)},
            backend=pre_fastqc_commands[0].backend,
            raw_command=raw_command,
            resolved_command=resolved_command,
            environment_fingerprint=pre_fastqc_commands[0].environment_fingerprint,
        )
        persist("running")

    # 4. 步骤 2：Trimmomatic 修剪
    console.print(
        t("qc_step_label", step="2/3", name="Trimmomatic"), style="bold blue"
    )
    if resume and step_resume_ready(
        existing_metadata,
        QC_STEP_TRIM,
        validator=(
            (
                lambda: (
                    _is_nonempty_file(trimmed_r1)
                    and _is_nonempty_file(trimmed_r2)
                    and unpaired_r1.exists()
                    and unpaired_r2.exists()
                )
            )
            if paired_mode
            else (lambda: _is_nonempty_file(trimmed_file))
        ),
        required_outputs=("trimmed_r1", "trimmed_r2", "unpaired_r1", "unpaired_r2") if paired_mode else ("trimmed",),
        current_execution=execution_payload,
    ):
        set_step_state(
            steps,
            QC_STEP_TRIM,
            STEP_SKIPPED,
            outputs=(
                {
                    "trimmed_r1": str(trimmed_r1),
                    "trimmed_r2": str(trimmed_r2),
                    "unpaired_r1": str(unpaired_r1),
                    "unpaired_r2": str(unpaired_r2),
                }
                if paired_mode
                else {"trimmed": str(trimmed_file)}
            ),
            note="reused existing output",
        )
        persist("running")
    else:
        trim_command = resolve_command(
            (
                [
                    "trimmomatic",
                    "PE",
                    "-phred33",
                    str(input_r1),
                    str(input_r2),
                    str(trimmed_r1),
                    str(unpaired_r1),
                    str(trimmed_r2),
                    str(unpaired_r2),
                ]
                if paired_mode
                else [
                    "trimmomatic",
                    "SE",
                    "-phred33",
                    str(input_file),
                    str(trimmed_file),
                ]
            )
            + ([f"ILLUMINACLIP:{adapter}:2:30:10"] if adapter else [])
            + ["LEADING:3", "TRAILING:3", "SLIDINGWINDOW:4:15", f"MINLEN:{minlen}"],
            execution_payload,
            path_hints=(
                input_r1,
                input_r2,
                trimmed_r1,
                unpaired_r1,
                trimmed_r2,
                unpaired_r2,
                adapter,
            ) if paired_mode and adapter else (
                input_r1,
                input_r2,
                trimmed_r1,
                unpaired_r1,
                trimmed_r2,
                unpaired_r2,
            ) if paired_mode else (
                input_file,
                trimmed_file,
                adapter,
            ) if adapter else (
                input_file,
                trimmed_file,
            ),
            workdir=layout.results_dir,
        )
        set_step_state(
            steps,
            QC_STEP_TRIM,
            STEP_RUNNING,
            backend=trim_command.backend,
            raw_command=" ".join(trim_command.raw_command),
            resolved_command=" ".join(trim_command.resolved_command),
            environment_fingerprint=trim_command.environment_fingerprint,
        )
        persist("running")
        trim_ok = (
            _run_trimmomatic_pe(
                input_r1,
                input_r2,
                trimmed_r1,
                unpaired_r1,
                trimmed_r2,
                unpaired_r2,
                adapter=adapter,
                minlen=minlen,
                execution=execution_payload,
                stdout_log=layout.stdout_log,
                stderr_log=layout.stderr_log,
            )
            if paired_mode
            else _run_trimmomatic(
                input_file,
                trimmed_file,
                adapter=adapter,
                minlen=minlen,
                execution=execution_payload,
                stdout_log=layout.stdout_log,
                stderr_log=layout.stderr_log,
            )
        )
        if not trim_ok:
            failure_summary = build_failure_summary(QC_STEP_TRIM, stderr_log=layout.stderr_log, fallback="Trimmomatic failed")
            trim_parts = (
                [
                    "trimmomatic PE -phred33",
                    str(input_r1),
                    str(input_r2),
                    str(trimmed_r1),
                    str(unpaired_r1),
                    str(trimmed_r2),
                    str(unpaired_r2),
                ]
                if paired_mode
                else [
                    "trimmomatic SE -phred33",
                    str(input_file),
                    str(trimmed_file),
                ]
            )
            if adapter:
                trim_parts.append(f"ILLUMINACLIP:{adapter}:2:30:10")
            trim_parts.extend(["LEADING:3", "TRAILING:3", "SLIDINGWINDOW:4:15", f"MINLEN:{minlen}"])
            failure_details = build_failure_details(
                step_name=QC_STEP_TRIM,
                command=" ".join(trim_command.resolved_command),
                layout=layout,
                error=failure_summary,
            )
            set_step_state(
                steps,
                QC_STEP_TRIM,
                STEP_FAILED,
                outputs=(
                    {
                        "trimmed_r1": str(trimmed_r1),
                        "trimmed_r2": str(trimmed_r2),
                        "unpaired_r1": str(unpaired_r1),
                        "unpaired_r2": str(unpaired_r2),
                    }
                    if paired_mode
                    else {"trimmed": str(trimmed_file)}
                ),
                error=failure_summary,
            )
            persist("failed", completed_at=utc_now_iso())
            return False
        set_step_state(
            steps,
            QC_STEP_TRIM,
            STEP_SUCCESS,
            outputs=(
                {
                    "trimmed_r1": str(trimmed_r1),
                    "trimmed_r2": str(trimmed_r2),
                    "unpaired_r1": str(unpaired_r1),
                    "unpaired_r2": str(unpaired_r2),
                }
                if paired_mode
                else {"trimmed": str(trimmed_file)}
            ),
            backend=trim_command.backend,
            raw_command=" ".join(trim_command.raw_command),
            resolved_command=" ".join(trim_command.resolved_command),
            environment_fingerprint=trim_command.environment_fingerprint,
        )
        persist("running")

    # 5. 步骤 3：修剪后 FastQC
    console.print(t("qc_step_label", step="3/3", name="FastQC"), style="bold blue")
    if resume and step_resume_ready(
        existing_metadata,
        QC_STEP_FASTQC_POST,
        validator=(
            (lambda: _fastqc_report_exists(trimmed_r1, fastqc_post_dir) and _fastqc_report_exists(trimmed_r2, fastqc_post_dir))
            if paired_mode
            else (lambda: _fastqc_report_exists(trimmed_file, fastqc_post_dir))
        ),
        required_outputs=("dir",),
        current_execution=execution_payload,
    ):
        set_step_state(steps, QC_STEP_FASTQC_POST, STEP_SKIPPED, outputs={"dir": str(fastqc_post_dir)}, note="reused existing output")
        persist("running")
    else:
        post_fastqc_commands = [
            resolve_command(
                ["fastqc", str(candidate), "-o", str(fastqc_post_dir), "--quiet"],
                execution_payload,
                path_hints=(candidate, fastqc_post_dir),
                workdir=fastqc_post_dir,
            )
            for candidate in ((trimmed_r1, trimmed_r2) if paired_mode else (trimmed_file,))
        ]
        raw_command, resolved_command = summarize_commands(post_fastqc_commands, separator=" && ")
        set_step_state(
            steps,
            QC_STEP_FASTQC_POST,
            STEP_RUNNING,
            backend=post_fastqc_commands[0].backend,
            raw_command=raw_command,
            resolved_command=resolved_command,
            environment_fingerprint=post_fastqc_commands[0].environment_fingerprint,
        )
        persist("running")
        fastqc_post_ok = (
            _run_fastqc(trimmed_r1, fastqc_post_dir, execution=execution_payload, stdout_log=layout.stdout_log, stderr_log=layout.stderr_log)
            and _run_fastqc(trimmed_r2, fastqc_post_dir, execution=execution_payload, stdout_log=layout.stdout_log, stderr_log=layout.stderr_log)
            if paired_mode
            else _run_fastqc(trimmed_file, fastqc_post_dir, execution=execution_payload, stdout_log=layout.stdout_log, stderr_log=layout.stderr_log)
        )
        if not fastqc_post_ok:
            failure_summary = build_failure_summary(QC_STEP_FASTQC_POST, stderr_log=layout.stderr_log, fallback="FastQC failed")
            failure_details = build_failure_details(
                step_name=QC_STEP_FASTQC_POST,
                command=resolved_command,
                layout=layout,
                error=failure_summary,
            )
            set_step_state(steps, QC_STEP_FASTQC_POST, STEP_FAILED, outputs={"dir": str(fastqc_post_dir)}, error=failure_summary)
            persist("failed", completed_at=utc_now_iso())
            return False
        set_step_state(
            steps,
            QC_STEP_FASTQC_POST,
            STEP_SUCCESS,
            outputs={"dir": str(fastqc_post_dir)},
            backend=post_fastqc_commands[0].backend,
            raw_command=raw_command,
            resolved_command=resolved_command,
            environment_fingerprint=post_fastqc_commands[0].environment_fingerprint,
        )

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
        input_mode = questionary.select(
            "Select input mode:",
            choices=["single-end", "paired-end"],
            default="single-end",
        ).ask()
    except KeyboardInterrupt:
        return
    if not input_mode:
        return

    src: Path | None = None
    src_r1: Path | None = None
    src_r2: Path | None = None
    if input_mode == "paired-end":
        try:
            input_r1_path = questionary.path(t("qc_input_r1_prompt")).ask()
            input_r2_path = questionary.path(t("qc_input_r2_prompt")).ask()
        except KeyboardInterrupt:
            return
        if not input_r1_path or not input_r2_path:
            return
        src_r1 = Path(input_r1_path)
        src_r2 = Path(input_r2_path)
        for candidate in (src_r1, src_r2):
            if not candidate.exists():
                console.print(t("seq_file_not_found", path=str(candidate)), style="bold red")
                input(t("press_enter"))
                return
    else:
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
    anchor = src or src_r1
    assert anchor is not None
    default_output = anchor.parent / "qc_run"
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
        input_r1=src_r1,
        input_r2=src_r2,
        output_dir=Path(output_path),
        adapter=adapter,
        minlen=minlen,
        resume=resume,
        cli_mode=False,
        skip_preflight=True,
    )
    input(t("press_enter"))

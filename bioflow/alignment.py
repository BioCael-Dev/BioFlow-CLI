"""BioFlow-CLI 序列比对模块 — BWA + SAMtools 完整比对流程。"""

from __future__ import annotations

import platform
import re
import subprocess
from pathlib import Path

import questionary
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn
from rich.table import Table

from bioflow.i18n import t
from bioflow.preflight import preflight_check
from bioflow.run_layout import (
    append_log,
    create_run_layout,
    resolve_result_path,
    utc_now_iso,
    write_metadata,
)

console = Console()

# 比对流程依赖的工具
ALIGN_REQUIRED_TOOLS = ("bwa", "samtools")


def _print_alignment_failure(description: str, err: str) -> bool:
    """打印统一的比对错误信息。"""
    console.print(
        t("align_step_failed", step=description, err=err),
        style="bold red",
    )
    return False


def _run_cmd(
    cmd: list[str],
    *,
    description: str = "",
    capture: bool = False,
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
) -> subprocess.CompletedProcess | None:
    """执行外部命令，失败时打印错误并返回 None。"""
    if description:
        console.print(f"  → {description}", style="cyan")
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=capture,
            text=True,
        )
        append_log(stdout_log, result.stdout or "")
        append_log(stderr_log, result.stderr or "")
        return result
    except subprocess.CalledProcessError as exc:
        stderr_text = exc.stderr or str(exc)
        append_log(stdout_log, exc.stdout or "")
        append_log(stderr_log, exc.stderr or "")
        _print_alignment_failure(description, stderr_text.strip())
        return None
    except FileNotFoundError as exc:
        append_log(stderr_log, str(exc))
        _print_alignment_failure(description, str(exc))
        return None


def _run_bwa_index(ref: Path, *, stdout_log: Path | None = None, stderr_log: Path | None = None) -> bool:
    """构建 BWA 索引。"""
    result = _run_cmd(
        ["bwa", "index", str(ref)],
        description=t("align_indexing", file=ref.name),
        stdout_log=stdout_log,
        stderr_log=stderr_log,
    )
    return result is not None


def _run_bwa_mem_pipe_sort(
    ref: Path,
    reads: Path,
    output_bam: Path,
    *,
    threads: int = 1,
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
) -> bool:
    """BWA mem → SAMtools view → SAMtools sort 管道。"""
    description = t("align_mapping")
    console.print(f"  → {description}", style="cyan")

    bwa_cmd = ["bwa", "mem", "-t", str(threads), str(ref), str(reads)]
    view_cmd = ["samtools", "view", "-bS", "-@", str(threads), "-"]
    sort_cmd = ["samtools", "sort", "-@", str(threads), "-o", str(output_bam), "-"]

    bwa_proc: subprocess.Popen[str] | None = None
    view_proc: subprocess.Popen[bytes] | None = None
    sort_proc: subprocess.Popen[bytes] | None = None
    try:
        bwa_proc = subprocess.Popen(
            bwa_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        view_proc = subprocess.Popen(
            view_cmd,
            stdin=bwa_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if bwa_proc.stdout is not None:
            bwa_proc.stdout.close()

        sort_proc = subprocess.Popen(
            sort_cmd,
            stdin=view_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if view_proc.stdout is not None:
            view_proc.stdout.close()

        _sort_stdout, sort_stderr = sort_proc.communicate()
        bwa_stderr = ""
        view_stderr = ""
        if bwa_proc.stderr is not None:
            bwa_stderr = bwa_proc.stderr.read()
        if view_proc.stderr is not None:
            view_stderr = view_proc.stderr.read()

        bwa_code = bwa_proc.wait()
        view_code = view_proc.wait()
        sort_code = sort_proc.returncode

        if bwa_code == 0 and view_code == 0 and sort_code == 0:
            append_log(stderr_log, bwa_stderr)
            append_log(stderr_log, view_stderr)
            append_log(stderr_log, sort_stderr.decode("utf-8", errors="replace"))
            return True

        errors = "\n".join(
            part.strip()
            for part in (bwa_stderr, view_stderr, sort_stderr.decode("utf-8", errors="replace"))
            if part and part.strip()
        ) or "pipeline execution failed"
        append_log(stderr_log, errors)
        return _print_alignment_failure(description, errors)
    except FileNotFoundError as exc:
        append_log(stderr_log, str(exc))
        return _print_alignment_failure(description, str(exc))
    finally:
        for proc in (bwa_proc, view_proc, sort_proc):
            if proc is not None and proc.poll() is None:
                proc.kill()

    return False


def _bwa_index_files(ref: Path) -> list[Path]:
    """返回 BWA 索引文件路径列表。"""
    return [ref.with_suffix(ref.suffix + ext) for ext in (".amb", ".ann", ".bwt", ".pac", ".sa")]


def _default_output_bam(reads: Path) -> Path:
    """返回默认输出 BAM 路径。"""
    return reads.parent / f"{reads.stem}.sorted.bam"


def _parse_threads(value: str | None) -> int:
    """解析线程数输入，非法值回退到 1。"""
    if value is None:
        return 1
    try:
        return max(1, int(value.strip()))
    except (TypeError, ValueError):
        return 1


def _format_step_label(step: str, name_key: str) -> str:
    """生成本地化步骤标签。"""
    return t("align_step_label", step=step, name=t(name_key))


def _run_samtools_index(
    bam: Path,
    *,
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
) -> bool:
    """为 BAM 文件创建索引 (.bai)。"""
    result = _run_cmd(
        ["samtools", "index", str(bam)],
        description=t("align_sorting"),
        stdout_log=stdout_log,
        stderr_log=stderr_log,
    )
    return result is not None


def _run_samtools_flagstat(
    bam: Path,
    *,
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
) -> str | None:
    """运行 samtools flagstat 并返回原始输出文本。"""
    description = t("align_flagstat")
    console.print(f"  → {description}", style="cyan")
    try:
        result = subprocess.run(
            ["samtools", "flagstat", str(bam)],
            check=True, capture_output=True, text=True,
        )
        append_log(stdout_log, result.stdout)
        append_log(stderr_log, result.stderr)
        return result.stdout
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        append_log(stderr_log, str(exc))
        _print_alignment_failure(description, str(exc))
        return None


def parse_flagstat(text: str) -> dict[str, int | float]:
    """解析 samtools flagstat 输出为结构化字典。

    典型输出格式：
        12345 + 0 in total (QC-passed reads + QC-failed reads)
        0 + 0 secondary
        0 + 0 supplementary
        0 + 0 duplicates
        11000 + 0 mapped (89.11% : N/A)
    """
    stats: dict[str, int | float] = {
        "total": 0,
        "mapped": 0,
        "unmapped": 0,
        "mapping_rate": 0.0,
        "secondary": 0,
        "supplementary": 0,
        "duplicates": 0,
        "paired": 0,
        "properly_paired": 0,
    }

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        # 提取第一个数字
        match = re.match(r"(\d+)\s*\+\s*\d+\s+(.*)", line)
        if not match:
            continue

        count = int(match.group(1))
        rest = match.group(2).lower()

        if "in total" in rest:
            stats["total"] = count
        elif "secondary" in rest:
            stats["secondary"] = count
        elif "supplementary" in rest:
            stats["supplementary"] = count
        elif "duplicates" in rest:
            stats["duplicates"] = count
        elif "mapped" in rest and "mate mapped" not in rest:
            stats["mapped"] = count
            # 提取映射率
            rate_match = re.search(r"\((\d+\.?\d*)\s*%", rest)
            if rate_match:
                stats["mapping_rate"] = float(rate_match.group(1)) / 100.0
        elif "paired in sequencing" in rest:
            stats["paired"] = count
        elif "properly paired" in rest:
            stats["properly_paired"] = count

    # 计算未比对数
    stats["unmapped"] = max(0, stats["total"] - stats["mapped"])

    # 如果没有从 flagstat 文本中提取到映射率，手动计算
    if stats["mapping_rate"] == 0.0 and stats["total"] > 0:
        stats["mapping_rate"] = stats["mapped"] / stats["total"]

    return stats


def display_alignment_stats(stats: dict[str, int | float]) -> None:
    """使用 rich 渲染比对统计结果。"""
    table = Table(
        title=t("align_stats_title"),
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column(t("align_stats_metric"), style="bold", min_width=20)
    table.add_column(t("align_stats_value"), justify="right", style="magenta", min_width=15)

    total = stats.get("total", 0)
    mapped = stats.get("mapped", 0)
    unmapped = stats.get("unmapped", 0)
    rate = stats.get("mapping_rate", 0.0)

    table.add_row(t("align_stats_total"), f"{total:,}")
    table.add_row(t("align_stats_mapped"), f"{mapped:,}")
    table.add_row(t("align_stats_unmapped"), f"{unmapped:,}")
    table.add_row(t("align_stats_rate"), f"{rate:.2%}")

    if stats.get("secondary", 0) > 0:
        table.add_row(t("align_stats_secondary"), f"{stats['secondary']:,}")
    if stats.get("supplementary", 0) > 0:
        table.add_row(t("align_stats_supplementary"), f"{stats['supplementary']:,}")
    if stats.get("duplicates", 0) > 0:
        table.add_row(t("align_stats_duplicates"), f"{stats['duplicates']:,}")

    console.print(table)

    # Mapping rate 进度条
    if total > 0:
        with Progress(
            TextColumn(f"[bold blue]{t('align_stats_rate')}"),
            BarColumn(bar_width=40),
            TextColumn("[bold]{task.percentage:.1f}%"),
            console=console,
        ) as progress:
            task = progress.add_task("", total=100)
            progress.update(task, completed=rate * 100)


def run_alignment_pipeline(
    ref: Path,
    reads: Path,
    output: Path | None = None,
    *,
    outdir: Path | None = None,
    threads: int = 1,
    cli_mode: bool = False,
    skip_preflight: bool = False,
) -> dict[str, int | float] | None:
    """执行完整的比对流程。

    BWA index → BWA mem + SAMtools view/sort → SAMtools index → flagstat

    Args:
        ref: 参考基因组文件路径。
        reads: 输入 reads 文件路径。
        output: 输出 BAM 文件路径（默认：reads.sorted.bam）。
        threads: 线程数。
        cli_mode: 是否为 CLI 模式。
        skip_preflight: 是否跳过预检。

    Returns:
        比对统计字典，失败时返回 None。
    """
    # 1. Preflight 检查
    if not skip_preflight:
        if not preflight_check(ALIGN_REQUIRED_TOOLS, cli_mode=cli_mode):
            return None

    # Windows 平台提示
    if platform.system() == "Windows":
        console.print(t("align_windows_warn"), style="bold yellow")

    # 2. 准备输出路径
    layout = create_run_layout("align", reads, outdir=outdir)
    started_at = utc_now_iso()
    output = resolve_result_path(layout, output, _default_output_bam(reads).name)
    write_metadata(
        layout,
        status="running",
        command="align",
        parameters={"threads": threads},
        inputs={"ref": str(ref), "reads": str(reads)},
        outputs={"root": str(layout.root), "bam": str(output)},
        started_at=started_at,
    )

    console.print(
        Panel(t("align_pipeline_start", file=str(reads)), style="bold magenta")
    )

    # 3. 步骤 1：BWA 索引（检查是否已存在）
    bwa_index_files = _bwa_index_files(ref)
    if all(f.exists() for f in bwa_index_files):
        console.print(_format_step_label("1/4", "align_step_index_cached"), style="bold blue")
    else:
        console.print(_format_step_label("1/4", "align_step_index"), style="bold blue")
        if not _run_bwa_index(ref, stdout_log=layout.stdout_log, stderr_log=layout.stderr_log):
            write_metadata(
                layout,
                status="failed",
                command="align",
                parameters={"threads": threads},
                inputs={"ref": str(ref), "reads": str(reads)},
                outputs={"root": str(layout.root), "bam": str(output)},
                started_at=started_at,
                completed_at=utc_now_iso(),
            )
            return None

    # 4. 步骤 2：BWA mem + SAMtools view/sort
    console.print(_format_step_label("2/4", "align_step_map_sort"), style="bold blue")
    if not _run_bwa_mem_pipe_sort(
        ref,
        reads,
        output,
        threads=threads,
        stdout_log=layout.stdout_log,
        stderr_log=layout.stderr_log,
    ):
        write_metadata(
            layout,
            status="failed",
            command="align",
            parameters={"threads": threads},
            inputs={"ref": str(ref), "reads": str(reads)},
            outputs={"root": str(layout.root), "bam": str(output)},
            started_at=started_at,
            completed_at=utc_now_iso(),
        )
        return None

    # 5. 步骤 3：SAMtools index
    console.print(_format_step_label("3/4", "align_step_bam_index"), style="bold blue")
    if not _run_samtools_index(output, stdout_log=layout.stdout_log, stderr_log=layout.stderr_log):
        write_metadata(
            layout,
            status="failed",
            command="align",
            parameters={"threads": threads},
            inputs={"ref": str(ref), "reads": str(reads)},
            outputs={"root": str(layout.root), "bam": str(output)},
            started_at=started_at,
            completed_at=utc_now_iso(),
        )
        return None

    # 6. 步骤 4：SAMtools flagstat
    console.print(_format_step_label("4/4", "align_step_flagstat"), style="bold blue")
    flagstat_text = _run_samtools_flagstat(output, stdout_log=layout.stdout_log, stderr_log=layout.stderr_log)
    if flagstat_text is None:
        write_metadata(
            layout,
            status="failed",
            command="align",
            parameters={"threads": threads},
            inputs={"ref": str(ref), "reads": str(reads)},
            outputs={"root": str(layout.root), "bam": str(output)},
            started_at=started_at,
            completed_at=utc_now_iso(),
        )
        return None

    stats = parse_flagstat(flagstat_text)
    flagstat_path = layout.results_dir / f"{output.stem}.flagstat.txt"
    flagstat_path.write_text(flagstat_text, encoding="utf-8")

    # 显示统计
    display_alignment_stats(stats)

    write_metadata(
        layout,
        status="success",
        command="align",
        parameters={"threads": threads},
        inputs={"ref": str(ref), "reads": str(reads)},
        outputs={
            "root": str(layout.root),
            "bam": str(output),
            "flagstat": str(flagstat_path),
        },
        started_at=started_at,
        completed_at=utc_now_iso(),
        extra={"stats": stats},
    )

    console.print(
        t("align_pipeline_done", output=str(layout.root)), style="bold green"
    )
    return stats


def align_menu() -> None:
    """序列比对交互菜单（TUI 模式）。"""
    console.print(Panel(t("align_title"), style="bold magenta"))

    # Preflight 检查
    if not preflight_check(ALIGN_REQUIRED_TOOLS, cli_mode=False):
        input(t("press_enter"))
        return

    # 参考基因组路径
    try:
        ref_path = questionary.path(t("align_ref_prompt")).ask()
    except KeyboardInterrupt:
        return
    if not ref_path:
        return

    ref = Path(ref_path)
    if not ref.exists():
        console.print(t("seq_file_not_found", path=str(ref)), style="bold red")
        input(t("press_enter"))
        return

    # Reads 文件路径
    try:
        reads_path = questionary.path(t("align_input_prompt")).ask()
    except KeyboardInterrupt:
        return
    if not reads_path:
        return

    reads = Path(reads_path)
    if not reads.exists():
        console.print(t("seq_file_not_found", path=str(reads)), style="bold red")
        input(t("press_enter"))
        return

    # 输出 BAM 路径
    default_output = _default_output_bam(reads)
    try:
        output_path = questionary.path(
            t("align_output_prompt"), default=str(default_output)
        ).ask()
    except KeyboardInterrupt:
        return
    if not output_path:
        return

    # 线程数
    threads_str = questionary.text(t("align_threads_prompt"), default="1").ask()
    threads = _parse_threads(threads_str)

    # 执行比对
    run_alignment_pipeline(
        ref,
        reads,
        output=Path(output_path),
        threads=threads,
        cli_mode=False,
        skip_preflight=True,
    )
    input(t("press_enter"))

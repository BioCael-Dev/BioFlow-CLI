"""BioFlow-CLI 序列处理模块 — FASTA/FASTQ 格式化工具。"""

from __future__ import annotations

import os
import re
import tempfile
import time
from collections.abc import Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from contextlib import nullcontext
from pathlib import Path
from typing import TextIO

import questionary
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from bioflow.i18n import t

console = Console()

SUPPORTED_FORMATS = ("fasta", "fastq")


def _parse_fasta(text: str) -> list[tuple[str, str]]:
    """解析 FASTA 文本，返回 (header, sequence) 列表。"""
    records: list[tuple[str, str]] = []
    current_header = ""
    current_seq: list[str] = []

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith(">"):
            if current_header:
                records.append((current_header, "".join(current_seq)))
            current_header = line
            current_seq = []
        else:
            current_seq.append(re.sub(r"\s+", "", line))

    if current_header:
        records.append((current_header, "".join(current_seq)))
    return records


def _detect_sequence_format(text: str) -> str | None:
    """根据首个非空行识别序列格式。"""
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith(">"):
            return "fasta"
        if line.startswith("@"):
            return "fastq"
        return None
    return None


def _parse_fastq(text: str) -> list[tuple[str, str, str, str]]:
    """解析 FASTQ 文本，返回 (header, sequence, plus, quality) 列表。"""
    lines = text.splitlines()
    records: list[tuple[str, str, str, str]] = []
    idx = 0

    while idx < len(lines):
        header = lines[idx].strip()
        if not header:
            idx += 1
            continue
        if not header.startswith("@"):
            return []
        if idx + 3 >= len(lines):
            return []

        seq = re.sub(r"\s+", "", lines[idx + 1].strip())
        plus = lines[idx + 2].strip()
        qual = re.sub(r"\s+", "", lines[idx + 3].strip())
        if not plus.startswith("+"):
            return []
        if not seq or not qual or len(seq) != len(qual):
            return []

        records.append((header, seq, plus, qual))
        idx += 4

    return records


def _wrap_sequence(seq: str, width: int = 80) -> str:
    """将序列按指定宽度换行。"""
    return "\n".join(seq[i : i + width] for i in range(0, len(seq), width))


def _format_fasta(records: list[tuple[str, str]], width: int = 80) -> str:
    """将解析后的记录格式化为标准 FASTA 输出。"""
    lines: list[str] = []
    for header, seq in records:
        lines.append(header)
        lines.append(_wrap_sequence(seq.upper(), width))
    return "\n".join(lines) + "\n"


def _format_fastq(records: list[tuple[str, str, str, str]], width: int = 80) -> str:
    """将解析后的记录格式化为标准 FASTQ 输出。"""
    lines: list[str] = []
    for header, seq, plus, qual in records:
        seq_upper = seq.upper()
        if len(seq_upper) != len(qual):
            raise ValueError("FASTQ sequence and quality length mismatch")
        lines.append(header)
        lines.append(_wrap_sequence(seq_upper, width))
        lines.append(plus)
        lines.append(_wrap_sequence(qual, width))
    return "\n".join(lines) + "\n"


def _fastq_quality_stats(records: list[tuple[str, str, str, str]]) -> dict[str, float]:
    """计算 FASTQ 质量统计（Phred+33）。"""
    total_bases = 0
    total_score = 0
    q20_bases = 0
    q30_bases = 0

    for _header, _seq, _plus, qual in records:
        for ch in qual:
            score = ord(ch) - 33
            total_bases += 1
            total_score += score
            if score >= 20:
                q20_bases += 1
            if score >= 30:
                q30_bases += 1

    if total_bases == 0:
        return {"avg_q": 0.0, "q20_ratio": 0.0, "q30_ratio": 0.0, "bases": 0}

    return {
        "avg_q": total_score / total_bases,
        "q20_ratio": q20_bases / total_bases,
        "q30_ratio": q30_bases / total_bases,
        "bases": float(total_bases),
    }


def _detect_sequence_format_in_handle(handle: TextIO) -> str | None:
    """从文本流中识别序列格式，并在结束后复位到原位置。"""
    start = handle.tell()
    try:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            if line.startswith(">"):
                return "fasta"
            if line.startswith("@"):
                return "fastq"
            return None
        return None
    finally:
        handle.seek(start)


def _iter_fasta_records(handle: TextIO) -> Iterator[tuple[str, str]]:
    """流式解析 FASTA 记录。"""
    current_header: str | None = None
    current_seq: list[str] = []

    for raw_line in handle:
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith(">"):
            if current_header is not None:
                yield current_header, "".join(current_seq)
            current_header = line
            current_seq = []
            continue
        if current_header is None:
            raise ValueError("parse_error")
        current_seq.append(re.sub(r"\s+", "", line))

    if current_header is None:
        raise ValueError("parse_error")

    yield current_header, "".join(current_seq)


def _read_next_nonempty_line(handle: TextIO) -> str | None:
    """读取下一个非空行。"""
    for raw_line in handle:
        if raw_line.strip():
            return raw_line
    return None


def _iter_fastq_records(handle: TextIO) -> Iterator[tuple[str, str, str, str]]:
    """流式解析 FASTQ 记录。"""
    while True:
        header_line = _read_next_nonempty_line(handle)
        if header_line is None:
            return

        header = header_line.strip()
        if not header.startswith("@"):
            raise ValueError("parse_error")

        seq_line = handle.readline()
        plus_line = handle.readline()
        qual_line = handle.readline()

        if not seq_line or not plus_line or not qual_line:
            raise ValueError("parse_error")

        seq = re.sub(r"\s+", "", seq_line.strip())
        plus = plus_line.strip()
        qual = re.sub(r"\s+", "", qual_line.strip())

        if not plus.startswith("+"):
            raise ValueError("parse_error")
        if not seq or not qual or len(seq) != len(qual):
            raise ValueError("parse_error")

        yield header, seq, plus, qual


def _create_fastq_stats() -> dict[str, float]:
    """创建流式 FASTQ 统计容器。"""
    return {
        "total_bases": 0.0,
        "total_score": 0.0,
        "q20_bases": 0.0,
        "q30_bases": 0.0,
    }


def _update_fastq_stats(stats: dict[str, float], qual: str) -> None:
    """更新 FASTQ 质量统计。"""
    for ch in qual:
        score = ord(ch) - 33
        stats["total_bases"] += 1
        stats["total_score"] += score
        if score >= 20:
            stats["q20_bases"] += 1
        if score >= 30:
            stats["q30_bases"] += 1


def _finalize_fastq_stats(stats: dict[str, float]) -> dict[str, float]:
    """将流式质量统计转换为对外结构。"""
    total_bases = stats["total_bases"]
    if total_bases == 0:
        return {"avg_q": 0.0, "q20_ratio": 0.0, "q30_ratio": 0.0, "bases": 0.0}

    return {
        "avg_q": stats["total_score"] / total_bases,
        "q20_ratio": stats["q20_bases"] / total_bases,
        "q30_ratio": stats["q30_bases"] / total_bases,
        "bases": total_bases,
    }


def _stream_format_fasta(
    src_handle: TextIO,
    dst_handle: TextIO,
    width: int,
) -> int:
    """流式格式化 FASTA 并返回记录数。"""
    count = 0
    for header, seq in _iter_fasta_records(src_handle):
        dst_handle.write(f"{header}\n")
        dst_handle.write(_wrap_sequence(seq.upper(), width))
        dst_handle.write("\n")
        count += 1
    return count


def _stream_format_fastq(
    src_handle: TextIO,
    dst_handle: TextIO,
    width: int,
) -> tuple[int, dict[str, float]]:
    """流式格式化 FASTQ 并返回记录数与质量统计。"""
    count = 0
    stats = _create_fastq_stats()
    for header, seq, plus, qual in _iter_fastq_records(src_handle):
        seq_upper = seq.upper()
        dst_handle.write(f"{header}\n")
        dst_handle.write(_wrap_sequence(seq_upper, width))
        dst_handle.write(f"\n{plus}\n")
        dst_handle.write(_wrap_sequence(qual, width))
        dst_handle.write("\n")
        _update_fastq_stats(stats, qual)
        count += 1
    return count, _finalize_fastq_stats(stats)


def format_sequence_file(
    input_path: Path,
    output_path: Path,
    width: int = 80,
) -> tuple[str, int, dict[str, float] | None]:
    """流式格式化单个序列文件并写入目标路径。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None

    with input_path.open("r", encoding="utf-8") as src_handle:
        seq_format = _detect_sequence_format_in_handle(src_handle)
        if seq_format not in SUPPORTED_FORMATS:
            raise ValueError("invalid_format")

        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=output_path.parent,
            prefix=f".{output_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as tmp_handle:
            temp_path = Path(tmp_handle.name)
            try:
                if seq_format == "fasta":
                    count = _stream_format_fasta(src_handle, tmp_handle, width)
                    stats = None
                else:
                    count, stats = _stream_format_fastq(src_handle, tmp_handle, width)
            except Exception:
                tmp_handle.close()
                temp_path.unlink(missing_ok=True)
                raise

    if temp_path is None:
        raise ValueError("runtime_error")

    temp_path.replace(output_path)
    return seq_format, count, stats


def seq_menu() -> None:
    """序列格式化交互菜单。"""
    console.print(Panel(t("seq_title"), style="bold magenta"))

    try:
        input_path = questionary.path(t("seq_input_prompt")).ask()
    except KeyboardInterrupt:
        return
    if not input_path:
        return

    src = Path(input_path)
    if not src.exists():
        console.print(t("seq_file_not_found", path=str(src)), style="bold red")
        input(t("press_enter"))
        return

    default_suffix = src.suffix if src.suffix else ".fasta"
    default_output = src.with_name(f"{src.stem}.formatted{default_suffix}")
    try:
        output_path = questionary.path(
            t("seq_output_prompt"), default=str(default_output)
        ).ask()
    except KeyboardInterrupt:
        return
    if not output_path:
        return

    width_str = questionary.text(t("seq_wrap_prompt"), default="80").ask()
    width = int(width_str) if width_str and width_str.isdigit() else 80
    width = max(1, width)  # 钳位，防止 range step=0 崩溃

    console.print(t("seq_processing"), style="cyan")
    try:
        _seq_format, count, fastq_stats = format_sequence_file(
            src,
            Path(output_path),
            width,
        )
    except ValueError:
        console.print(t("seq_invalid_format"), style="bold red")
        input(t("press_enter"))
        return

    console.print(t("seq_done", count=count, path=output_path), style="bold green")
    if fastq_stats:
        console.print(
            t(
                "seq_fastq_stats",
                avg_q=f"{fastq_stats['avg_q']:.2f}",
                q20=f"{fastq_stats['q20_ratio']:.1%}",
                q30=f"{fastq_stats['q30_ratio']:.1%}",
                bases=int(fastq_stats["bases"]),
            ),
            style="bold cyan",
        )
    input(t("press_enter"))


def _make_unique_output_path(
    file_path: Path,
    input_dir: Path,
    output_dir: Path,
    recursive: bool,
    seen: set[str],
) -> Path:
    """生成唯一的输出文件路径，递归模式下加入相对路径前缀避免冲突。"""
    if recursive:
        try:
            rel = file_path.relative_to(input_dir)
            if rel.parent != Path("."):
                prefix = str(rel.parent).replace("/", "__").replace("\\", "__")
                name = f"{prefix}__{file_path.stem}.formatted{file_path.suffix}"
            else:
                name = f"{file_path.stem}.formatted{file_path.suffix}"
        except ValueError:
            name = f"{file_path.stem}.formatted{file_path.suffix}"
    else:
        name = f"{file_path.stem}.formatted{file_path.suffix}"

    # 冲突检测：追加序号
    base_name = name
    counter = 1
    while name in seen:
        stem, suffix = base_name.rsplit(".", 1) if "." in base_name else (base_name, "")
        name = f"{stem}_{counter}.{suffix}" if suffix else f"{stem}_{counter}"
        counter += 1
    seen.add(name)
    return output_dir / name


def _process_single_file(
    file_path: Path,
    output_path: Path,
    width: int,
) -> tuple[str, int]:
    """处理单个序列文件，返回 (格式化后的格式类型, 序列数)。

    Raises:
        ValueError: 格式不支持或解析失败。
    """
    try:
        seq_format, count, _stats = format_sequence_file(file_path, output_path, width)
    except ValueError as exc:
        if str(exc) == "invalid_format":
            with file_path.open("r", encoding="utf-8") as handle:
                detected = _detect_sequence_format_in_handle(handle)
            if detected not in SUPPORTED_FORMATS:
                raise ValueError("unsupported_format") from exc
        raise ValueError("parse_error") from exc
    return seq_format, count


def _run_batch_job(
    index: int,
    file_path_str: str,
    output_path_str: str,
    width: int,
) -> dict[str, int | float | str]:
    """子进程/主进程通用的单文件批处理任务。"""
    start_time = time.time()
    file_path = Path(file_path_str)
    output_path = Path(output_path_str)

    try:
        _seq_format, count = _process_single_file(file_path, output_path, width)
        return {
            "index": index,
            "kind": "success",
            "file": file_path.name,
            "sequences": count,
            "output": output_path.name,
            "time": time.time() - start_time,
        }
    except ValueError as exc:
        reason = str(exc)
        if reason == "unsupported_format":
            return {
                "index": index,
                "kind": "skipped",
                "file": file_path.name,
                "reason": "unsupported_format",
                "time": 0.0,
            }
        return {
            "index": index,
            "kind": "failed",
            "file": file_path.name,
            "error": reason,
            "time": time.time() - start_time,
        }
    except Exception as exc:
        return {
            "index": index,
            "kind": "failed",
            "file": file_path.name,
            "error": str(exc),
            "time": time.time() - start_time,
        }


def _append_batch_result(results: dict[str, list[dict]], item: dict[str, int | float | str]) -> None:
    """将单文件结果写入聚合结构。"""
    kind = item.pop("kind")
    item.pop("index", None)
    if kind == "success":
        results["success"].append(item)
    elif kind == "skipped":
        results["skipped"].append(item)
    else:
        results["failed"].append(item)


def _normalize_workers(workers: int | None) -> int:
    """规范化并发数，非法值回退到 1。"""
    if workers is None:
        return 1
    try:
        return max(1, int(workers))
    except (TypeError, ValueError):
        return 1


def batch_format_sequences(
    input_dir: Path,
    output_dir: Path,
    pattern: str = "*.fasta",
    recursive: bool = False,
    width: int = 80,
    continue_on_error: bool = True,
    quiet: bool = False,
    workers: int = 1,
) -> dict[str, list[dict]]:
    """批量格式化序列文件。

    Args:
        input_dir: 输入目录
        output_dir: 输出目录
        pattern: 文件匹配模式（如 *.fasta, *.fa, *.fastq）
        recursive: 是否递归扫描子目录
        width: 序列换行宽度
        continue_on_error: 遇到错误是否继续处理
        quiet: 静默模式（不显示进度）
        workers: 并发进程数，1 表示串行处理

    Returns:
        包含 success/failed/skipped 列表的字典
    """
    # 收集文件
    if recursive:
        files = sorted(input_dir.rglob(pattern))
    else:
        files = sorted(input_dir.glob(pattern))

    results: dict[str, list[dict]] = {
        "success": [],
        "failed": [],
        "skipped": [],
    }

    if not files:
        return results

    # 确保输出目录存在
    output_dir.mkdir(parents=True, exist_ok=True)
    seen_names: set[str] = set()

    workers = _normalize_workers(workers)
    jobs: list[dict[str, int | str]] = []
    skipped_items: list[dict[str, int | float | str]] = []
    for index, file_path in enumerate(files):
        if not file_path.is_file():
            skipped_items.append({
                "index": index,
                "kind": "skipped",
                "file": file_path.name,
                "reason": "unsupported_format",
                "time": 0.0,
            })
            continue
        out_path = _make_unique_output_path(
            file_path, input_dir, output_dir, recursive, seen_names
        )
        jobs.append({
            "index": index,
            "file_path": str(file_path),
            "output_path": str(out_path),
        })

    completed_items: list[dict[str, int | float | str]] = []
    completed_items.extend(skipped_items)

    progress_cm: Progress | None = None
    if not quiet:
        progress_cm = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
        )

    with progress_cm or nullcontext() as progress:
        task_id = None
        if progress is not None:
            task_id = progress.add_task(t("batch_processing"), total=len(files))
            if skipped_items:
                progress.advance(task_id, advance=len(skipped_items))

        if workers == 1 or len(jobs) <= 1:
            for job in jobs:
                item = _run_batch_job(job["index"], job["file_path"], job["output_path"], width)
                completed_items.append(item)
                if progress is not None and task_id is not None:
                    progress.advance(task_id)
                if item["kind"] == "failed" and not continue_on_error:
                    break
        else:
            max_workers = min(workers, len(jobs), os.cpu_count() or workers)
            executor = ProcessPoolExecutor(max_workers=max_workers)
            try:
                job_iter = iter(jobs)
                pending: dict[Future, dict[str, int | str]] = {}

                for _ in range(max_workers):
                    try:
                        job = next(job_iter)
                    except StopIteration:
                        break
                    future = executor.submit(
                        _run_batch_job,
                        job["index"],
                        job["file_path"],
                        job["output_path"],
                        width,
                    )
                    pending[future] = job

                stop_early = False
                while pending:
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        pending.pop(future, None)
                        item = future.result()
                        completed_items.append(item)
                        if progress is not None and task_id is not None:
                            progress.advance(task_id)
                        if item["kind"] == "failed" and not continue_on_error:
                            stop_early = True
                            break

                        try:
                            job = next(job_iter)
                        except StopIteration:
                            continue
                        new_future = executor.submit(
                            _run_batch_job,
                            job["index"],
                            job["file_path"],
                            job["output_path"],
                            width,
                        )
                        pending[new_future] = job

                    if stop_early:
                        for future in pending:
                            future.cancel()
                        break
            finally:
                executor.shutdown(wait=True, cancel_futures=True)

    for item in sorted(completed_items, key=lambda entry: int(entry["index"])):
        _append_batch_result(results, item)

    return results


def display_batch_results(results: dict[str, list[dict]]) -> None:
    """显示批量处理结果表格。"""
    total = len(results["success"]) + len(results["failed"]) + len(results["skipped"])

    if total == 0:
        console.print(t("batch_no_files"), style="yellow")
        return

    # 成功表格
    if results["success"]:
        table = Table(title=t("batch_success_title"), show_header=True, header_style="bold green")
        table.add_column(t("batch_col_file"), style="cyan")
        table.add_column(t("batch_col_sequences"), justify="right", style="magenta")
        table.add_column(t("batch_col_output"), style="blue")
        table.add_column(t("batch_col_time"), justify="right", style="yellow")

        for item in results["success"]:
            table.add_row(
                item["file"],
                str(item["sequences"]),
                item["output"],
                f"{item['time']:.2f}s",
            )

        console.print(table)

    # 失败表格
    if results["failed"]:
        table = Table(title=t("batch_failed_title"), show_header=True, header_style="bold red")
        table.add_column(t("batch_col_file"), style="cyan")
        table.add_column(t("batch_col_error"), style="red")
        table.add_column(t("batch_col_time"), justify="right", style="yellow")

        for item in results["failed"]:
            table.add_row(
                item["file"],
                item["error"],
                f"{item['time']:.2f}s",
            )

        console.print(table)

    # 跳过表格
    if results["skipped"]:
        table = Table(title=t("batch_skipped_title"), show_header=True, header_style="bold yellow")
        table.add_column(t("batch_col_file"), style="cyan")
        table.add_column(t("batch_col_reason"), style="yellow")

        for item in results["skipped"]:
            table.add_row(item["file"], item["reason"])

        console.print(table)

    # 统计摘要
    console.print(
        t(
            "batch_summary",
            total=total,
            success=len(results["success"]),
            failed=len(results["failed"]),
            skipped=len(results["skipped"]),
        ),
        style="bold cyan",
    )

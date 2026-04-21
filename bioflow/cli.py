#!/usr/bin/env python3
"""BioFlow-CLI 非交互式命令行接口。"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any

from rich.console import Console

from bioflow import __version__
from bioflow.bio_tasks import (
    batch_format_sequences,
    display_batch_results,
    format_sequence_file,
)
from bioflow.env_manager import BIO_TOOLS, _check_conda, _check_installed
from bioflow.alignment import run_alignment_pipeline
from bioflow.config import ConfigError, load_workflow_config
from bioflow.i18n import init_language, t
from bioflow.inspect import inspect_run, render_inspection_text
from bioflow.pipeline import run_qc_pipeline
from bioflow.preflight import PreflightError
from bioflow.report import generate_report
from bioflow.search import run_blast_search

# 退出码标准
EXIT_SUCCESS = 0
EXIT_RUNTIME_ERROR = 1
EXIT_ARGUMENT_ERROR = 2
EXIT_DEPENDENCY_MISSING = 3

# stderr 专用 console（进度、警告、错误）
console_err = Console(stderr=True)
# stdout 专用 console（结果输出）
console_out = Console(stderr=False)

# Unicode 符号缓存（模块加载时一次性计算，与 env_manager 保持一致）
try:
    "✓".encode(sys.stdout.encoding or "utf-8")
    _SYM_OK, _SYM_FAIL = "✓", "✗"
except (UnicodeEncodeError, LookupError):
    _SYM_OK, _SYM_FAIL = "+", "-"


def _setup_logging(quiet: bool = False) -> None:
    """配置日志系统。"""
    root = logging.getLogger("bioflow")
    if not root.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        root.addHandler(handler)
        root.setLevel(logging.ERROR if quiet else logging.WARNING)


def _resolve_config_path(config_value: str | None) -> Path | None:
    """将 CLI 传入的配置路径转换为 Path。"""
    if not config_value:
        return None
    return Path(config_value)


def _default_workflow_outdir(workflow: str, anchor: Path) -> Path:
    """返回工作流默认运行目录。"""
    return anchor.parent / f"{workflow}_run"


def _resolve_align_json_output(input_path: Path, output_path: Path | None, outdir: Path | None) -> Path:
    """返回 align JSON 模式下展示的主输出 BAM 路径。"""
    if output_path is None:
        return (outdir or _default_workflow_outdir("align", input_path)) / "results" / f"{input_path.stem}.sorted.bam"
    if output_path.is_absolute():
        return output_path
    return (outdir or _default_workflow_outdir("align", input_path)) / "results" / output_path.name


def _merge_workflow_args(
    args: argparse.Namespace,
    workflow: str,
    defaults: dict[str, Any],
) -> dict[str, Any]:
    """按 CLI > config > default 合并工作流参数。"""
    merged = dict(defaults)
    config_path = _resolve_config_path(getattr(args, "config", None))
    if config_path is not None:
        config_data = load_workflow_config(config_path, workflow)
        merged.update(config_data)

    for key in defaults:
        if hasattr(args, key):
            value = getattr(args, key)
            if value is not None:
                merged[key] = value

    if config_path is not None:
        merged["config"] = str(config_path)
    return merged


def cmd_seq(args: argparse.Namespace) -> int:
    """处理 seq 子命令：FASTA/FASTQ 格式化。"""
    input_path = Path(args.input)
    default_suffix = input_path.suffix if input_path.suffix else ".fasta"
    default_output = input_path.with_name(f"{input_path.stem}.formatted{default_suffix}")
    output_path = Path(args.output) if args.output else default_output
    width = args.width

    # JSON 模式自动启用 quiet
    quiet = args.quiet or args.json

    # 参数校验
    if not input_path.exists():
        if args.json:
            print(json.dumps({"error": "file_not_found", "path": str(input_path)}, ensure_ascii=False))
        else:
            console_err.print(t("seq_file_not_found", path=str(input_path)), style="bold red")
        return EXIT_ARGUMENT_ERROR

    if width <= 0:
        if args.json:
            print(json.dumps({"error": "invalid_width", "width": width}, ensure_ascii=False))
        else:
            console_err.print(f"Error: width must be positive (got {width})", style="bold red")
        return EXIT_ARGUMENT_ERROR

    # 读取和解析
    try:
        if not quiet:
            console_err.print(t("seq_processing"), style="cyan")

        try:
            seq_format, count, fastq_stats = format_sequence_file(
                input_path,
                output_path,
                width,
            )
        except ValueError:
            if args.json:
                print(
                    json.dumps(
                        {"error": "invalid_format", "path": str(input_path)},
                        ensure_ascii=False,
                    )
                )
            else:
                console_err.print(t("seq_invalid_format"), style="bold red")
            return EXIT_RUNTIME_ERROR

        # 输出结果
        if args.json:
            payload: dict[str, Any] = {
                "status": "success",
                "input": str(input_path),
                "output": str(output_path),
                "format": seq_format,
                "records": count,
                "width": width,
            }
            if fastq_stats:
                payload["quality"] = {
                    "avg_q": round(fastq_stats["avg_q"], 4),
                    "q20_ratio": round(fastq_stats["q20_ratio"], 6),
                    "q30_ratio": round(fastq_stats["q30_ratio"], 6),
                    "bases": int(fastq_stats["bases"]),
                }
            result = json.dumps(payload, ensure_ascii=False)
            # 直接使用 print 避免 rich 的自动换行
            print(result)
        else:
            if not quiet:
                console_err.print(
                    t("seq_done", count=count, path=str(output_path)),
                    style="bold green"
                )
            if fastq_stats:
                console_out.print(
                    t(
                        "seq_fastq_stats",
                        avg_q=f"{fastq_stats['avg_q']:.2f}",
                        q20=f"{fastq_stats['q20_ratio']:.1%}",
                        q30=f"{fastq_stats['q30_ratio']:.1%}",
                        bases=int(fastq_stats["bases"]),
                    )
                )

        return EXIT_SUCCESS

    except Exception as exc:
        if args.json:
            print(json.dumps({"error": "runtime_error", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(t("error_unexpected", err=str(exc)), style="bold red")
        return EXIT_RUNTIME_ERROR


def cmd_batch(args: argparse.Namespace) -> int:
    """处理 batch 子命令：批量格式化序列文件。"""
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir) if args.output_dir else Path("./formatted_output")
    pattern = args.pattern
    recursive = args.recursive
    width = args.width
    workers = args.workers
    continue_on_error = args.continue_on_error
    quiet = args.quiet or args.json

    # 参数校验
    if not input_dir.exists():
        if args.json:
            print(json.dumps({"error": "directory_not_found", "path": str(input_dir)}, ensure_ascii=False))
        else:
            console_err.print(f"Error: directory not found: {input_dir}", style="bold red")
        return EXIT_ARGUMENT_ERROR

    if not input_dir.is_dir():
        if args.json:
            print(json.dumps({"error": "not_a_directory", "path": str(input_dir)}, ensure_ascii=False))
        else:
            console_err.print(f"Error: not a directory: {input_dir}", style="bold red")
        return EXIT_ARGUMENT_ERROR

    if width <= 0:
        if args.json:
            print(json.dumps({"error": "invalid_width", "width": width}, ensure_ascii=False))
        else:
            console_err.print(f"Error: width must be positive (got {width})", style="bold red")
        return EXIT_ARGUMENT_ERROR

    if workers <= 0:
        if args.json:
            print(json.dumps({"error": "invalid_workers", "workers": workers}, ensure_ascii=False))
        else:
            console_err.print(f"Error: workers must be positive (got {workers})", style="bold red")
        return EXIT_ARGUMENT_ERROR

    try:
        # 执行批量处理
        results = batch_format_sequences(
            input_dir=input_dir,
            output_dir=output_dir,
            pattern=pattern,
            recursive=recursive,
            width=width,
            continue_on_error=continue_on_error,
            quiet=quiet,
            workers=workers,
        )

        # 输出结果
        if args.json:
            payload = {
                "status": "success",
                "input_dir": str(input_dir),
                "output_dir": str(output_dir),
                "pattern": pattern,
                "recursive": recursive,
                "width": width,
                "workers": workers,
                "results": {
                    "success": results["success"],
                    "failed": results["failed"],
                    "skipped": results["skipped"],
                },
                "summary": {
                    "total": len(results["success"]) + len(results["failed"]) + len(results["skipped"]),
                    "success_count": len(results["success"]),
                    "failed_count": len(results["failed"]),
                    "skipped_count": len(results["skipped"]),
                },
            }
            print(json.dumps(payload, ensure_ascii=False))
        else:
            if not quiet:
                display_batch_results(results)

        # 如果有失败且未设置 continue_on_error，返回错误码
        if results["failed"] and not continue_on_error:
            return EXIT_RUNTIME_ERROR

        return EXIT_SUCCESS

    except Exception as exc:
        if args.json:
            print(json.dumps({"error": "runtime_error", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(t("error_unexpected", err=str(exc)), style="bold red")
        return EXIT_RUNTIME_ERROR


def cmd_env_list(args: argparse.Namespace) -> int:
    """处理 env --list：列出工具状态。"""
    # 注意：不检查 Conda，允许只读状态查询
    tools_status = []
    for name, exe, _ in BIO_TOOLS:
        installed = _check_installed(exe)
        tools_status.append({"name": name, "executable": exe, "installed": installed})

    if args.json:
        print(json.dumps({"tools": tools_status}, ensure_ascii=False))
    else:
        for tool in tools_status:
            status = _SYM_OK if tool["installed"] else _SYM_FAIL
            console_out.print(f"{status} {tool['name']}")

    return EXIT_SUCCESS


def cmd_env_install(args: argparse.Namespace) -> int:
    """处理 env --install：安装指定工具。"""
    tool_name = args.install

    # JSON 模式自动启用 quiet
    quiet = args.quiet or args.json

    # 检查 conda
    if not _check_conda():
        if args.json:
            print(json.dumps({"error": "conda_missing"}, ensure_ascii=False))
        else:
            console_err.print(t("env_conda_missing"), style="bold red")
            console_err.print("Install conda from: https://docs.conda.io/en/latest/miniconda.html")
        return EXIT_DEPENDENCY_MISSING

    # 查找工具
    tool_info = None
    for name, exe, cmd in BIO_TOOLS:
        if name.lower() == tool_name.lower():
            tool_info = (name, exe, cmd)
            break

    if not tool_info:
        if args.json:
            print(json.dumps({"error": "unknown_tool", "tool": tool_name}, ensure_ascii=False))
        else:
            console_err.print(f"Error: Unknown tool '{tool_name}'", style="bold red")
            console_err.print(f"Available tools: {', '.join(tool_entry[0] for tool_entry in BIO_TOOLS)}")
        return EXIT_ARGUMENT_ERROR

    name, exe, cmd = tool_info

    # 检查是否已安装
    if _check_installed(exe):
        if args.json:
            print(json.dumps({"status": "already_installed", "tool": name}, ensure_ascii=False))
        else:
            if not quiet:
                console_err.print(t("env_already", tool=name), style="yellow")
        return EXIT_SUCCESS

    # 执行安装
    try:
        if not quiet:
            console_err.print(t("env_installing", tool=name), style="bold cyan")

        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL if quiet else None)

        if args.json:
            print(json.dumps({"status": "success", "tool": name}, ensure_ascii=False))
        else:
            if not quiet:
                console_err.print(t("env_install_ok", tool=name), style="bold green")

        return EXIT_SUCCESS

    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        if args.json:
            print(json.dumps({"error": "install_failed", "tool": name, "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(t("env_install_fail", tool=name, err=str(exc)), style="bold red")
        return EXIT_RUNTIME_ERROR


def cmd_qc(args: argparse.Namespace) -> int:
    """处理 qc 子命令：质控流程。"""
    quiet = args.quiet or args.json
    try:
        params = _merge_workflow_args(
            args,
            "qc",
            {"input": None, "output": None, "outdir": None, "adapter": None, "minlen": 36, "resume": False},
        )
    except ConfigError as exc:
        if args.json:
            print(json.dumps({"error": "config_error", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(f"Error: {exc}", style="bold red")
        return EXIT_ARGUMENT_ERROR

    if not params["input"]:
        if args.json:
            print(json.dumps({"error": "missing_required", "field": "input"}, ensure_ascii=False))
        else:
            console_err.print("Error: input is required (CLI or config)", style="bold red")
        return EXIT_ARGUMENT_ERROR

    input_path = Path(str(params["input"]))

    if not input_path.exists():
        if args.json:
            print(json.dumps({"error": "file_not_found", "path": str(input_path)}, ensure_ascii=False))
        else:
            console_err.print(t("seq_file_not_found", path=str(input_path)), style="bold red")
        return EXIT_ARGUMENT_ERROR

    output_dir = Path(str(params["output"])) if params["output"] else None
    outdir = Path(str(params["outdir"])) if params["outdir"] else None
    adapter = str(params["adapter"]) if params["adapter"] else None
    minlen = int(params["minlen"])
    resume = bool(params["resume"])

    if minlen <= 0:
        if args.json:
            print(json.dumps({"error": "invalid_minlen", "minlen": minlen}, ensure_ascii=False))
        else:
            console_err.print(f"Error: minlen must be positive (got {minlen})", style="bold red")
        return EXIT_ARGUMENT_ERROR

    try:
        success = run_qc_pipeline(
            input_path,
            output_dir=output_dir,
            outdir=outdir,
            adapter=adapter,
            minlen=minlen,
            resume=resume,
            cli_mode=True,
        )
        if success:
            if args.json:
                final_outdir = str(outdir or output_dir or _default_workflow_outdir("qc", input_path))
                payload = {
                    "status": "success",
                    "input": str(input_path),
                    "outdir": final_outdir,
                    "output": str(Path(final_outdir) / "results"),
                    "metadata": str(Path(final_outdir) / "metadata.json"),
                    "resume_used": resume,
                }
                print(json.dumps(payload, ensure_ascii=False))
            return EXIT_SUCCESS
        else:
            return EXIT_RUNTIME_ERROR
    except PreflightError as exc:
        if args.json:
            print(json.dumps({"error": "dependency_missing", "tools": exc.missing_tools}, ensure_ascii=False))
        return EXIT_DEPENDENCY_MISSING
    except Exception as exc:
        if args.json:
            print(json.dumps({"error": "runtime_error", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(t("error_unexpected", err=str(exc)), style="bold red")
        return EXIT_RUNTIME_ERROR


def cmd_align(args: argparse.Namespace) -> int:
    """处理 align 子命令：序列比对流程。"""
    quiet = args.quiet or args.json
    try:
        params = _merge_workflow_args(
            args,
            "align",
            {"ref": None, "input": None, "output": None, "outdir": None, "threads": 1, "resume": False},
        )
    except ConfigError as exc:
        if args.json:
            print(json.dumps({"error": "config_error", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(f"Error: {exc}", style="bold red")
        return EXIT_ARGUMENT_ERROR

    if not params["ref"] or not params["input"]:
        missing = "ref" if not params["ref"] else "input"
        if args.json:
            print(json.dumps({"error": "missing_required", "field": missing}, ensure_ascii=False))
        else:
            console_err.print(f"Error: {missing} is required (CLI or config)", style="bold red")
        return EXIT_ARGUMENT_ERROR

    ref_path = Path(str(params["ref"]))
    input_path = Path(str(params["input"]))
    threads = int(params["threads"])
    resume = bool(params["resume"])

    # 参数校验
    if not ref_path.exists():
        if args.json:
            print(json.dumps({"error": "file_not_found", "path": str(ref_path)}, ensure_ascii=False))
        else:
            console_err.print(t("seq_file_not_found", path=str(ref_path)), style="bold red")
        return EXIT_ARGUMENT_ERROR

    if not input_path.exists():
        if args.json:
            print(json.dumps({"error": "file_not_found", "path": str(input_path)}, ensure_ascii=False))
        else:
            console_err.print(t("seq_file_not_found", path=str(input_path)), style="bold red")
        return EXIT_ARGUMENT_ERROR

    if threads <= 0:
        if args.json:
            print(json.dumps({"error": "invalid_threads", "threads": threads}, ensure_ascii=False))
        else:
            console_err.print(f"Error: threads must be positive (got {threads})", style="bold red")
        return EXIT_ARGUMENT_ERROR

    output_path = Path(str(params["output"])) if params["output"] else None
    outdir = Path(str(params["outdir"])) if params["outdir"] else None

    try:
        stats = run_alignment_pipeline(
            ref_path,
            input_path,
            output=output_path,
            outdir=outdir,
            threads=threads,
            resume=resume,
            cli_mode=True,
        )
        if stats is not None:
            if args.json:
                payload = {
                    "status": "success",
                    "ref": str(ref_path),
                    "input": str(input_path),
                    "output": str(_resolve_align_json_output(input_path, output_path, outdir)),
                    "outdir": str(outdir or _default_workflow_outdir("align", input_path)),
                    "metadata": str((outdir or _default_workflow_outdir("align", input_path)) / "metadata.json"),
                    "resume_used": resume,
                    "stats": {
                        "total": stats["total"],
                        "mapped": stats["mapped"],
                        "unmapped": stats["unmapped"],
                        "mapping_rate": round(float(stats["mapping_rate"]), 6),
                    },
                }
                print(json.dumps(payload, ensure_ascii=False))
            return EXIT_SUCCESS
        else:
            return EXIT_RUNTIME_ERROR
    except PreflightError as exc:
        if args.json:
            print(json.dumps({"error": "dependency_missing", "tools": exc.missing_tools}, ensure_ascii=False))
        return EXIT_DEPENDENCY_MISSING
    except Exception as exc:
        if args.json:
            print(json.dumps({"error": "runtime_error", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(t("error_unexpected", err=str(exc)), style="bold red")
        return EXIT_RUNTIME_ERROR


def cmd_report(args: argparse.Namespace) -> int:
    """处理 report 子命令：生成 HTML 运行报告。"""
    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else Path("report.html")
    title = args.title if hasattr(args, "title") and args.title else None
    quiet = args.quiet or args.json

    if not input_path.exists() or not input_path.is_dir():
        if args.json:
            print(json.dumps({"error": "path_not_found", "path": str(input_path)}, ensure_ascii=False))
        else:
            console_err.print(t("report_invalid_input", path=str(input_path)), style="bold red")
        return EXIT_ARGUMENT_ERROR

    try:
        if not quiet:
            console_err.print(t("report_generating"), style="cyan")

        result_path = generate_report(input_path, output_path, title=title)

        if args.json:
            print(json.dumps({
                "status": "success",
                "input": str(input_path),
                "output": str(result_path),
            }, ensure_ascii=False))
        elif not quiet:
            console_err.print(t("report_done", path=str(result_path)), style="bold green")

        return EXIT_SUCCESS

    except FileNotFoundError as exc:
        if args.json:
            print(json.dumps({"error": "no_runs_found", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(str(exc), style="bold red")
        return EXIT_ARGUMENT_ERROR

    except Exception as exc:
        if args.json:
            print(json.dumps({"error": "runtime_error", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(t("error_unexpected", err=str(exc)), style="bold red")
        return EXIT_RUNTIME_ERROR


def cmd_inspect(args: argparse.Namespace) -> int:
    """处理 inspect 子命令：检查运行目录状态。"""
    input_path = Path(args.input)

    if not input_path.exists() or not input_path.is_dir():
        if args.json:
            print(json.dumps({"error": "path_not_found", "path": str(input_path)}, ensure_ascii=False))
        else:
            console_err.print(t("inspect_invalid_input", path=str(input_path)), style="bold red")
        return EXIT_ARGUMENT_ERROR

    try:
        payload = inspect_run(input_path)
        if args.json:
            print(json.dumps({"status": "success", **payload}, ensure_ascii=False))
        else:
            console_out.print(render_inspection_text(payload))
        return EXIT_SUCCESS
    except FileNotFoundError as exc:
        if args.json:
            print(json.dumps({"error": "metadata_not_found", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(str(exc), style="bold red")
        return EXIT_ARGUMENT_ERROR
    except Exception as exc:
        if args.json:
            print(json.dumps({"error": "runtime_error", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(t("error_unexpected", err=str(exc)), style="bold red")
        return EXIT_RUNTIME_ERROR


def cmd_search(args: argparse.Namespace) -> int:
    """处理 search 子命令：BLAST 检索流程。"""
    try:
        params = _merge_workflow_args(
            args,
            "search",
            {
                "db": None,
                "query": None,
                "output": None,
                "outdir": None,
                "evalue": 10.0,
                "max_target_seqs": 10,
                "top": 5,
                "resume": False,
            },
        )
    except ConfigError as exc:
        if args.json:
            print(json.dumps({"error": "config_error", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(f"Error: {exc}", style="bold red")
        return EXIT_ARGUMENT_ERROR

    if not params["db"] or not params["query"]:
        missing = "db" if not params["db"] else "query"
        if args.json:
            print(json.dumps({"error": "missing_required", "field": missing}, ensure_ascii=False))
        else:
            console_err.print(f"Error: {missing} is required (CLI or config)", style="bold red")
        return EXIT_ARGUMENT_ERROR

    db_path = Path(str(params["db"]))
    query_path = Path(str(params["query"]))
    evalue = float(params["evalue"])
    max_target_seqs = int(params["max_target_seqs"])
    top_n = int(params["top"])
    resume = bool(params["resume"])

    if not db_path.exists():
        if args.json:
            print(json.dumps({"error": "file_not_found", "path": str(db_path)}, ensure_ascii=False))
        else:
            console_err.print(t("seq_file_not_found", path=str(db_path)), style="bold red")
        return EXIT_ARGUMENT_ERROR

    if not query_path.exists():
        if args.json:
            print(json.dumps({"error": "file_not_found", "path": str(query_path)}, ensure_ascii=False))
        else:
            console_err.print(t("seq_file_not_found", path=str(query_path)), style="bold red")
        return EXIT_ARGUMENT_ERROR

    if evalue <= 0:
        if args.json:
            print(json.dumps({"error": "invalid_evalue", "evalue": evalue}, ensure_ascii=False))
        else:
            console_err.print(f"Error: evalue must be positive (got {evalue})", style="bold red")
        return EXIT_ARGUMENT_ERROR

    if max_target_seqs <= 0:
        if args.json:
            print(
                json.dumps(
                    {"error": "invalid_max_target_seqs", "max_target_seqs": max_target_seqs},
                    ensure_ascii=False,
                )
            )
        else:
            console_err.print(
                f"Error: max_target_seqs must be positive (got {max_target_seqs})",
                style="bold red",
            )
        return EXIT_ARGUMENT_ERROR

    if top_n <= 0:
        if args.json:
            print(json.dumps({"error": "invalid_top", "top": top_n}, ensure_ascii=False))
        else:
            console_err.print(f"Error: top must be positive (got {top_n})", style="bold red")
        return EXIT_ARGUMENT_ERROR

    output_path = Path(str(params["output"])) if params["output"] else None
    outdir = Path(str(params["outdir"])) if params["outdir"] else None

    try:
        result = run_blast_search(
            db_path,
            query_path,
            output=output_path,
            outdir=outdir,
            evalue=evalue,
            max_target_seqs=max_target_seqs,
            top_n=top_n,
            resume=resume,
            cli_mode=True,
        )
        if result is None:
            return EXIT_RUNTIME_ERROR
        if args.json:
            print(json.dumps({"status": "success", **result}, ensure_ascii=False))
        elif not args.quiet:
            from bioflow.search import display_search_summary

            display_search_summary(result["summary"])
        return EXIT_SUCCESS
    except PreflightError as exc:
        if args.json:
            print(json.dumps({"error": "dependency_missing", "tools": exc.missing_tools}, ensure_ascii=False))
        return EXIT_DEPENDENCY_MISSING
    except Exception as exc:
        if args.json:
            print(json.dumps({"error": "runtime_error", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(t("error_unexpected", err=str(exc)), style="bold red")
        return EXIT_RUNTIME_ERROR


def main() -> int:
    """CLI 主入口。"""
    parser = argparse.ArgumentParser(
        prog="bioflow",
        description="BioFlow-CLI - Bioinformatics workflow tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--quiet", "-q", action="store_true", help="Suppress progress messages")
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # seq 子命令
    parser_seq = subparsers.add_parser("seq", help="Format FASTA/FASTQ sequences")
    parser_seq.add_argument("--input", "-i", required=True, help="Input FASTA/FASTQ file")
    parser_seq.add_argument("--output", "-o", help="Output file (default: input.formatted.fasta)")
    parser_seq.add_argument("--width", "-w", type=int, default=80, help="Line width (default: 80)")

    # env 子命令
    parser_env = subparsers.add_parser("env", help="Manage bioinformatics tools")
    env_group = parser_env.add_mutually_exclusive_group(required=True)
    env_group.add_argument("--list", "-l", action="store_true", help="List all tools and their status")
    env_group.add_argument("--install", "-i", metavar="TOOL", help="Install a specific tool")

    # qc 子命令
    parser_qc = subparsers.add_parser("qc", help="Run QC pipeline (FastQC + Trimmomatic)")
    parser_qc.add_argument("--config", help="YAML config file for qc workflow")
    parser_qc.add_argument("--input", "-i", help="Input FASTQ file")
    parser_qc.add_argument("--output", "-o", help="Legacy run root directory (same effect as --outdir)")
    parser_qc.add_argument("--outdir", help="Run output root directory (default: input_dir/qc_run)")
    parser_qc.add_argument("--resume", action="store_true", help="Resume from the latest valid QC checkpoint")
    parser_qc.add_argument("--adapter", "-a", help="Adapter file for Trimmomatic")
    parser_qc.add_argument("--minlen", type=int, help="Minimum read length (default: 36)")

    # batch 子命令
    parser_batch = subparsers.add_parser("batch", help="Batch format multiple sequence files")
    parser_batch.add_argument("--input-dir", "-i", required=True, help="Input directory containing sequence files")
    parser_batch.add_argument("--output-dir", "-o", help="Output directory (default: ./formatted_output)")
    parser_batch.add_argument("--pattern", "-p", default="*.fasta", help="File pattern to match (default: *.fasta)")
    parser_batch.add_argument("--recursive", "-r", action="store_true", help="Recursively scan subdirectories")
    parser_batch.add_argument("--width", "-w", type=int, default=80, help="Line width (default: 80)")
    parser_batch.add_argument("--workers", type=int, default=1, help="Number of worker processes (default: 1)")
    parser_batch.add_argument("--continue-on-error", "-c", action="store_true", help="Continue processing on error")

    # align 子命令
    parser_align = subparsers.add_parser("align", help="Run alignment pipeline (BWA + SAMtools)")
    parser_align.add_argument("--config", help="YAML config file for alignment workflow")
    parser_align.add_argument("--ref", "-r", help="Reference genome FASTA file")
    parser_align.add_argument("--input", "-i", help="Input reads file (FASTQ)")
    parser_align.add_argument("--output", "-o", help="Output BAM file written under results/ unless absolute path is given")
    parser_align.add_argument("--outdir", help="Run output root directory (default: input_dir/align_run)")
    parser_align.add_argument("--resume", action="store_true", help="Resume from the latest valid alignment checkpoint")
    parser_align.add_argument("--threads", "-t", type=int, help="Number of threads (default: 1)")

    # search 子命令
    parser_search = subparsers.add_parser("search", help="Run BLAST nucleotide search")
    parser_search.add_argument("--config", help="YAML config file for search workflow")
    parser_search.add_argument("--db", help="Reference database FASTA file")
    parser_search.add_argument("--query", "-q", help="Query FASTA file")
    parser_search.add_argument("--output", "-o", help="Output TSV file written under results/ unless absolute path is given")
    parser_search.add_argument("--outdir", help="Run output root directory (default: query_dir/search_run)")
    parser_search.add_argument("--resume", action="store_true", help="Resume from the latest valid search checkpoint")
    parser_search.add_argument("--evalue", type=float, help="E-value threshold (default: 10.0)")
    parser_search.add_argument(
        "--max-target-seqs",
        type=int,
        help="Maximum target sequences per query (default: 10)",
    )
    parser_search.add_argument("--top", type=int, help="Number of top hits to summarize (default: 5)")

    # report 子命令
    parser_report = subparsers.add_parser("report", help="Generate HTML run report")
    parser_report.add_argument("--input", "-i", required=True, help="Run directory or parent directory containing runs")
    parser_report.add_argument("--output", "-o", help="Output HTML file (default: report.html)")
    parser_report.add_argument("--title", help="Custom report title")

    # inspect 子命令
    parser_inspect = subparsers.add_parser("inspect", help="Inspect run metadata and diagnostics")
    parser_inspect.add_argument("--input", "-i", required=True, help="Run directory containing metadata.json")

    args = parser.parse_args()

    # 初始化
    _setup_logging(quiet=args.quiet)
    init_language()

    # 路由到子命令
    if args.command == "seq":
        return cmd_seq(args)
    elif args.command == "batch":
        return cmd_batch(args)
    elif args.command == "env":
        if args.list:
            return cmd_env_list(args)
        elif args.install:
            return cmd_env_install(args)
        else:
            return EXIT_ARGUMENT_ERROR
    elif args.command == "qc":
        return cmd_qc(args)
    elif args.command == "align":
        return cmd_align(args)
    elif args.command == "search":
        return cmd_search(args)
    elif args.command == "report":
        return cmd_report(args)
    elif args.command == "inspect":
        return cmd_inspect(args)
    else:
        parser.print_help(sys.stderr)
        return EXIT_ARGUMENT_ERROR


if __name__ == "__main__":
    sys.exit(main())

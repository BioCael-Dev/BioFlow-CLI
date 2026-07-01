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
from bioflow.execution import build_execution_context
from bioflow.alignment import run_alignment_pipeline
from bioflow.config import ConfigError, load_project_config, load_workflow_config
from bioflow.i18n import init_language, t
from bioflow.inspect import inspect_run, render_inspection_text
from bioflow.pipeline import run_qc_pipeline
from bioflow.preflight import PreflightError
from bioflow.project_batch import run_project_batch
from bioflow.report import collect_summary_data, generate_report, write_summary_json, write_summary_tsv
from bioflow.run_layout import format_failure_diagnostics
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


def _validate_single_or_paired_inputs(
    *,
    input_value: Any = None,
    input_r1_value: Any = None,
    input_r2_value: Any = None,
    workflow: str,
) -> tuple[Path | None, Path | None, Path | None] | str:
    """校验单端/双端输入组合。"""
    has_single = bool(input_value)
    has_r1 = bool(input_r1_value)
    has_r2 = bool(input_r2_value)
    if has_single and (has_r1 or has_r2):
        return f"{workflow} cannot mix input with input_r1/input_r2"
    if has_r1 != has_r2:
        return f"{workflow} paired-end mode requires both input_r1 and input_r2"
    if not has_single and not (has_r1 and has_r2):
        return f"{workflow} requires input or both input_r1 and input_r2"
    return (
        Path(str(input_value)) if has_single else None,
        Path(str(input_r1_value)) if has_r1 else None,
        Path(str(input_r2_value)) if has_r2 else None,
    )


def _print_failure_diagnostics(metadata_path: Path, *, as_json: bool) -> None:
    """从 metadata.json 读取并打印统一失败诊断。"""
    if as_json or not metadata_path.exists():
        return
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    details = payload.get("failure_details")
    if isinstance(details, dict):
        console_err.print(format_failure_diagnostics(details), style="bold red")


def _json_error_payload(error: str, **extra: Any) -> str:
    """构造 JSON 错误输出。"""
    payload = {"error": error, **extra}
    return json.dumps(payload, ensure_ascii=False)


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
            {
                "input": None, "input_r1": None, "input_r2": None, "output": None, "outdir": None,
                "adapter": None, "minlen": 36, "resume": False, "profile": "local",
                "threads": None, "memory": None, "queue": None, "time_limit": None,
                "backend": "system", "conda_env": None, "container_image": None,
            },
        )
    except ConfigError as exc:
        if args.json:
            print(json.dumps({"error": "config_error", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(f"Error: {exc}", style="bold red")
        return EXIT_ARGUMENT_ERROR

    inputs = _validate_single_or_paired_inputs(
        input_value=params["input"],
        input_r1_value=params["input_r1"],
        input_r2_value=params["input_r2"],
        workflow="qc",
    )
    if isinstance(inputs, str):
        if args.json:
            print(_json_error_payload("invalid_input_combination", message=inputs))
        else:
            console_err.print(f"Error: {inputs}", style="bold red")
        return EXIT_ARGUMENT_ERROR

    input_path, input_r1_path, input_r2_path = inputs

    for candidate in (input_path, input_r1_path, input_r2_path):
        if candidate is not None and not candidate.exists():
            if args.json:
                print(json.dumps({"error": "file_not_found", "path": str(candidate)}, ensure_ascii=False))
            else:
                console_err.print(t("seq_file_not_found", path=str(candidate)), style="bold red")
            return EXIT_ARGUMENT_ERROR

    output_dir = Path(str(params["output"])) if params["output"] else None
    outdir = Path(str(params["outdir"])) if params["outdir"] else None
    adapter = str(params["adapter"]) if params["adapter"] else None
    minlen = int(params["minlen"])
    resume = bool(params["resume"])
    execution = build_execution_context(params, source="cli_or_config")

    if minlen <= 0:
        if args.json:
            print(json.dumps({"error": "invalid_minlen", "minlen": minlen}, ensure_ascii=False))
        else:
            console_err.print(f"Error: minlen must be positive (got {minlen})", style="bold red")
        return EXIT_ARGUMENT_ERROR

    try:
        success = run_qc_pipeline(
            input_path,
            input_r1=input_r1_path,
            input_r2=input_r2_path,
            output_dir=output_dir,
            outdir=outdir,
            adapter=adapter,
            minlen=minlen,
            resume=resume,
            execution=execution,
            cli_mode=True,
        )
        if success:
            if args.json:
                anchor = input_path or input_r1_path
                assert anchor is not None
                final_outdir = str(outdir or output_dir or _default_workflow_outdir("qc", anchor))
                payload = {
                    "status": "success",
                    "input": str(input_path) if input_path else None,
                    "input_r1": str(input_r1_path) if input_r1_path else None,
                    "input_r2": str(input_r2_path) if input_r2_path else None,
                    "outdir": final_outdir,
                    "output": str(Path(final_outdir) / "results"),
                    "metadata": str(Path(final_outdir) / "metadata.json"),
                    "resume_used": resume,
                    "execution": execution,
                }
                print(json.dumps(payload, ensure_ascii=False))
            return EXIT_SUCCESS
        else:
            anchor = input_path or input_r1_path
            assert anchor is not None
            metadata_path = Path(str(outdir or output_dir or _default_workflow_outdir("qc", anchor))) / "metadata.json"
            _print_failure_diagnostics(metadata_path, as_json=args.json)
            return EXIT_RUNTIME_ERROR
    except PreflightError as exc:
        if args.json:
            print(
                json.dumps(
                    {
                        "error": "dependency_missing",
                        "tools": exc.missing_tools,
                        "backend": exc.backend,
                        "reason": exc.reason,
                        "missing_runtime": exc.missing_runtime,
                        "conda_env": exc.conda_env,
                        "container_image": exc.container_image,
                    },
                    ensure_ascii=False,
                )
            )
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
            {
                "ref": None, "input": None, "input_r1": None, "input_r2": None, "output": None,
                "outdir": None, "threads": 1, "resume": False, "profile": "local",
                "memory": None, "queue": None, "time_limit": None,
                "backend": "system", "conda_env": None, "container_image": None,
            },
        )
    except ConfigError as exc:
        if args.json:
            print(json.dumps({"error": "config_error", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(f"Error: {exc}", style="bold red")
        return EXIT_ARGUMENT_ERROR

    inputs = _validate_single_or_paired_inputs(
        input_value=params["input"],
        input_r1_value=params["input_r1"],
        input_r2_value=params["input_r2"],
        workflow="align",
    )
    if isinstance(inputs, str):
        if args.json:
            print(_json_error_payload("invalid_input_combination", message=str(inputs)))
        else:
            console_err.print(f"Error: {inputs}", style="bold red")
        return EXIT_ARGUMENT_ERROR

    if not params["ref"]:
        if args.json:
            print(_json_error_payload("missing_required", field="ref"))
        else:
            console_err.print("Error: ref is required (CLI or config)", style="bold red")
        return EXIT_ARGUMENT_ERROR

    ref_path = Path(str(params["ref"]))
    input_path, input_r1_path, input_r2_path = inputs
    threads = int(params["threads"])
    resume = bool(params["resume"])
    execution = build_execution_context(params, source="cli_or_config")

    # 参数校验
    if not ref_path.exists():
        if args.json:
            print(json.dumps({"error": "file_not_found", "path": str(ref_path)}, ensure_ascii=False))
        else:
            console_err.print(t("seq_file_not_found", path=str(ref_path)), style="bold red")
        return EXIT_ARGUMENT_ERROR

    for candidate in (input_path, input_r1_path, input_r2_path):
        if candidate is not None and not candidate.exists():
            if args.json:
                print(json.dumps({"error": "file_not_found", "path": str(candidate)}, ensure_ascii=False))
            else:
                console_err.print(t("seq_file_not_found", path=str(candidate)), style="bold red")
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
            input_r1=input_r1_path,
            input_r2=input_r2_path,
            output=output_path,
            outdir=outdir,
            threads=threads,
            resume=resume,
            execution=execution,
            cli_mode=True,
        )
        if stats is not None:
            if args.json:
                anchor = input_path or input_r1_path
                assert anchor is not None
                payload = {
                    "status": "success",
                    "ref": str(ref_path),
                    "input": str(input_path) if input_path else None,
                    "input_r1": str(input_r1_path) if input_r1_path else None,
                    "input_r2": str(input_r2_path) if input_r2_path else None,
                    "output": str(_resolve_align_json_output(anchor, output_path, outdir)),
                    "outdir": str(outdir or _default_workflow_outdir("align", anchor)),
                    "metadata": str((outdir or _default_workflow_outdir("align", anchor)) / "metadata.json"),
                    "resume_used": resume,
                    "execution": execution,
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
            anchor = input_path or input_r1_path
            assert anchor is not None
            metadata_path = (outdir or _default_workflow_outdir("align", anchor)) / "metadata.json"
            _print_failure_diagnostics(metadata_path, as_json=args.json)
            return EXIT_RUNTIME_ERROR
    except PreflightError as exc:
        if args.json:
            print(
                json.dumps(
                    {
                        "error": "dependency_missing",
                        "tools": exc.missing_tools,
                        "backend": exc.backend,
                        "reason": exc.reason,
                        "missing_runtime": exc.missing_runtime,
                        "conda_env": exc.conda_env,
                        "container_image": exc.container_image,
                    },
                    ensure_ascii=False,
                )
            )
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
    summary_json_path = Path(args.summary_json) if getattr(args, "summary_json", None) else None
    summary_tsv_path = Path(args.summary_tsv) if getattr(args, "summary_tsv", None) else None
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
        summary_data = None
        if summary_json_path is not None or summary_tsv_path is not None:
            summary_data = collect_summary_data(input_path)
        if summary_json_path is not None and summary_data is not None:
            write_summary_json(summary_data, summary_json_path)
        if summary_tsv_path is not None and summary_data is not None:
            write_summary_tsv(summary_data, summary_tsv_path)

        if args.json:
            print(json.dumps({
                "status": "success",
                "input": str(input_path),
                "output": str(result_path),
                "summary_json": str(summary_json_path) if summary_json_path is not None else "",
                "summary_tsv": str(summary_tsv_path) if summary_tsv_path is not None else "",
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
        payload = inspect_run(input_path, show_log=getattr(args, "show_log", None))
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
                "profile": "local",
                "threads": None,
                "memory": None,
                "queue": None,
                "time_limit": None,
                "backend": "system",
                "conda_env": None,
                "container_image": None,
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
    execution = build_execution_context(params, source="cli_or_config")

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
            execution=execution,
            cli_mode=True,
        )
        if result is None:
            metadata_path = (outdir or _default_workflow_outdir("search", query_path)) / "metadata.json"
            _print_failure_diagnostics(metadata_path, as_json=args.json)
            return EXIT_RUNTIME_ERROR
        if args.json:
            print(json.dumps({"status": "success", "execution": execution, **result}, ensure_ascii=False))
        elif not args.quiet:
            from bioflow.search import display_search_summary

            display_search_summary(result["summary"])
        return EXIT_SUCCESS
    except PreflightError as exc:
        if args.json:
            print(
                json.dumps(
                    {
                        "error": "dependency_missing",
                        "tools": exc.missing_tools,
                        "backend": exc.backend,
                        "reason": exc.reason,
                        "missing_runtime": exc.missing_runtime,
                        "conda_env": exc.conda_env,
                        "container_image": exc.container_image,
                    },
                    ensure_ascii=False,
                )
            )
        return EXIT_DEPENDENCY_MISSING
    except Exception as exc:
        if args.json:
            print(json.dumps({"error": "runtime_error", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(t("error_unexpected", err=str(exc)), style="bold red")
        return EXIT_RUNTIME_ERROR


def cmd_project(args: argparse.Namespace) -> int:
    """处理 project 子命令：项目级多样本 workflow batch。"""
    config_path = _resolve_config_path(getattr(args, "config", None))
    if config_path is None:
        if args.json:
            print(json.dumps({"error": "missing_required", "field": "config"}, ensure_ascii=False))
        else:
            console_err.print("Error: config is required", style="bold red")
        return EXIT_ARGUMENT_ERROR

    try:
        config = load_project_config(config_path)
    except ConfigError as exc:
        if args.json:
            print(json.dumps({"error": "config_error", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(f"Error: {exc}", style="bold red")
        return EXIT_ARGUMENT_ERROR

    outdir = Path(args.outdir) if getattr(args, "outdir", None) else None
    if getattr(args, "continue_on_error", False):
        config["continue_on_error"] = True
    if getattr(args, "profile", None):
        config["profile"] = args.profile
    if getattr(args, "threads", None) is not None:
        config["threads"] = args.threads
    if getattr(args, "memory", None):
        config["memory"] = args.memory
    if getattr(args, "queue", None):
        config["queue"] = args.queue
    if getattr(args, "time_limit", None):
        config["time_limit"] = args.time_limit
    if getattr(args, "backend", None):
        config["backend"] = args.backend
    if getattr(args, "conda_env", None):
        config["conda_env"] = args.conda_env
    if getattr(args, "container_image", None):
        config["container_image"] = args.container_image

    try:
        result = run_project_batch(
            config_path=config_path,
            project_config=config,
            outdir=outdir,
            quiet=args.quiet or args.json,
        )
    except Exception as exc:
        if args.json:
            print(json.dumps({"error": "runtime_error", "message": str(exc)}, ensure_ascii=False))
        else:
            console_err.print(t("error_unexpected", err=str(exc)), style="bold red")
        return EXIT_RUNTIME_ERROR

    if args.json:
        print(json.dumps(result, ensure_ascii=False))
    elif not (args.quiet or args.json):
        console_out.print(
            t(
                "project_batch_done",
                root=result["project_root"],
                planned=result["planned_sample_count"],
                success=result["success_count"],
                failed=result["failed_count"],
                summary=result["summary"],
                report=result["report"] or "-",
            )
        )

    if result["failed_count"] and not result["continue_on_error"]:
        return EXIT_RUNTIME_ERROR
    return EXIT_SUCCESS


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
    parser_qc.add_argument("--input-r1", help="Input R1 FASTQ file for paired-end mode")
    parser_qc.add_argument("--input-r2", help="Input R2 FASTQ file for paired-end mode")
    parser_qc.add_argument("--output", "-o", help="Legacy run root directory (same effect as --outdir)")
    parser_qc.add_argument("--outdir", help="Run output root directory (default: input_dir/qc_run)")
    parser_qc.add_argument("--resume", action="store_true", help="Resume from the latest valid QC checkpoint")
    parser_qc.add_argument("--adapter", "-a", help="Adapter file for Trimmomatic")
    parser_qc.add_argument("--minlen", type=int, help="Minimum read length (default: 36)")
    parser_qc.add_argument("--profile", help="Execution profile (default: local)")
    parser_qc.add_argument("--threads", type=int, help="Requested threads for execution metadata")
    parser_qc.add_argument("--memory", help="Requested memory for execution metadata")
    parser_qc.add_argument("--queue", help="Requested queue/partition for execution metadata")
    parser_qc.add_argument("--time-limit", dest="time_limit", help="Requested walltime for execution metadata")
    parser_qc.add_argument("--backend", choices=["system", "conda", "container"], help="Execution backend (default: system)")
    parser_qc.add_argument("--conda-env", dest="conda_env", help="Conda environment name for backend metadata")
    parser_qc.add_argument("--container-image", dest="container_image", help="Container image name for backend metadata")

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
    parser_align.add_argument("--input-r1", help="Input R1 FASTQ file for paired-end mode")
    parser_align.add_argument("--input-r2", help="Input R2 FASTQ file for paired-end mode")
    parser_align.add_argument("--output", "-o", help="Output BAM file written under results/ unless absolute path is given")
    parser_align.add_argument("--outdir", help="Run output root directory (default: input_dir/align_run)")
    parser_align.add_argument("--resume", action="store_true", help="Resume from the latest valid alignment checkpoint")
    parser_align.add_argument("--threads", "-t", type=int, help="Number of threads (default: 1)")
    parser_align.add_argument("--profile", help="Execution profile (default: local)")
    parser_align.add_argument("--memory", help="Requested memory for execution metadata")
    parser_align.add_argument("--queue", help="Requested queue/partition for execution metadata")
    parser_align.add_argument("--time-limit", dest="time_limit", help="Requested walltime for execution metadata")
    parser_align.add_argument("--backend", choices=["system", "conda", "container"], help="Execution backend (default: system)")
    parser_align.add_argument("--conda-env", dest="conda_env", help="Conda environment name for backend metadata")
    parser_align.add_argument("--container-image", dest="container_image", help="Container image name for backend metadata")

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
    parser_search.add_argument("--profile", help="Execution profile (default: local)")
    parser_search.add_argument("--threads", type=int, help="Requested threads for execution metadata")
    parser_search.add_argument("--memory", help="Requested memory for execution metadata")
    parser_search.add_argument("--queue", help="Requested queue/partition for execution metadata")
    parser_search.add_argument("--time-limit", dest="time_limit", help="Requested walltime for execution metadata")
    parser_search.add_argument("--backend", choices=["system", "conda", "container"], help="Execution backend (default: system)")
    parser_search.add_argument("--conda-env", dest="conda_env", help="Conda environment name for backend metadata")
    parser_search.add_argument("--container-image", dest="container_image", help="Container image name for backend metadata")

    # report 子命令
    parser_report = subparsers.add_parser("report", help="Generate HTML run report")
    parser_report.add_argument("--input", "-i", required=True, help="Run directory or parent directory containing runs")
    parser_report.add_argument("--output", "-o", help="Output HTML file (default: report.html)")
    parser_report.add_argument("--title", help="Custom report title")
    parser_report.add_argument("--summary-json", dest="summary_json", help="Optional structured summary JSON output")
    parser_report.add_argument("--summary-tsv", dest="summary_tsv", help="Optional structured summary TSV output")

    # project 子命令
    parser_project = subparsers.add_parser("project", help="Run project-level workflow batch from YAML")
    parser_project.add_argument("--config", required=True, help="YAML project batch config file")
    parser_project.add_argument("--outdir", help="Project output root directory (default: config_dir/project_run)")
    parser_project.add_argument("--continue-on-error", "-c", action="store_true", help="Continue remaining samples when one sample fails")
    parser_project.add_argument("--profile", help="Default execution profile for project samples")
    parser_project.add_argument("--threads", type=int, help="Default threads for project samples")
    parser_project.add_argument("--memory", help="Default memory for project samples")
    parser_project.add_argument("--queue", help="Default queue/partition for project samples")
    parser_project.add_argument("--time-limit", dest="time_limit", help="Default walltime for project samples")
    parser_project.add_argument("--backend", choices=["system", "conda", "container"], help="Default execution backend for project samples")
    parser_project.add_argument("--conda-env", dest="conda_env", help="Default conda environment name for project samples")
    parser_project.add_argument("--container-image", dest="container_image", help="Default container image name for project samples")

    # inspect 子命令
    parser_inspect = subparsers.add_parser("inspect", help="Inspect run metadata and diagnostics")
    parser_inspect.add_argument("--input", "-i", required=True, help="Run directory containing metadata.json")
    parser_inspect.add_argument("--show-log", choices=["tail"], help="Show stderr log tail")

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
    elif args.command == "project":
        return cmd_project(args)
    elif args.command == "inspect":
        return cmd_inspect(args)
    else:
        parser.print_help(sys.stderr)
        return EXIT_ARGUMENT_ERROR


if __name__ == "__main__":
    sys.exit(main())

"""Long-read alignment and QC workflow powered by minimap2 and SAMtools."""

from __future__ import annotations

import gzip
import json
import subprocess
from pathlib import Path
from typing import Any, TextIO

import questionary
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from bioflow.alignment import parse_flagstat
from bioflow.execution import (
    ResolvedCommand,
    resolve_command,
    resolve_pipeline_commands,
    stringify_command,
    summarize_commands,
)
from bioflow.i18n import t
from bioflow.registry import get_workflow_manifest
from bioflow.preflight import preflight_check
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
    resolve_result_path,
    set_step_state,
    step_resume_ready,
    utc_now_iso,
    write_metadata,
)

console = Console()

LONGREAD_REQUIRED_TOOLS = get_workflow_manifest("longread").required_tools
LONGREAD_PRESETS = ("map-ont", "map-hifi", "map-pb")
LONGREAD_STEP_QC = "sequence_stats"
LONGREAD_STEP_MAP = "map_sort"
LONGREAD_STEP_BAM_INDEX = "bam_index"
LONGREAD_STEP_FLAGSTAT = "flagstat"
LONGREAD_STEP_SUMMARY = "summary"


def _open_sequence_text(path: Path) -> TextIO:
    """Open a plain-text or gzip-compressed sequence file."""
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def _next_nonempty_line(handle: TextIO) -> str | None:
    for raw_line in handle:
        line = raw_line.strip()
        if line:
            return line
    return None


def _fasta_lengths(handle: TextIO) -> list[int]:
    lengths: list[int] = []
    current_length: int | None = None
    for raw_line in handle:
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith(">"):
            if current_length is not None:
                if current_length <= 0:
                    raise ValueError("FASTA contains an empty sequence")
                lengths.append(current_length)
            current_length = 0
            continue
        if current_length is None:
            raise ValueError("invalid FASTA: sequence data appears before a header")
        current_length += len("".join(line.split()))

    if current_length is None:
        raise ValueError("FASTA contains no records")
    if current_length <= 0:
        raise ValueError("FASTA contains an empty sequence")
    lengths.append(current_length)
    return lengths


def _fastq_metrics(handle: TextIO) -> tuple[list[int], int, int, int, int]:
    lengths: list[int] = []
    quality_score_sum = 0
    quality_bases = 0
    q20_bases = 0
    q30_bases = 0

    while True:
        header = _next_nonempty_line(handle)
        if header is None:
            break
        sequence_raw = handle.readline()
        plus_raw = handle.readline()
        quality_raw = handle.readline()
        if not sequence_raw or not plus_raw or not quality_raw:
            raise ValueError("invalid FASTQ: incomplete record")

        sequence = "".join(sequence_raw.strip().split())
        plus = plus_raw.strip()
        quality = "".join(quality_raw.strip().split())
        if not header.startswith("@") or not plus.startswith("+"):
            raise ValueError("invalid FASTQ record structure")
        if not sequence or len(sequence) != len(quality):
            raise ValueError("FASTQ sequence and quality lengths differ")

        lengths.append(len(sequence))
        scores = [ord(character) - 33 for character in quality]
        if any(score < 0 for score in scores):
            raise ValueError("FASTQ contains invalid Phred+33 quality characters")
        quality_score_sum += sum(scores)
        quality_bases += len(scores)
        q20_bases += sum(score >= 20 for score in scores)
        q30_bases += sum(score >= 30 for score in scores)

    if not lengths:
        raise ValueError("FASTQ contains no records")
    return lengths, quality_score_sum, quality_bases, q20_bases, q30_bases


def _calculate_n50(lengths: list[int]) -> int:
    """Return the conventional length-weighted N50."""
    threshold = (sum(lengths) + 1) // 2
    accumulated = 0
    for length in sorted(lengths, reverse=True):
        accumulated += length
        if accumulated >= threshold:
            return length
    return 0


def calculate_longread_stats(path: Path) -> dict[str, Any]:
    """Stream a FASTA/FASTQ file and return long-read length/quality metrics."""
    with _open_sequence_text(path) as handle:
        first_line = _next_nonempty_line(handle)
        if first_line is None:
            raise ValueError("sequence file is empty")
        handle.seek(0)

        quality_score_sum = 0
        quality_bases = 0
        q20_bases = 0
        q30_bases = 0
        if first_line.startswith(">"):
            sequence_format = "fasta"
            lengths = _fasta_lengths(handle)
        elif first_line.startswith("@"):
            sequence_format = "fastq"
            lengths, quality_score_sum, quality_bases, q20_bases, q30_bases = _fastq_metrics(handle)
        else:
            raise ValueError("unsupported sequence format; expected FASTA or FASTQ")

    total_bases = sum(lengths)
    stats: dict[str, Any] = {
        "input_format": sequence_format,
        "read_count": len(lengths),
        "total_bases": total_bases,
        "mean_read_length": total_bases / len(lengths),
        "min_read_length": min(lengths),
        "max_read_length": max(lengths),
        "n50": _calculate_n50(lengths),
        "quality_bases": quality_bases,
        "avg_quality": None,
        "q20_ratio": None,
        "q30_ratio": None,
    }
    if quality_bases:
        stats.update(
            {
                "avg_quality": quality_score_sum / quality_bases,
                "q20_ratio": q20_bases / quality_bases,
                "q30_ratio": q30_bases / quality_bases,
            }
        )
    return stats


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def _read_json_mapping(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _is_nonempty_file(path: Path) -> bool:
    return path.is_file() and path.stat().st_size > 0


def _qc_summary_ready(path: Path) -> bool:
    payload = _read_json_mapping(path)
    return all(key in payload for key in ("input_format", "read_count", "total_bases", "n50"))


def _flagstat_ready(path: Path) -> bool:
    if not _is_nonempty_file(path):
        return False
    try:
        parse_flagstat(path.read_text(encoding="utf-8"))
    except OSError:
        return False
    return True


def _summary_ready(path: Path) -> bool:
    payload = _read_json_mapping(path)
    return isinstance(payload.get("stats"), dict) and payload.get("workflow") == "longread"


def _resume_context_matches(
    metadata: dict[str, Any],
    *,
    ref: Path,
    reads: Path,
    preset: str,
    sample_id: str | None,
    input_details: dict[str, dict[str, Any]],
) -> bool:
    """Prevent resume when inputs or biologically relevant parameters changed."""
    if metadata.get("inputs") != {"ref": str(ref), "reads": str(reads)}:
        return False
    parameters = metadata.get("parameters")
    if not isinstance(parameters, dict):
        return False
    if parameters.get("preset") != preset or parameters.get("sample_id") != sample_id:
        return False
    previous_details = metadata.get("input_details")
    if not isinstance(previous_details, dict):
        return False
    for key in ("ref", "reads"):
        previous = previous_details.get(key)
        current = input_details.get(key)
        if not isinstance(previous, dict) or not isinstance(current, dict):
            return False
        if previous.get("sha256") != current.get("sha256"):
            return False
        if previous.get("size_bytes") != current.get("size_bytes"):
            return False
    return True


def _run_command(
    command: ResolvedCommand,
    *,
    stdout_log: Path,
    stderr_log: Path,
    capture: bool = False,
) -> subprocess.CompletedProcess[str] | None:
    try:
        result = subprocess.run(
            list(command.resolved_command),
            check=True,
            capture_output=capture,
            text=True,
        )
        append_log(stdout_log, result.stdout or "")
        append_log(stderr_log, result.stderr or "")
        return result
    except subprocess.CalledProcessError as exc:
        append_log(stdout_log, exc.stdout or "")
        append_log(stderr_log, exc.stderr or str(exc))
        return None
    except FileNotFoundError as exc:
        append_log(stderr_log, str(exc))
        return None


def _run_minimap2_pipe_sort(
    ref: Path,
    reads: Path,
    output_bam: Path,
    *,
    preset: str,
    threads: int,
    execution: dict[str, object] | None,
    stdout_log: Path,
    stderr_log: Path,
) -> bool:
    raw_commands = [
        ["minimap2", "-a", "-x", preset, "-t", str(threads), str(ref), str(reads)],
        ["samtools", "sort", "-@", str(threads), "-o", str(output_bam), "-"],
    ]
    commands = resolve_pipeline_commands(
        raw_commands,
        execution,
        path_hints=(ref, reads, output_bam),
        workdir=output_bam.parent,
    )
    minimap_process: subprocess.Popen[bytes] | None = None
    sort_process: subprocess.Popen[bytes] | None = None
    try:
        with stdout_log.open("ab") as stdout_handle, stderr_log.open("ab") as stderr_handle:
            minimap_process = subprocess.Popen(
                list(commands[0].resolved_command),
                stdout=subprocess.PIPE,
                stderr=stderr_handle,
            )
            sort_process = subprocess.Popen(
                list(commands[1].resolved_command),
                stdin=minimap_process.stdout,
                stdout=stdout_handle,
                stderr=stderr_handle,
            )
            if minimap_process.stdout is not None:
                minimap_process.stdout.close()

            sort_code = sort_process.wait()
            minimap_code = minimap_process.wait()
        return minimap_code == 0 and sort_code == 0 and _is_nonempty_file(output_bam)
    except OSError as exc:
        append_log(stderr_log, str(exc))
        return False
    finally:
        for process in (minimap_process, sort_process):
            if process is not None and process.poll() is None:
                process.kill()


def _run_samtools_index(
    bam: Path,
    *,
    execution: dict[str, object] | None,
    stdout_log: Path,
    stderr_log: Path,
) -> bool:
    command = resolve_command(
        ["samtools", "index", str(bam)],
        execution,
        path_hints=(bam,),
        workdir=bam.parent,
    )
    return _run_command(command, stdout_log=stdout_log, stderr_log=stderr_log) is not None


def _run_samtools_flagstat(
    bam: Path,
    *,
    execution: dict[str, object] | None,
    stdout_log: Path,
    stderr_log: Path,
) -> str | None:
    command = resolve_command(
        ["samtools", "flagstat", str(bam)],
        execution,
        path_hints=(bam,),
        workdir=bam.parent,
    )
    result = _run_command(
        command,
        stdout_log=stdout_log,
        stderr_log=stderr_log,
        capture=True,
    )
    return result.stdout if result is not None else None


def display_longread_stats(stats: dict[str, Any]) -> None:
    table = Table(title=t("longread_stats_title"), header_style="bold cyan")
    table.add_column(t("longread_stats_metric"), style="bold")
    table.add_column(t("longread_stats_value"), justify="right", style="magenta")
    rows = (
        (t("longread_metric_format"), stats.get("input_format", "-")),
        (t("longread_metric_reads"), f"{int(stats.get('read_count', 0)):,}"),
        (t("longread_metric_bases"), f"{int(stats.get('total_bases', 0)):,}"),
        (t("longread_metric_mean_length"), f"{float(stats.get('mean_read_length', 0.0)):.2f}"),
        (t("longread_metric_n50"), f"{int(stats.get('n50', 0)):,}"),
        (t("longread_metric_mapping_rate"), f"{float(stats.get('mapping_rate', 0.0)):.2%}"),
    )
    for label, value in rows:
        table.add_row(label, str(value))
    if stats.get("avg_quality") is not None:
        table.add_row(t("longread_metric_avg_quality"), f"{float(stats['avg_quality']):.2f}")
        table.add_row(t("longread_metric_q20"), f"{float(stats['q20_ratio']):.2%}")
        table.add_row(t("longread_metric_q30"), f"{float(stats['q30_ratio']):.2%}")
    console.print(table)


def run_longread_pipeline(
    ref: Path,
    reads: Path,
    *,
    output: Path | None = None,
    outdir: Path | None = None,
    preset: str = "map-ont",
    threads: int = 1,
    sample_id: str | None = None,
    resume: bool = False,
    execution: dict[str, object] | None = None,
    cli_mode: bool = False,
    skip_preflight: bool = False,
) -> dict[str, Any] | None:
    """Run long-read QC, minimap2 alignment, BAM indexing, and summaries."""
    if preset not in LONGREAD_PRESETS:
        raise ValueError(f"unsupported minimap2 preset: {preset}")
    if threads <= 0:
        raise ValueError("threads must be positive")

    execution_payload = execution or {
        "profile": "local",
        "backend": "system",
        "conda_env": None,
        "container_image": None,
        "resources": {"threads": threads},
        "source": "default",
    }
    if not skip_preflight:
        if not preflight_check(
            LONGREAD_REQUIRED_TOOLS,
            backend=str(execution_payload.get("backend", "system")),
            conda_env=str(execution_payload["conda_env"]) if execution_payload.get("conda_env") else None,
            container_image=(
                str(execution_payload["container_image"])
                if execution_payload.get("container_image")
                else None
            ),
            cli_mode=cli_mode,
        ):
            return None

    layout = create_run_layout("longread", reads, outdir=outdir)
    output_bam = resolve_result_path(layout, output, f"{reads.stem}.longread.sorted.bam")
    bai_path = output_bam.with_suffix(output_bam.suffix + ".bai")
    flagstat_path = layout.results_dir / f"{output_bam.stem}.flagstat.txt"
    qc_summary_path = layout.results_dir / "longread_qc.json"
    summary_path = layout.results_dir / "longread_summary.json"
    started_at = utc_now_iso()
    existing_metadata = read_metadata(layout)
    input_details = collect_input_details({"ref": ref, "reads": reads})
    tool_versions = collect_tool_versions(LONGREAD_REQUIRED_TOOLS)
    resume_context_matches = _resume_context_matches(
        existing_metadata,
        ref=ref,
        reads=reads,
        preset=preset,
        sample_id=sample_id,
        input_details=input_details,
    )
    steps = init_steps(
        [
            LONGREAD_STEP_QC,
            LONGREAD_STEP_MAP,
            LONGREAD_STEP_BAM_INDEX,
            LONGREAD_STEP_FLAGSTAT,
            LONGREAD_STEP_SUMMARY,
        ],
        existing_metadata.get("steps") if resume_context_matches else None,
    )
    failure_summary = ""
    failure_details: dict[str, Any] = {}
    workflow_stats: dict[str, Any] = {}

    outputs = {
        "root": str(layout.root),
        "bam": str(output_bam),
        "bai": str(bai_path),
        "flagstat": str(flagstat_path),
        "qc_summary": str(qc_summary_path),
        "summary": str(summary_path),
    }

    def persist(status: str, *, completed_at: str | None = None) -> None:
        extra: dict[str, Any] = {
            "steps": steps,
            "resume_used": resume,
            "input_details": input_details,
            "tool_versions": tool_versions,
            "failure_summary": failure_summary,
            "failure_details": failure_details,
        }
        if workflow_stats:
            extra["stats"] = workflow_stats
            extra["summary"] = workflow_stats
        write_metadata(
            layout,
            status=status,
            command="longread",
            parameters={
                "preset": preset,
                "threads": threads,
                "sample_id": sample_id,
                "resume": resume,
                "execution": execution_payload,
            },
            inputs={"ref": str(ref), "reads": str(reads)},
            outputs=outputs,
            started_at=started_at,
            completed_at=completed_at,
            extra=extra,
        )

    def fail(step_name: str, command: str, fallback: str) -> None:
        nonlocal failure_summary, failure_details
        failure_summary = build_failure_summary(
            step_name,
            stderr_log=layout.stderr_log,
            fallback=fallback,
        )
        failure_details = build_failure_details(
            step_name=step_name,
            command=command,
            layout=layout,
            error=failure_summary,
        )
        set_step_state(steps, step_name, STEP_FAILED, error=failure_summary)
        persist("failed", completed_at=utc_now_iso())

    persist("running")
    console.print(Panel(t("longread_pipeline_start", file=str(reads)), style="bold magenta"))

    qc_command = f"bioflow internal longread-stats {reads}"
    if resume and resume_context_matches and step_resume_ready(
        existing_metadata,
        LONGREAD_STEP_QC,
        validator=lambda: _qc_summary_ready(qc_summary_path),
        required_outputs=("qc_summary",),
        current_execution=execution_payload,
    ):
        qc_stats = _read_json_mapping(qc_summary_path)
        set_step_state(
            steps,
            LONGREAD_STEP_QC,
            STEP_SKIPPED,
            outputs={"qc_summary": str(qc_summary_path)},
            note="reused existing output",
        )
    else:
        set_step_state(
            steps,
            LONGREAD_STEP_QC,
            STEP_RUNNING,
            backend="python",
            raw_command=qc_command,
            resolved_command=qc_command,
        )
        persist("running")
        try:
            qc_stats = calculate_longread_stats(reads)
            _write_json(qc_summary_path, qc_stats)
        except (OSError, UnicodeError, ValueError) as exc:
            append_log(layout.stderr_log, str(exc))
            fail(LONGREAD_STEP_QC, qc_command, str(exc))
            return None
        set_step_state(
            steps,
            LONGREAD_STEP_QC,
            STEP_SUCCESS,
            outputs={"qc_summary": str(qc_summary_path)},
            backend="python",
            raw_command=qc_command,
            resolved_command=qc_command,
        )
    workflow_stats.update(qc_stats)
    persist("running")

    map_commands = resolve_pipeline_commands(
        [
            ["minimap2", "-a", "-x", preset, "-t", str(threads), str(ref), str(reads)],
            ["samtools", "sort", "-@", str(threads), "-o", str(output_bam), "-"],
        ],
        execution_payload,
        path_hints=(ref, reads, output_bam),
        workdir=output_bam.parent,
    )
    map_raw, map_resolved = summarize_commands(map_commands, separator=" | ")
    if resume and resume_context_matches and step_resume_ready(
        existing_metadata,
        LONGREAD_STEP_MAP,
        validator=lambda: _is_nonempty_file(output_bam),
        required_outputs=("bam",),
        current_execution=execution_payload,
    ):
        set_step_state(
            steps,
            LONGREAD_STEP_MAP,
            STEP_SKIPPED,
            outputs={"bam": str(output_bam)},
            note="reused existing output",
        )
    else:
        set_step_state(
            steps,
            LONGREAD_STEP_MAP,
            STEP_RUNNING,
            backend=map_commands[0].backend,
            raw_command=map_raw,
            resolved_command=map_resolved,
            environment_fingerprint=map_commands[0].environment_fingerprint,
        )
        persist("running")
        if not _run_minimap2_pipe_sort(
            ref,
            reads,
            output_bam,
            preset=preset,
            threads=threads,
            execution=execution_payload,
            stdout_log=layout.stdout_log,
            stderr_log=layout.stderr_log,
        ):
            fail(LONGREAD_STEP_MAP, map_resolved, "minimap2 alignment failed")
            return None
        set_step_state(
            steps,
            LONGREAD_STEP_MAP,
            STEP_SUCCESS,
            outputs={"bam": str(output_bam)},
            backend=map_commands[0].backend,
            raw_command=map_raw,
            resolved_command=map_resolved,
            environment_fingerprint=map_commands[0].environment_fingerprint,
        )
    persist("running")
    map_reused = steps[LONGREAD_STEP_MAP].get("status") == STEP_SKIPPED

    index_command = resolve_command(
        ["samtools", "index", str(output_bam)],
        execution_payload,
        path_hints=(output_bam,),
        workdir=output_bam.parent,
    )
    if resume and resume_context_matches and map_reused and step_resume_ready(
        existing_metadata,
        LONGREAD_STEP_BAM_INDEX,
        validator=lambda: _is_nonempty_file(bai_path),
        required_outputs=("bai",),
        current_execution=execution_payload,
    ):
        set_step_state(
            steps,
            LONGREAD_STEP_BAM_INDEX,
            STEP_SKIPPED,
            outputs={"bai": str(bai_path)},
            note="reused existing output",
        )
    else:
        set_step_state(
            steps,
            LONGREAD_STEP_BAM_INDEX,
            STEP_RUNNING,
            backend=index_command.backend,
            raw_command=stringify_command(index_command.raw_command),
            resolved_command=stringify_command(index_command.resolved_command),
            environment_fingerprint=index_command.environment_fingerprint,
        )
        persist("running")
        if not _run_samtools_index(
            output_bam,
            execution=execution_payload,
            stdout_log=layout.stdout_log,
            stderr_log=layout.stderr_log,
        ):
            fail(
                LONGREAD_STEP_BAM_INDEX,
                stringify_command(index_command.resolved_command),
                "BAM indexing failed",
            )
            return None
        set_step_state(
            steps,
            LONGREAD_STEP_BAM_INDEX,
            STEP_SUCCESS,
            outputs={"bai": str(bai_path)},
            backend=index_command.backend,
            raw_command=stringify_command(index_command.raw_command),
            resolved_command=stringify_command(index_command.resolved_command),
            environment_fingerprint=index_command.environment_fingerprint,
        )
    persist("running")

    flagstat_command = resolve_command(
        ["samtools", "flagstat", str(output_bam)],
        execution_payload,
        path_hints=(output_bam,),
        workdir=output_bam.parent,
    )
    if resume and resume_context_matches and map_reused and step_resume_ready(
        existing_metadata,
        LONGREAD_STEP_FLAGSTAT,
        validator=lambda: _flagstat_ready(flagstat_path),
        required_outputs=("flagstat",),
        current_execution=execution_payload,
    ):
        flagstat_text = flagstat_path.read_text(encoding="utf-8")
        set_step_state(
            steps,
            LONGREAD_STEP_FLAGSTAT,
            STEP_SKIPPED,
            outputs={"flagstat": str(flagstat_path)},
            note="reused existing output",
        )
    else:
        set_step_state(
            steps,
            LONGREAD_STEP_FLAGSTAT,
            STEP_RUNNING,
            backend=flagstat_command.backend,
            raw_command=stringify_command(flagstat_command.raw_command),
            resolved_command=stringify_command(flagstat_command.resolved_command),
            environment_fingerprint=flagstat_command.environment_fingerprint,
        )
        persist("running")
        flagstat_text = _run_samtools_flagstat(
            output_bam,
            execution=execution_payload,
            stdout_log=layout.stdout_log,
            stderr_log=layout.stderr_log,
        )
        if flagstat_text is None:
            fail(
                LONGREAD_STEP_FLAGSTAT,
                stringify_command(flagstat_command.resolved_command),
                "samtools flagstat failed",
            )
            return None
        flagstat_path.write_text(flagstat_text, encoding="utf-8")
        set_step_state(
            steps,
            LONGREAD_STEP_FLAGSTAT,
            STEP_SUCCESS,
            outputs={"flagstat": str(flagstat_path)},
            backend=flagstat_command.backend,
            raw_command=stringify_command(flagstat_command.raw_command),
            resolved_command=stringify_command(flagstat_command.resolved_command),
            environment_fingerprint=flagstat_command.environment_fingerprint,
        )
    persist("running")

    alignment_stats = parse_flagstat(flagstat_text)
    workflow_stats.update(
        {
            "aligned_records": int(alignment_stats["total"]),
            "mapped_reads": int(alignment_stats["mapped"]),
            "unmapped_reads": int(alignment_stats["unmapped"]),
            "mapping_rate": float(alignment_stats["mapping_rate"]),
            "secondary_alignments": int(alignment_stats["secondary"]),
            "supplementary_alignments": int(alignment_stats["supplementary"]),
        }
    )
    summary_payload = {
        "workflow": "longread",
        "sample_id": sample_id,
        "preset": preset,
        "input": str(reads),
        "reference": str(ref),
        "stats": workflow_stats,
        "outputs": outputs,
    }
    summary_command = f"bioflow internal longread-summary {summary_path}"
    upstream_reused = all(
        isinstance(steps.get(step_name), dict)
        and steps[step_name].get("status") == STEP_SKIPPED
        for step_name in (
            LONGREAD_STEP_QC,
            LONGREAD_STEP_MAP,
            LONGREAD_STEP_BAM_INDEX,
            LONGREAD_STEP_FLAGSTAT,
        )
    )
    if resume and resume_context_matches and upstream_reused and step_resume_ready(
        existing_metadata,
        LONGREAD_STEP_SUMMARY,
        validator=lambda: _summary_ready(summary_path),
        required_outputs=("summary",),
        current_execution=execution_payload,
    ):
        previous_summary = _read_json_mapping(summary_path)
        previous_stats = previous_summary.get("stats")
        if isinstance(previous_stats, dict):
            workflow_stats = previous_stats
        set_step_state(
            steps,
            LONGREAD_STEP_SUMMARY,
            STEP_SKIPPED,
            outputs={"summary": str(summary_path)},
            note="reused existing output",
        )
    else:
        set_step_state(
            steps,
            LONGREAD_STEP_SUMMARY,
            STEP_RUNNING,
            backend="python",
            raw_command=summary_command,
            resolved_command=summary_command,
        )
        persist("running")
        _write_json(summary_path, summary_payload)
        set_step_state(
            steps,
            LONGREAD_STEP_SUMMARY,
            STEP_SUCCESS,
            outputs={"summary": str(summary_path)},
            backend="python",
            raw_command=summary_command,
            resolved_command=summary_command,
        )

    failure_summary = ""
    failure_details = {}
    persist("success", completed_at=utc_now_iso())
    display_longread_stats(workflow_stats)
    console.print(t("longread_pipeline_done", output=str(layout.root)), style="bold green")
    return {"run_dir": str(layout.root), **outputs, "stats": workflow_stats}


def _parse_threads(value: str | None) -> int:
    try:
        return max(1, int(str(value).strip()))
    except (TypeError, ValueError):
        return 1


def longread_menu() -> None:
    """Interactive long-read workflow entry."""
    console.print(Panel(t("longread_title"), style="bold magenta"))
    if not preflight_check(LONGREAD_REQUIRED_TOOLS, cli_mode=False):
        input(t("press_enter"))
        return

    try:
        ref_raw = questionary.path(t("longread_ref_prompt")).ask()
        reads_raw = questionary.path(t("longread_input_prompt")).ask()
    except KeyboardInterrupt:
        return
    if not ref_raw or not reads_raw:
        return
    ref = Path(ref_raw)
    reads = Path(reads_raw)
    for candidate in (ref, reads):
        if not candidate.exists():
            console.print(t("seq_file_not_found", path=str(candidate)), style="bold red")
            input(t("press_enter"))
            return

    try:
        preset = questionary.select(
            t("longread_preset_prompt"),
            choices=list(LONGREAD_PRESETS),
            default="map-ont",
        ).ask()
        threads_raw = questionary.text(t("longread_threads_prompt"), default="1").ask()
        output_raw = questionary.path(
            t("longread_output_prompt"),
            default=str(reads.parent / "longread_run"),
        ).ask()
    except KeyboardInterrupt:
        return
    if not preset or not output_raw:
        return

    run_root = Path(output_raw)
    resume = False
    if (run_root / "metadata.json").exists():
        try:
            resume = bool(
                questionary.confirm(
                    t("resume_detected_prompt", path=str(run_root)),
                    default=True,
                ).ask()
            )
        except KeyboardInterrupt:
            return

    run_longread_pipeline(
        ref,
        reads,
        outdir=run_root,
        preset=str(preset),
        threads=_parse_threads(threads_raw),
        resume=resume,
        skip_preflight=True,
    )
    input(t("press_enter"))

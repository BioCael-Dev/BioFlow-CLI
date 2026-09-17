"""BioFlow-CLI variant calling workflow based on SAMtools and BCFtools."""

from __future__ import annotations

import gzip
import json
import math
import subprocess
from pathlib import Path
from typing import Any, TextIO

import questionary
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from bioflow.execution import (
    ResolvedCommand,
    resolve_command,
    resolve_pipeline_commands,
    stringify_command,
    summarize_commands,
)
from bioflow.i18n import t
from bioflow.preflight import preflight_check
from bioflow.run_layout import (
    STEP_FAILED,
    STEP_PENDING,
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

VARIANT_REQUIRED_TOOLS = ("samtools", "bcftools")
VARIANT_CALLERS = ("bcftools",)
VARIANT_STEP_REFERENCE_INDEX = "reference_index"
VARIANT_STEP_BAM_INDEX = "bam_index"
VARIANT_STEP_CALL = "variant_call"
VARIANT_STEP_FILTER = "variant_filter"
VARIANT_STEP_VCF_INDEX = "vcf_index"
VARIANT_STEP_STATS = "bcftools_stats"
VARIANT_STEP_SUMMARY = "summary"


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
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


def _summary_ready(path: Path) -> bool:
    payload = _read_json_mapping(path)
    return payload.get("workflow") == "variant" and isinstance(payload.get("stats"), dict)


def _open_vcf_text(path: Path) -> TextIO:
    if path.name.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def _classify_variant(ref: str, alt_field: str) -> str:
    """Classify one VCF record using all non-symbolic ALT alleles."""
    alts = [alt for alt in alt_field.split(",") if alt and alt != "."]
    if not alts or any(alt.startswith("<") or alt == "*" for alt in alts):
        return "other"
    if len(ref) == 1 and all(len(alt) == 1 for alt in alts):
        return "snp"
    if len(ref) > 1 and all(len(alt) == len(ref) for alt in alts):
        return "mnp"
    if any(len(alt) != len(ref) for alt in alts):
        return "indel"
    return "other"


def summarize_vcf(path: Path) -> dict[str, Any]:
    """Stream a VCF/VCF.GZ file and return stable record-level metrics."""
    stats: dict[str, Any] = {
        "total_variants": 0,
        "pass_variants": 0,
        "filtered_variants": 0,
        "snp_count": 0,
        "indel_count": 0,
        "mnp_count": 0,
        "other_variant_count": 0,
        "multiallelic_count": 0,
    }
    quality_sum = 0.0
    quality_count = 0
    with _open_vcf_text(path) as handle:
        for raw_line in handle:
            if not raw_line.strip() or raw_line.startswith("#"):
                continue
            columns = raw_line.rstrip("\n").split("\t")
            if len(columns) < 7:
                raise ValueError(f"invalid VCF record in {path}")
            ref, alt_field, qual_text, filter_text = columns[3], columns[4], columns[5], columns[6]
            stats["total_variants"] += 1
            if filter_text in {"PASS", "."}:
                stats["pass_variants"] += 1
            else:
                stats["filtered_variants"] += 1
            if "," in alt_field:
                stats["multiallelic_count"] += 1
            variant_type = _classify_variant(ref, alt_field)
            key = {
                "snp": "snp_count",
                "indel": "indel_count",
                "mnp": "mnp_count",
                "other": "other_variant_count",
            }[variant_type]
            stats[key] += 1
            if qual_text not in {"", "."}:
                try:
                    quality = float(qual_text)
                except ValueError as exc:
                    raise ValueError(f"invalid VCF QUAL value in {path}: {qual_text}") from exc
                if not math.isfinite(quality):
                    raise ValueError(f"invalid VCF QUAL value in {path}: {qual_text}")
                quality_sum += quality
                quality_count += 1
    stats["mean_quality"] = quality_sum / quality_count if quality_count else None
    return stats


def _resume_context_matches(
    metadata: dict[str, Any],
    *,
    ref: Path,
    bam: Path,
    caller: str,
    min_qual: float,
    min_depth: int,
    sample_id: str | None,
    output_vcf: Path,
    input_details: dict[str, Any],
) -> bool:
    if metadata.get("workflow") != "variant":
        return False
    inputs = metadata.get("inputs")
    parameters = metadata.get("parameters")
    outputs = metadata.get("outputs")
    previous_details = metadata.get("input_details")
    if (
        not isinstance(inputs, dict)
        or not isinstance(parameters, dict)
        or not isinstance(outputs, dict)
        or not isinstance(previous_details, dict)
    ):
        return False
    if inputs.get("ref") != str(ref) or inputs.get("bam") != str(bam):
        return False
    if parameters.get("caller") != caller:
        return False
    try:
        previous_min_qual = float(parameters.get("min_qual", -1))
        previous_min_depth = int(parameters.get("min_depth", -1))
    except (TypeError, ValueError):
        return False
    if previous_min_qual != float(min_qual):
        return False
    if previous_min_depth != int(min_depth):
        return False
    if parameters.get("sample_id") != sample_id:
        return False
    if outputs.get("vcf") != str(output_vcf):
        return False
    for key in ("ref", "bam"):
        previous = previous_details.get(key)
        current = input_details.get(key)
        if not isinstance(previous, dict) or not isinstance(current, dict):
            return False
        if previous.get("sha256") != current.get("sha256"):
            return False
    return True


def _run_command(
    command: ResolvedCommand,
    *,
    stdout_log: Path,
    stderr_log: Path,
    stdout_path: Path | None = None,
) -> bool:
    try:
        result = subprocess.run(
            list(command.resolved_command),
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        append_log(stdout_log, exc.stdout or "")
        append_log(stderr_log, exc.stderr or str(exc))
        return False
    except FileNotFoundError as exc:
        append_log(stderr_log, str(exc))
        return False
    if stdout_path is not None:
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        stdout_path.write_text(result.stdout or "", encoding="utf-8")
    append_log(stdout_log, result.stdout or "")
    append_log(stderr_log, result.stderr or "")
    return True


def _run_mpileup_call_pipeline(
    ref: Path,
    bam: Path,
    raw_bcf: Path,
    *,
    threads: int,
    execution: dict[str, object],
    stdout_log: Path,
    stderr_log: Path,
) -> bool:
    raw_commands = [
        [
            "bcftools",
            "mpileup",
            "--threads",
            str(threads),
            "-Ou",
            "-a",
            "FORMAT/DP",
            "-f",
            str(ref),
            str(bam),
        ],
        [
            "bcftools",
            "call",
            "--threads",
            str(threads),
            "-mv",
            "-Ob",
            "-o",
            str(raw_bcf),
        ],
    ]
    commands = resolve_pipeline_commands(
        raw_commands,
        execution,
        path_hints=(ref, bam, raw_bcf),
        workdir=raw_bcf.parent,
    )
    mpileup_proc: subprocess.Popen[bytes] | None = None
    call_proc: subprocess.Popen[bytes] | None = None
    try:
        mpileup_proc = subprocess.Popen(
            list(commands[0].resolved_command),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        call_proc = subprocess.Popen(
            list(commands[1].resolved_command),
            stdin=mpileup_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if mpileup_proc.stdout is not None:
            mpileup_proc.stdout.close()
        call_stdout, call_stderr = call_proc.communicate()
        mpileup_stderr = mpileup_proc.stderr.read() if mpileup_proc.stderr is not None else b""
        mpileup_code = mpileup_proc.wait()
        call_code = call_proc.returncode
        append_log(stdout_log, call_stdout.decode("utf-8", errors="replace"))
        append_log(stderr_log, mpileup_stderr.decode("utf-8", errors="replace"))
        append_log(stderr_log, call_stderr.decode("utf-8", errors="replace"))
        return mpileup_code == 0 and call_code == 0
    except FileNotFoundError as exc:
        append_log(stderr_log, str(exc))
        return False
    finally:
        for process in (mpileup_proc, call_proc):
            if process is not None and process.poll() is None:
                process.kill()


def display_variant_stats(stats: dict[str, Any]) -> None:
    table = Table(title=t("variant_stats_title"), header_style="bold cyan")
    table.add_column(t("variant_stats_metric"), style="bold")
    table.add_column(t("variant_stats_value"), justify="right", style="magenta")
    rows = (
        (t("variant_metric_total"), stats.get("total_variants", 0)),
        (t("variant_metric_pass"), stats.get("pass_variants", 0)),
        (t("variant_metric_filtered"), stats.get("filtered_variants", 0)),
        (t("variant_metric_snp"), stats.get("snp_count", 0)),
        (t("variant_metric_indel"), stats.get("indel_count", 0)),
        (t("variant_metric_mnp"), stats.get("mnp_count", 0)),
    )
    for label, value in rows:
        table.add_row(label, f"{int(value):,}")
    if stats.get("mean_quality") is not None:
        table.add_row(t("variant_metric_mean_quality"), f"{float(stats['mean_quality']):.2f}")
    console.print(table)


def run_variant_pipeline(
    ref: Path,
    bam: Path,
    *,
    output: Path | None = None,
    outdir: Path | None = None,
    caller: str = "bcftools",
    min_qual: float = 20.0,
    min_depth: int = 1,
    threads: int = 1,
    sample_id: str | None = None,
    resume: bool = False,
    execution: dict[str, object] | None = None,
    cli_mode: bool = False,
    skip_preflight: bool = False,
) -> dict[str, Any] | None:
    """Run reference/BAM preparation, calling, filtering, indexing, and summaries."""
    if caller not in VARIANT_CALLERS:
        raise ValueError(f"unsupported variant caller: {caller}")
    if min_qual <= 0:
        raise ValueError("min_qual must be positive")
    if min_depth <= 0:
        raise ValueError("min_depth must be positive")
    if threads <= 0:
        raise ValueError("threads must be positive")
    if output is not None and not output.name.endswith(".vcf.gz"):
        raise ValueError("variant output must end with .vcf.gz")

    ref = ref.expanduser().resolve()
    bam = bam.expanduser().resolve()

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
            VARIANT_REQUIRED_TOOLS,
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

    layout = create_run_layout("variant", bam, outdir=outdir)
    default_stem = sample_id or bam.stem
    output_vcf = resolve_result_path(layout, output, f"{default_stem}.variants.vcf.gz")
    raw_bcf = layout.results_dir / f"{default_stem}.raw.bcf"
    vcf_index = Path(f"{output_vcf}.tbi")
    bcftools_stats = layout.results_dir / f"{default_stem}.bcftools.stats.txt"
    summary_path = layout.results_dir / "variant_summary.json"
    reference_index = Path(f"{ref}.fai")
    bam_index = Path(f"{bam}.bai")

    started_at = utc_now_iso()
    existing_metadata = read_metadata(layout)
    input_details = collect_input_details({"ref": ref, "bam": bam})
    tool_versions = collect_tool_versions(VARIANT_REQUIRED_TOOLS)
    resume_context_matches = _resume_context_matches(
        existing_metadata,
        ref=ref,
        bam=bam,
        caller=caller,
        min_qual=min_qual,
        min_depth=min_depth,
        sample_id=sample_id,
        output_vcf=output_vcf,
        input_details=input_details,
    )
    step_order = [
        VARIANT_STEP_REFERENCE_INDEX,
        VARIANT_STEP_BAM_INDEX,
        VARIANT_STEP_CALL,
        VARIANT_STEP_FILTER,
        VARIANT_STEP_VCF_INDEX,
        VARIANT_STEP_STATS,
        VARIANT_STEP_SUMMARY,
    ]
    steps = init_steps(
        step_order,
        existing_metadata.get("steps") if resume_context_matches else None,
    )
    failure_summary = ""
    failure_details: dict[str, Any] = {}
    workflow_stats: dict[str, Any] = {}
    outputs = {
        "root": str(layout.root),
        "reference_index": str(reference_index),
        "bam_index": str(bam_index),
        "bcf": str(raw_bcf),
        "vcf": str(output_vcf),
        "vcf_index": str(vcf_index),
        "bcftools_stats": str(bcftools_stats),
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
            command="variant",
            parameters={
                "caller": caller,
                "min_qual": min_qual,
                "min_depth": min_depth,
                "threads": threads,
                "sample_id": sample_id,
                "resume": resume,
                "execution": execution_payload,
            },
            inputs={"ref": str(ref), "bam": str(bam), "sample_id": sample_id},
            outputs=outputs,
            started_at=started_at,
            completed_at=completed_at,
            extra=extra,
        )

    def invalidate_after(step_name: str) -> None:
        index = step_order.index(step_name)
        for downstream in step_order[index + 1 :]:
            steps[downstream] = {"status": STEP_PENDING}

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

    def run_external_step(
        step_name: str,
        command: ResolvedCommand,
        *,
        output_payload: dict[str, Any],
        failure_text: str,
        stdout_path: Path | None = None,
    ) -> bool:
        invalidate_after(step_name)
        raw_text = stringify_command(command.raw_command)
        resolved_text = stringify_command(command.resolved_command)
        set_step_state(
            steps,
            step_name,
            STEP_RUNNING,
            backend=command.backend,
            raw_command=raw_text,
            resolved_command=resolved_text,
            environment_fingerprint=command.environment_fingerprint,
        )
        persist("running")
        if not _run_command(
            command,
            stdout_log=layout.stdout_log,
            stderr_log=layout.stderr_log,
            stdout_path=stdout_path,
        ):
            fail(step_name, resolved_text, failure_text)
            return False
        set_step_state(
            steps,
            step_name,
            STEP_SUCCESS,
            outputs=output_payload,
            backend=command.backend,
            raw_command=raw_text,
            resolved_command=resolved_text,
            environment_fingerprint=command.environment_fingerprint,
        )
        persist("running")
        return True

    persist("running")
    console.print(Panel(t("variant_pipeline_start", file=str(bam)), style="bold magenta"))

    reference_command = resolve_command(
        ["samtools", "faidx", str(ref)],
        execution_payload,
        path_hints=(ref, reference_index),
        workdir=ref.parent,
    )
    if resume and resume_context_matches and step_resume_ready(
        existing_metadata,
        VARIANT_STEP_REFERENCE_INDEX,
        validator=lambda: _is_nonempty_file(reference_index),
        required_outputs=("reference_index",),
        current_execution=execution_payload,
    ):
        set_step_state(
            steps,
            VARIANT_STEP_REFERENCE_INDEX,
            STEP_SKIPPED,
            outputs={"reference_index": str(reference_index)},
            note="reused existing output",
        )
    elif not run_external_step(
        VARIANT_STEP_REFERENCE_INDEX,
        reference_command,
        output_payload={"reference_index": str(reference_index)},
        failure_text="reference indexing failed",
    ):
        return None

    bam_index_command = resolve_command(
        ["samtools", "index", "-@", str(threads), str(bam)],
        execution_payload,
        path_hints=(bam, bam_index),
        workdir=bam.parent,
    )
    if resume and resume_context_matches and step_resume_ready(
        existing_metadata,
        VARIANT_STEP_BAM_INDEX,
        validator=lambda: _is_nonempty_file(bam_index),
        required_outputs=("bam_index",),
        current_execution=execution_payload,
    ):
        set_step_state(
            steps,
            VARIANT_STEP_BAM_INDEX,
            STEP_SKIPPED,
            outputs={"bam_index": str(bam_index)},
            note="reused existing output",
        )
    elif not run_external_step(
        VARIANT_STEP_BAM_INDEX,
        bam_index_command,
        output_payload={"bam_index": str(bam_index)},
        failure_text="BAM indexing failed",
    ):
        return None

    index_steps_reused = all(
        steps[name].get("status") == STEP_SKIPPED
        for name in (VARIANT_STEP_REFERENCE_INDEX, VARIANT_STEP_BAM_INDEX)
    )
    call_commands = resolve_pipeline_commands(
        [
            [
                "bcftools",
                "mpileup",
                "--threads",
                str(threads),
                "-Ou",
                "-a",
                "FORMAT/DP",
                "-f",
                str(ref),
                str(bam),
            ],
            [
                "bcftools",
                "call",
                "--threads",
                str(threads),
                "-mv",
                "-Ob",
                "-o",
                str(raw_bcf),
            ],
        ],
        execution_payload,
        path_hints=(ref, bam, raw_bcf),
        workdir=layout.results_dir,
    )
    call_raw, call_resolved = summarize_commands(call_commands, separator=" | ")
    if resume and resume_context_matches and index_steps_reused and step_resume_ready(
        existing_metadata,
        VARIANT_STEP_CALL,
        validator=lambda: _is_nonempty_file(raw_bcf),
        required_outputs=("bcf",),
        current_execution=execution_payload,
    ):
        set_step_state(
            steps,
            VARIANT_STEP_CALL,
            STEP_SKIPPED,
            outputs={"bcf": str(raw_bcf)},
            note="reused existing output",
        )
    else:
        invalidate_after(VARIANT_STEP_CALL)
        set_step_state(
            steps,
            VARIANT_STEP_CALL,
            STEP_RUNNING,
            backend=call_commands[0].backend,
            raw_command=call_raw,
            resolved_command=call_resolved,
            environment_fingerprint=call_commands[0].environment_fingerprint,
        )
        persist("running")
        if not _run_mpileup_call_pipeline(
            ref,
            bam,
            raw_bcf,
            threads=threads,
            execution=execution_payload,
            stdout_log=layout.stdout_log,
            stderr_log=layout.stderr_log,
        ):
            fail(VARIANT_STEP_CALL, call_resolved, "variant calling failed")
            return None
        set_step_state(
            steps,
            VARIANT_STEP_CALL,
            STEP_SUCCESS,
            outputs={"bcf": str(raw_bcf)},
            backend=call_commands[0].backend,
            raw_command=call_raw,
            resolved_command=call_resolved,
            environment_fingerprint=call_commands[0].environment_fingerprint,
        )
        persist("running")

    filter_expression = f"QUAL<{min_qual:g} || FORMAT/DP<{min_depth}"
    filter_command = resolve_command(
        [
            "bcftools",
            "filter",
            "--threads",
            str(threads),
            "-s",
            "LowQual",
            "-e",
            filter_expression,
            "-Oz",
            "-o",
            str(output_vcf),
            str(raw_bcf),
        ],
        execution_payload,
        path_hints=(raw_bcf, output_vcf),
        workdir=layout.results_dir,
    )
    call_reused = steps[VARIANT_STEP_CALL].get("status") == STEP_SKIPPED
    if resume and resume_context_matches and call_reused and step_resume_ready(
        existing_metadata,
        VARIANT_STEP_FILTER,
        validator=lambda: _is_nonempty_file(output_vcf),
        required_outputs=("vcf",),
        current_execution=execution_payload,
    ):
        set_step_state(
            steps,
            VARIANT_STEP_FILTER,
            STEP_SKIPPED,
            outputs={"vcf": str(output_vcf)},
            note="reused existing output",
        )
    elif not run_external_step(
        VARIANT_STEP_FILTER,
        filter_command,
        output_payload={"vcf": str(output_vcf)},
        failure_text="variant filtering failed",
    ):
        return None

    filter_reused = steps[VARIANT_STEP_FILTER].get("status") == STEP_SKIPPED
    vcf_index_command = resolve_command(
        ["bcftools", "index", "--tbi", "--force", str(output_vcf)],
        execution_payload,
        path_hints=(output_vcf, vcf_index),
        workdir=layout.results_dir,
    )
    if resume and resume_context_matches and filter_reused and step_resume_ready(
        existing_metadata,
        VARIANT_STEP_VCF_INDEX,
        validator=lambda: _is_nonempty_file(vcf_index),
        required_outputs=("vcf_index",),
        current_execution=execution_payload,
    ):
        set_step_state(
            steps,
            VARIANT_STEP_VCF_INDEX,
            STEP_SKIPPED,
            outputs={"vcf_index": str(vcf_index)},
            note="reused existing output",
        )
    elif not run_external_step(
        VARIANT_STEP_VCF_INDEX,
        vcf_index_command,
        output_payload={"vcf_index": str(vcf_index)},
        failure_text="VCF indexing failed",
    ):
        return None

    upstream_reused = all(
        steps[name].get("status") == STEP_SKIPPED
        for name in (
            VARIANT_STEP_REFERENCE_INDEX,
            VARIANT_STEP_BAM_INDEX,
            VARIANT_STEP_CALL,
            VARIANT_STEP_FILTER,
            VARIANT_STEP_VCF_INDEX,
        )
    )
    stats_command = resolve_command(
        ["bcftools", "stats", str(output_vcf)],
        execution_payload,
        path_hints=(output_vcf, bcftools_stats),
        workdir=layout.results_dir,
    )
    if resume and resume_context_matches and upstream_reused and step_resume_ready(
        existing_metadata,
        VARIANT_STEP_STATS,
        validator=lambda: _is_nonempty_file(bcftools_stats),
        required_outputs=("bcftools_stats",),
        current_execution=execution_payload,
    ):
        set_step_state(
            steps,
            VARIANT_STEP_STATS,
            STEP_SKIPPED,
            outputs={"bcftools_stats": str(bcftools_stats)},
            note="reused existing output",
        )
    elif not run_external_step(
        VARIANT_STEP_STATS,
        stats_command,
        output_payload={"bcftools_stats": str(bcftools_stats)},
        failure_text="BCFtools statistics failed",
        stdout_path=bcftools_stats,
    ):
        return None

    summary_command = f"bioflow internal variant-summary {output_vcf}"
    all_external_reused = all(
        steps[name].get("status") == STEP_SKIPPED
        for name in step_order[:-1]
    )
    if resume and resume_context_matches and all_external_reused and step_resume_ready(
        existing_metadata,
        VARIANT_STEP_SUMMARY,
        validator=lambda: _summary_ready(summary_path),
        required_outputs=("summary",),
        current_execution=execution_payload,
    ):
        previous_summary = _read_json_mapping(summary_path)
        previous_stats = previous_summary.get("stats")
        if isinstance(previous_stats, dict):
            workflow_stats.update(previous_stats)
        set_step_state(
            steps,
            VARIANT_STEP_SUMMARY,
            STEP_SKIPPED,
            outputs={"summary": str(summary_path)},
            note="reused existing output",
        )
    else:
        set_step_state(
            steps,
            VARIANT_STEP_SUMMARY,
            STEP_RUNNING,
            backend="python",
            raw_command=summary_command,
            resolved_command=summary_command,
        )
        persist("running")
        try:
            workflow_stats.update(summarize_vcf(output_vcf))
        except (OSError, UnicodeError, ValueError) as exc:
            append_log(layout.stderr_log, str(exc))
            fail(VARIANT_STEP_SUMMARY, summary_command, str(exc))
            return None
        _write_json(
            summary_path,
            {
                "workflow": "variant",
                "sample_id": sample_id,
                "caller": caller,
                "filters": {"min_qual": min_qual, "min_depth": min_depth},
                "reference": str(ref),
                "bam": str(bam),
                "stats": workflow_stats,
                "outputs": outputs,
            },
        )
        set_step_state(
            steps,
            VARIANT_STEP_SUMMARY,
            STEP_SUCCESS,
            outputs={"summary": str(summary_path)},
            backend="python",
            raw_command=summary_command,
            resolved_command=summary_command,
        )

    failure_summary = ""
    failure_details = {}
    persist("success", completed_at=utc_now_iso())
    display_variant_stats(workflow_stats)
    console.print(t("variant_pipeline_done", output=str(layout.root)), style="bold green")
    return {"run_dir": str(layout.root), **outputs, "stats": workflow_stats}


def _parse_positive_int(value: str | None, default: int) -> int:
    try:
        return max(1, int(str(value).strip()))
    except (TypeError, ValueError):
        return default


def _parse_positive_float(value: str | None, default: float) -> float:
    try:
        return max(0.000001, float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def variant_menu() -> None:
    """Interactive variant calling workflow entry."""
    console.print(Panel(t("variant_title"), style="bold magenta"))
    if not preflight_check(VARIANT_REQUIRED_TOOLS, cli_mode=False):
        input(t("press_enter"))
        return

    try:
        ref_raw = questionary.path(t("variant_ref_prompt")).ask()
        bam_raw = questionary.path(t("variant_bam_prompt")).ask()
    except KeyboardInterrupt:
        return
    if not ref_raw or not bam_raw:
        return
    ref = Path(ref_raw)
    bam = Path(bam_raw)
    for candidate in (ref, bam):
        if not candidate.is_file():
            console.print(t("seq_file_not_found", path=str(candidate)), style="bold red")
            input(t("press_enter"))
            return

    try:
        min_qual_raw = questionary.text(t("variant_min_qual_prompt"), default="20").ask()
        min_depth_raw = questionary.text(t("variant_min_depth_prompt"), default="1").ask()
        threads_raw = questionary.text(t("variant_threads_prompt"), default="1").ask()
        output_raw = questionary.path(
            t("variant_output_prompt"),
            default=str(bam.parent / "variant_run"),
        ).ask()
    except KeyboardInterrupt:
        return
    if not output_raw:
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

    run_variant_pipeline(
        ref,
        bam,
        outdir=run_root,
        min_qual=_parse_positive_float(min_qual_raw, 20.0),
        min_depth=_parse_positive_int(min_depth_raw, 1),
        threads=_parse_positive_int(threads_raw, 1),
        resume=resume,
        skip_preflight=True,
    )
    input(t("press_enter"))

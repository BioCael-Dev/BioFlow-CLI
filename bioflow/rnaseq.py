"""BioFlow-CLI RNA-seq workflow -- FastQC + Salmon quantification."""

from __future__ import annotations

import csv
import json
import subprocess
from pathlib import Path
from typing import Any

import questionary
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from bioflow.execution import ResolvedCommand, resolve_command, stringify_command, summarize_commands
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
    set_step_state,
    step_resume_ready,
    utc_now_iso,
    write_metadata,
)

console = Console()

RNASEQ_REQUIRED_TOOLS = get_workflow_manifest("rnaseq").required_tools
RNASEQ_STEP_FASTQC = "fastqc"
RNASEQ_STEP_INDEX = "salmon_index"
RNASEQ_STEP_QUANT = "salmon_quant"
RNASEQ_STEP_SUMMARY = "summary"


def _is_nonempty_file(path: Path) -> bool:
    return path.is_file() and path.stat().st_size > 0


def _index_ready(path: Path) -> bool:
    return path.is_dir() and any(path.iterdir())


def _fastqc_report_exists(input_file: Path, output_dir: Path) -> bool:
    stem = input_file.name
    for suffix in (".fastq.gz", ".fq.gz", ".fastq", ".fq", ".fasta.gz", ".fa.gz", ".fasta", ".fa"):
        if stem.lower().endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    return (output_dir / f"{stem}_fastqc.html").is_file()


def _quant_ready(quant_dir: Path) -> bool:
    return _is_nonempty_file(quant_dir / "quant.sf") and _is_nonempty_file(
        quant_dir / "aux_info" / "meta_info.json"
    )


def _summary_ready(path: Path) -> bool:
    if not _is_nonempty_file(path):
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return isinstance(payload, dict) and "transcript_count" in payload


def _run_command(
    command: ResolvedCommand,
    *,
    description: str,
    quiet: bool,
    stdout_log: Path,
    stderr_log: Path,
) -> bool:
    if not quiet:
        console.print(description, style="cyan")
    try:
        result = subprocess.run(
            list(command.resolved_command),
            check=True,
            capture_output=True,
            text=True,
        )
        append_log(stdout_log, result.stdout)
        append_log(stderr_log, result.stderr)
        return True
    except subprocess.CalledProcessError as exc:
        append_log(stdout_log, exc.stdout or "")
        append_log(stderr_log, exc.stderr or str(exc))
        if not quiet:
            console.print(t("rnaseq_step_failed", step=description, err=str(exc)), style="bold red")
        return False
    except OSError as exc:
        append_log(stderr_log, str(exc))
        if not quiet:
            console.print(t("rnaseq_step_failed", step=description, err=str(exc)), style="bold red")
        return False


def _validate_inputs(
    input_file: Path | None,
    input_r1: Path | None,
    input_r2: Path | None,
) -> tuple[bool, Path]:
    has_single = input_file is not None
    has_r1 = input_r1 is not None
    has_r2 = input_r2 is not None
    if has_single and (has_r1 or has_r2):
        raise ValueError("rnaseq cannot mix input with input_r1/input_r2")
    if has_r1 != has_r2:
        raise ValueError("rnaseq paired-end mode requires both input_r1 and input_r2")
    if not has_single and not (has_r1 and has_r2):
        raise ValueError("rnaseq requires input or input_r1/input_r2")
    anchor = input_file if input_file is not None else input_r1
    assert anchor is not None
    return has_r1 and has_r2, anchor


def _validate_reference(transcriptome: Path | None, index: Path | None) -> None:
    if transcriptome is not None and index is not None:
        raise ValueError("rnaseq cannot mix transcriptome with index")
    if transcriptome is None and index is None:
        raise ValueError("rnaseq requires transcriptome or index")


def _validate_design(
    *,
    group: str | None,
    condition: str | None,
    lane: str | None,
    replicate: int | None,
) -> None:
    """Validate optional sample-design fields used by direct and project runs."""
    if group is not None and not group.strip():
        raise ValueError("rnaseq group must be non-empty")
    if condition is not None and not condition.strip():
        raise ValueError("rnaseq condition must be non-empty")
    has_group = group is not None and bool(group.strip())
    has_condition = condition is not None and bool(condition.strip())
    if has_group != has_condition:
        raise ValueError("rnaseq requires group and condition together")
    if lane is not None and not lane.strip():
        raise ValueError("rnaseq lane must be non-empty")
    if replicate is not None and replicate <= 0:
        raise ValueError("rnaseq replicate must be positive")


def _summary_matches_design(
    path: Path,
    *,
    sample_id: str | None,
    group: str | None,
    condition: str | None,
    lane: str | None,
    replicate: int | None,
    library_type: str,
    input_mode: str,
) -> bool:
    """Return whether a reusable summary matches the current sample design."""
    if not _summary_ready(path):
        return False
    payload = _read_json(path)
    expected = {
        "sample_id": sample_id,
        "group": group,
        "condition": condition,
        "lane": lane,
        "replicate": replicate,
        "library_type": library_type,
        "input_mode": input_mode,
    }
    return all(payload.get(key) == value for key, value in expected.items())


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def summarize_salmon_quant(
    quant_dir: Path,
    *,
    sample_id: str | None = None,
    group: str | None = None,
    condition: str | None = None,
    lane: str | None = None,
    replicate: int | None = None,
    library_type: str = "A",
    input_mode: str,
) -> dict[str, Any]:
    """Parse Salmon outputs into stable run/report metrics."""
    quant_path = quant_dir / "quant.sf"
    transcript_count = 0
    expressed_transcripts = 0
    total_tpm = 0.0
    estimated_num_reads = 0.0
    with quant_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        required = {"Name", "Length", "EffectiveLength", "TPM", "NumReads"}
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            raise ValueError("invalid Salmon quant.sf header")
        for row in reader:
            transcript_count += 1
            tpm = float(row["TPM"])
            num_reads = float(row["NumReads"])
            total_tpm += tpm
            estimated_num_reads += num_reads
            if num_reads > 0:
                expressed_transcripts += 1

    meta = _read_json(quant_dir / "aux_info" / "meta_info.json")
    processed = meta.get("num_processed")
    mapped = meta.get("num_mapped")
    percent_mapped = meta.get("percent_mapped")
    mapping_rate: float | None = None
    if isinstance(percent_mapped, (int, float)):
        mapping_rate = float(percent_mapped) / 100.0
    elif isinstance(processed, (int, float)) and processed and isinstance(mapped, (int, float)):
        mapping_rate = float(mapped) / float(processed)

    return {
        "sample_id": sample_id,
        "group": group,
        "condition": condition,
        "lane": lane,
        "replicate": replicate,
        "input_mode": input_mode,
        "library_type": library_type,
        "inferred_library_type": meta.get("library_types"),
        "processed_fragments": processed,
        "mapped_fragments": mapped,
        "mapping_rate": mapping_rate,
        "transcript_count": transcript_count,
        "expressed_transcripts": expressed_transcripts,
        "total_tpm": total_tpm,
        "estimated_num_reads": estimated_num_reads,
    }


def display_rnaseq_summary(summary: dict[str, Any]) -> None:
    table = Table(title=t("rnaseq_summary_title"), show_header=True, header_style="bold cyan")
    table.add_column(t("rnaseq_summary_metric"))
    table.add_column(t("rnaseq_summary_value"), justify="right")
    rows = (
        (t("rnaseq_metric_processed"), summary.get("processed_fragments")),
        (t("rnaseq_metric_mapped"), summary.get("mapped_fragments")),
        (
            t("rnaseq_metric_mapping_rate"),
            f"{float(summary['mapping_rate']):.2%}" if summary.get("mapping_rate") is not None else None,
        ),
        (t("rnaseq_metric_transcripts"), summary.get("transcript_count")),
        (t("rnaseq_metric_expressed"), summary.get("expressed_transcripts")),
    )
    for label, value in rows:
        table.add_row(label, "-" if value is None else str(value))
    console.print(table)


def run_rnaseq_pipeline(
    transcriptome: Path | None,
    input_file: Path | None,
    *,
    index: Path | None = None,
    input_r1: Path | None = None,
    input_r2: Path | None = None,
    outdir: Path | None = None,
    threads: int = 1,
    library_type: str = "A",
    sample_id: str | None = None,
    group: str | None = None,
    condition: str | None = None,
    lane: str | None = None,
    replicate: int | None = None,
    resume: bool = False,
    execution: dict[str, object] | None = None,
    cli_mode: bool = False,
    skip_preflight: bool = False,
) -> dict[str, Any] | None:
    """Run FastQC and Salmon mapping-based transcript quantification."""
    paired_mode, anchor = _validate_inputs(input_file, input_r1, input_r2)
    _validate_reference(transcriptome, index)
    _validate_design(
        group=group,
        condition=condition,
        lane=lane,
        replicate=replicate,
    )
    if threads <= 0:
        raise ValueError("threads must be positive")
    if not library_type.strip():
        raise ValueError("library_type must be non-empty")

    execution_payload = execution or {
        "profile": "local",
        "backend": "system",
        "resources": {"threads": threads},
        "source": "default",
    }
    if not skip_preflight:
        if not preflight_check(
            RNASEQ_REQUIRED_TOOLS,
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
            return None

    layout = create_run_layout("rnaseq", anchor, outdir=outdir)
    started_at = utc_now_iso()
    fastqc_dir = layout.results_dir / "fastqc"
    fastqc_dir.mkdir(parents=True, exist_ok=True)
    resolved_index = index if index is not None else layout.results_dir / "salmon_index"
    quant_dir = layout.results_dir / "salmon_quant"
    summary_path = layout.results_dir / "rnaseq_summary.json"
    quant_path = quant_dir / "quant.sf"
    meta_info_path = quant_dir / "aux_info" / "meta_info.json"
    existing_metadata = read_metadata(layout)
    steps = init_steps(
        [RNASEQ_STEP_FASTQC, RNASEQ_STEP_INDEX, RNASEQ_STEP_QUANT, RNASEQ_STEP_SUMMARY],
        existing_metadata.get("steps"),
    )
    tool_versions = collect_tool_versions(RNASEQ_REQUIRED_TOOLS)
    input_paths: dict[str, Path] = {}
    if transcriptome is not None:
        input_paths["transcriptome"] = transcriptome
    else:
        assert index is not None
        input_paths["index"] = index
    if paired_mode:
        assert input_r1 is not None and input_r2 is not None
        input_paths.update({"input_r1": input_r1, "input_r2": input_r2})
        read_files = (input_r1, input_r2)
    else:
        assert input_file is not None
        input_paths["input"] = input_file
        read_files = (input_file,)
    input_details = collect_input_details(input_paths)
    failure_summary = str(existing_metadata.get("failure_summary", ""))
    failure_details = existing_metadata.get("failure_details", {})

    def persist(
        status: str,
        *,
        completed_at: str | None = None,
        summary: dict[str, Any] | None = None,
    ) -> None:
        outputs = {
            "root": str(layout.root),
            "fastqc": str(fastqc_dir),
            "salmon_index": str(resolved_index),
            "quant_dir": str(quant_dir),
            "quant_sf": str(quant_path),
            "meta_info": str(meta_info_path),
            "summary": str(summary_path),
        }
        parameters = {
            "threads": threads,
            "library_type": library_type,
            "sample_id": sample_id,
            "group": group,
            "condition": condition,
            "lane": lane,
            "replicate": replicate,
            "resume": resume,
            "execution": execution_payload,
        }
        extra: dict[str, Any] = {
            "steps": steps,
            "resume_used": resume,
            "input_details": input_details,
            "tool_versions": tool_versions,
            "failure_summary": failure_summary,
            "failure_details": failure_details,
        }
        if summary is not None:
            extra["summary"] = summary
            extra["stats"] = {
                key: summary.get(key)
                for key in (
                    "processed_fragments",
                    "mapped_fragments",
                    "mapping_rate",
                    "transcript_count",
                    "expressed_transcripts",
                    "total_tpm",
                    "estimated_num_reads",
                )
            }
        write_metadata(
            layout,
            status=status,
            command="rnaseq",
            parameters=parameters,
            inputs={key: str(value) for key, value in input_paths.items()},
            outputs=outputs,
            started_at=started_at,
            completed_at=completed_at,
            extra=extra,
        )

    persist("running")
    quiet = cli_mode
    if not quiet:
        console.print(Panel(t("rnaseq_pipeline_start", file=str(anchor)), style="bold magenta"))

    fastqc_commands = [
        resolve_command(
            ["fastqc", str(read_file), "-o", str(fastqc_dir), "--quiet"],
            execution_payload,
            path_hints=(read_file, fastqc_dir),
            workdir=fastqc_dir,
        )
        for read_file in read_files
    ]
    if resume and step_resume_ready(
        existing_metadata,
        RNASEQ_STEP_FASTQC,
        validator=lambda: all(_fastqc_report_exists(path, fastqc_dir) for path in read_files),
        required_outputs=("dir",),
        current_execution=execution_payload,
    ):
        set_step_state(
            steps,
            RNASEQ_STEP_FASTQC,
            STEP_SKIPPED,
            outputs={"dir": str(fastqc_dir)},
            note="reused existing output",
        )
        persist("running")
    else:
        raw, resolved = summarize_commands(fastqc_commands, separator=" && ")
        set_step_state(
            steps,
            RNASEQ_STEP_FASTQC,
            STEP_RUNNING,
            backend=fastqc_commands[0].backend,
            raw_command=raw,
            resolved_command=resolved,
            environment_fingerprint=fastqc_commands[0].environment_fingerprint,
        )
        persist("running")
        if not all(
            _run_command(
                command,
                description=t("rnaseq_running_fastqc"),
                quiet=quiet,
                stdout_log=layout.stdout_log,
                stderr_log=layout.stderr_log,
            )
            for command in fastqc_commands
        ):
            failure_summary = build_failure_summary(
                RNASEQ_STEP_FASTQC,
                stderr_log=layout.stderr_log,
                fallback="FastQC failed",
            )
            failure_details = build_failure_details(
                step_name=RNASEQ_STEP_FASTQC,
                command=resolved,
                layout=layout,
                error=failure_summary,
            )
            set_step_state(steps, RNASEQ_STEP_FASTQC, STEP_FAILED, error=failure_summary)
            persist("failed", completed_at=utc_now_iso())
            return None
        set_step_state(
            steps,
            RNASEQ_STEP_FASTQC,
            STEP_SUCCESS,
            outputs={"dir": str(fastqc_dir)},
            backend=fastqc_commands[0].backend,
            raw_command=raw,
            resolved_command=resolved,
            environment_fingerprint=fastqc_commands[0].environment_fingerprint,
        )
        persist("running")

    if index is not None:
        if not _index_ready(index):
            failure_summary = f"{RNASEQ_STEP_INDEX}: invalid or empty Salmon index: {index}"
            failure_details = build_failure_details(
                step_name=RNASEQ_STEP_INDEX,
                command="use prebuilt Salmon index",
                layout=layout,
                error=failure_summary,
            )
            set_step_state(steps, RNASEQ_STEP_INDEX, STEP_FAILED, error=failure_summary)
            persist("failed", completed_at=utc_now_iso())
            return None
        set_step_state(
            steps,
            RNASEQ_STEP_INDEX,
            STEP_SKIPPED,
            outputs={"index": str(index)},
            note="using prebuilt index",
        )
        persist("running")
    elif resume and step_resume_ready(
        existing_metadata,
        RNASEQ_STEP_INDEX,
        validator=lambda: _index_ready(resolved_index),
        required_outputs=("index",),
        current_execution=execution_payload,
    ):
        set_step_state(
            steps,
            RNASEQ_STEP_INDEX,
            STEP_SKIPPED,
            outputs={"index": str(resolved_index)},
            note="reused existing output",
        )
        persist("running")
    else:
        assert transcriptome is not None
        index_command = resolve_command(
            ["salmon", "index", "-t", str(transcriptome), "-i", str(resolved_index)],
            execution_payload,
            path_hints=(transcriptome, resolved_index),
            workdir=layout.root,
        )
        set_step_state(
            steps,
            RNASEQ_STEP_INDEX,
            STEP_RUNNING,
            backend=index_command.backend,
            raw_command=stringify_command(index_command.raw_command),
            resolved_command=stringify_command(index_command.resolved_command),
            environment_fingerprint=index_command.environment_fingerprint,
        )
        persist("running")
        if not _run_command(
            index_command,
            description=t("rnaseq_running_index"),
            quiet=quiet,
            stdout_log=layout.stdout_log,
            stderr_log=layout.stderr_log,
        ):
            failure_summary = build_failure_summary(
                RNASEQ_STEP_INDEX,
                stderr_log=layout.stderr_log,
                fallback="Salmon index failed",
            )
            failure_details = build_failure_details(
                step_name=RNASEQ_STEP_INDEX,
                command=stringify_command(index_command.resolved_command),
                layout=layout,
                error=failure_summary,
            )
            set_step_state(steps, RNASEQ_STEP_INDEX, STEP_FAILED, error=failure_summary)
            persist("failed", completed_at=utc_now_iso())
            return None
        if not _index_ready(resolved_index):
            failure_summary = f"{RNASEQ_STEP_INDEX}: expected Salmon index files were not created"
            failure_details = build_failure_details(
                step_name=RNASEQ_STEP_INDEX,
                command=stringify_command(index_command.resolved_command),
                layout=layout,
                error=failure_summary,
            )
            set_step_state(steps, RNASEQ_STEP_INDEX, STEP_FAILED, error=failure_summary)
            persist("failed", completed_at=utc_now_iso())
            return None
        set_step_state(
            steps,
            RNASEQ_STEP_INDEX,
            STEP_SUCCESS,
            outputs={"index": str(resolved_index)},
            backend=index_command.backend,
            raw_command=stringify_command(index_command.raw_command),
            resolved_command=stringify_command(index_command.resolved_command),
            environment_fingerprint=index_command.environment_fingerprint,
        )
        persist("running")

    quant_args = [
        "salmon",
        "quant",
        "-i",
        str(resolved_index),
        "-l",
        library_type,
    ]
    if paired_mode:
        assert input_r1 is not None and input_r2 is not None
        quant_args.extend(["-1", str(input_r1), "-2", str(input_r2)])
    else:
        assert input_file is not None
        quant_args.extend(["-r", str(input_file)])
    quant_args.extend(["--validateMappings", "-p", str(threads), "-o", str(quant_dir)])
    quant_command = resolve_command(
        quant_args,
        execution_payload,
        path_hints=(*read_files, resolved_index, quant_dir),
        workdir=layout.root,
    )
    if resume and step_resume_ready(
        existing_metadata,
        RNASEQ_STEP_QUANT,
        validator=lambda: _quant_ready(quant_dir),
        required_outputs=("quant_sf", "meta_info"),
        current_execution=execution_payload,
    ):
        set_step_state(
            steps,
            RNASEQ_STEP_QUANT,
            STEP_SKIPPED,
            outputs={"quant_sf": str(quant_path), "meta_info": str(meta_info_path)},
            note="reused existing output",
        )
        persist("running")
    else:
        set_step_state(
            steps,
            RNASEQ_STEP_QUANT,
            STEP_RUNNING,
            backend=quant_command.backend,
            raw_command=stringify_command(quant_command.raw_command),
            resolved_command=stringify_command(quant_command.resolved_command),
            environment_fingerprint=quant_command.environment_fingerprint,
        )
        persist("running")
        if not _run_command(
            quant_command,
            description=t("rnaseq_running_quant"),
            quiet=quiet,
            stdout_log=layout.stdout_log,
            stderr_log=layout.stderr_log,
        ):
            failure_summary = build_failure_summary(
                RNASEQ_STEP_QUANT,
                stderr_log=layout.stderr_log,
                fallback="Salmon quant failed",
            )
            failure_details = build_failure_details(
                step_name=RNASEQ_STEP_QUANT,
                command=stringify_command(quant_command.resolved_command),
                layout=layout,
                error=failure_summary,
            )
            set_step_state(steps, RNASEQ_STEP_QUANT, STEP_FAILED, error=failure_summary)
            persist("failed", completed_at=utc_now_iso())
            return None
        if not _quant_ready(quant_dir):
            failure_summary = f"{RNASEQ_STEP_QUANT}: expected Salmon outputs were not created"
            failure_details = build_failure_details(
                step_name=RNASEQ_STEP_QUANT,
                command=stringify_command(quant_command.resolved_command),
                layout=layout,
                error=failure_summary,
            )
            set_step_state(steps, RNASEQ_STEP_QUANT, STEP_FAILED, error=failure_summary)
            persist("failed", completed_at=utc_now_iso())
            return None
        set_step_state(
            steps,
            RNASEQ_STEP_QUANT,
            STEP_SUCCESS,
            outputs={"quant_sf": str(quant_path), "meta_info": str(meta_info_path)},
            backend=quant_command.backend,
            raw_command=stringify_command(quant_command.raw_command),
            resolved_command=stringify_command(quant_command.resolved_command),
            environment_fingerprint=quant_command.environment_fingerprint,
        )
        persist("running")

    if resume and step_resume_ready(
        existing_metadata,
        RNASEQ_STEP_SUMMARY,
        validator=lambda: _summary_matches_design(
            summary_path,
            sample_id=sample_id,
            group=group,
            condition=condition,
            lane=lane,
            replicate=replicate,
            library_type=library_type,
            input_mode="paired-end" if paired_mode else "single-end",
        ),
        required_outputs=("summary",),
        current_execution=execution_payload,
    ):
        summary = _read_json(summary_path)
        set_step_state(
            steps,
            RNASEQ_STEP_SUMMARY,
            STEP_SKIPPED,
            outputs={"summary": str(summary_path)},
            note="reused existing output",
        )
    else:
        set_step_state(steps, RNASEQ_STEP_SUMMARY, STEP_RUNNING)
        persist("running")
        try:
            summary = summarize_salmon_quant(
                quant_dir,
                sample_id=sample_id,
                group=group,
                condition=condition,
                lane=lane,
                replicate=replicate,
                library_type=library_type,
                input_mode="paired-end" if paired_mode else "single-end",
            )
            summary_path.write_text(
                json.dumps(summary, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except (OSError, TypeError, ValueError) as exc:
            failure_summary = f"{RNASEQ_STEP_SUMMARY}: {exc}"
            failure_details = build_failure_details(
                step_name=RNASEQ_STEP_SUMMARY,
                command="parse Salmon quant.sf and meta_info.json",
                layout=layout,
                error=failure_summary,
            )
            set_step_state(steps, RNASEQ_STEP_SUMMARY, STEP_FAILED, error=failure_summary)
            persist("failed", completed_at=utc_now_iso())
            return None
        set_step_state(
            steps,
            RNASEQ_STEP_SUMMARY,
            STEP_SUCCESS,
            outputs={"summary": str(summary_path)},
        )

    failure_summary = ""
    failure_details = {}
    persist("success", completed_at=utc_now_iso(), summary=summary)
    if not quiet:
        console.print(t("rnaseq_pipeline_done", output=str(layout.root)), style="bold green")
        display_rnaseq_summary(summary)
    return {
        "transcriptome": str(transcriptome) if transcriptome is not None else None,
        "index": str(resolved_index),
        "input": str(input_file) if input_file is not None else None,
        "input_r1": str(input_r1) if input_r1 is not None else None,
        "input_r2": str(input_r2) if input_r2 is not None else None,
        "outdir": str(layout.root),
        "quant_sf": str(quant_path),
        "summary_path": str(summary_path),
        "summary": summary,
        "resume_used": resume,
    }


def _parse_positive_int(value: str | None, default: int) -> int:
    try:
        return max(1, int((value or "").strip()))
    except ValueError:
        return default


def rnaseq_menu() -> None:
    """Interactive RNA-seq workflow."""
    console.print(Panel(t("rnaseq_title"), style="bold magenta"))
    if not preflight_check(RNASEQ_REQUIRED_TOOLS, cli_mode=False):
        input(t("press_enter"))
        return

    try:
        reference_mode = questionary.select(
            t("rnaseq_reference_mode_prompt"),
            choices=[
                questionary.Choice(t("rnaseq_reference_transcriptome"), value="transcriptome"),
                questionary.Choice(t("rnaseq_reference_index"), value="index"),
            ],
        ).ask()
        if reference_mode is None:
            return
        reference_value = questionary.path(
            t("rnaseq_transcriptome_prompt")
            if reference_mode == "transcriptome"
            else t("rnaseq_index_prompt")
        ).ask()
        input_mode = questionary.select(
            t("rnaseq_input_mode_prompt"),
            choices=[
                questionary.Choice(t("rnaseq_input_single"), value="single"),
                questionary.Choice(t("rnaseq_input_paired"), value="paired"),
            ],
        ).ask()
        if input_mode == "paired":
            input_r1_value = questionary.path(t("rnaseq_input_r1_prompt")).ask()
            input_r2_value = questionary.path(t("rnaseq_input_r2_prompt")).ask()
            input_value = None
        else:
            input_value = questionary.path(t("rnaseq_input_prompt")).ask()
            input_r1_value = input_r2_value = None
        outdir_value = questionary.path(t("rnaseq_output_prompt")).ask()
        library_type = questionary.text(t("rnaseq_library_type_prompt"), default="A").ask() or "A"
        threads = _parse_positive_int(
            questionary.text(t("rnaseq_threads_prompt"), default="1").ask(),
            1,
        )
    except KeyboardInterrupt:
        return

    if not reference_value or not (input_value or (input_r1_value and input_r2_value)):
        return
    reference_path = Path(reference_value)
    read_paths = [Path(value) for value in (input_value, input_r1_value, input_r2_value) if value]
    if not reference_path.exists() or any(not path.exists() for path in read_paths):
        missing = reference_path if not reference_path.exists() else next(path for path in read_paths if not path.exists())
        console.print(t("seq_file_not_found", path=str(missing)), style="bold red")
        input(t("press_enter"))
        return

    anchor = read_paths[0]
    run_root = Path(outdir_value) if outdir_value else anchor.parent / "rnaseq_run"
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

    run_rnaseq_pipeline(
        reference_path if reference_mode == "transcriptome" else None,
        Path(input_value) if input_value else None,
        index=reference_path if reference_mode == "index" else None,
        input_r1=Path(input_r1_value) if input_r1_value else None,
        input_r2=Path(input_r2_value) if input_r2_value else None,
        outdir=run_root,
        threads=threads,
        library_type=library_type,
        resume=resume,
        cli_mode=False,
        skip_preflight=True,
    )
    input(t("press_enter"))

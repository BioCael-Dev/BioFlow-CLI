"""BioFlow-CLI BLAST 检索模块 — makeblastdb + blastn 基础检索流程。"""

from __future__ import annotations

import subprocess
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import questionary
from rich.console import Console
from rich.panel import Panel
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

SEARCH_REQUIRED_TOOLS = ("makeblastdb", "blastn")
BLAST_OUTFMT6_COLUMNS = (
    "query_id",
    "subject_id",
    "identity",
    "alignment_length",
    "mismatches",
    "gap_opens",
    "query_start",
    "query_end",
    "subject_start",
    "subject_end",
    "evalue",
    "bitscore",
)


@dataclass
class BlastHit:
    query_id: str
    subject_id: str
    identity: float
    alignment_length: int
    mismatches: int
    gap_opens: int
    query_start: int
    query_end: int
    subject_start: int
    subject_end: int
    evalue: float
    bitscore: float


def _print_search_failure(description: str, err: str) -> bool:
    """打印统一的检索错误信息。"""
    console.print(
        t("search_step_failed", step=description, err=err),
        style="bold red",
    )
    return False


def _run_cmd(
    cmd: list[str],
    *,
    description: str = "",
    quiet: bool = False,
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
) -> bool:
    """执行外部命令并返回是否成功。"""
    if description and not quiet:
        console.print(f"  → {description}", style="cyan")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        append_log(stdout_log, result.stdout)
        append_log(stderr_log, result.stderr)
        return True
    except subprocess.CalledProcessError as exc:
        stderr_text = exc.stderr or str(exc)
        append_log(stdout_log, exc.stdout or "")
        append_log(stderr_log, exc.stderr or "")
        return _print_search_failure(description, stderr_text.strip())
    except FileNotFoundError as exc:
        append_log(stderr_log, str(exc))
        return _print_search_failure(description, str(exc))


def _blast_db_index_files(db_fasta: Path) -> list[Path]:
    """返回 BLAST nucleotide 数据库索引文件列表。"""
    return [db_fasta.with_suffix(db_fasta.suffix + ext) for ext in (".nhr", ".nin", ".nsq")]


def _blast_db_ready(db_fasta: Path) -> bool:
    """判断 BLAST 数据库索引是否已存在。"""
    return all(path.exists() for path in _blast_db_index_files(db_fasta))


def _run_makeblastdb(
    db_fasta: Path,
    *,
    quiet: bool = False,
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
) -> bool:
    """构建 BLAST nucleotide 数据库。"""
    return _run_cmd(
        [
            "makeblastdb",
            "-in",
            str(db_fasta),
            "-dbtype",
            "nucl",
        ],
        description=t("search_building_db", file=db_fasta.name),
        quiet=quiet,
        stdout_log=stdout_log,
        stderr_log=stderr_log,
    )


def _run_blastn(
    db_fasta: Path,
    query_fasta: Path,
    output_path: Path,
    *,
    evalue: float = 10.0,
    max_target_seqs: int = 10,
    quiet: bool = False,
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
) -> bool:
    """执行 blastn 检索。"""
    return _run_cmd(
        [
            "blastn",
            "-query",
            str(query_fasta),
            "-db",
            str(db_fasta),
            "-out",
            str(output_path),
            "-outfmt",
            "6",
            "-evalue",
            str(evalue),
            "-max_target_seqs",
            str(max_target_seqs),
        ],
        description=t("search_running_blastn", file=query_fasta.name),
        quiet=quiet,
        stdout_log=stdout_log,
        stderr_log=stderr_log,
    )


def parse_blast_tsv(output_path: Path) -> list[BlastHit]:
    """解析 BLAST outfmt 6 结果。"""
    hits: list[BlastHit] = []
    if not output_path.exists():
        return hits

    with output_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) != len(BLAST_OUTFMT6_COLUMNS):
                raise ValueError("invalid_blast_output")
            hits.append(
                BlastHit(
                    query_id=parts[0],
                    subject_id=parts[1],
                    identity=float(parts[2]),
                    alignment_length=int(parts[3]),
                    mismatches=int(parts[4]),
                    gap_opens=int(parts[5]),
                    query_start=int(parts[6]),
                    query_end=int(parts[7]),
                    subject_start=int(parts[8]),
                    subject_end=int(parts[9]),
                    evalue=float(parts[10]),
                    bitscore=float(parts[11]),
                )
            )
    return hits


def _rank_hits(hits: list[BlastHit]) -> list[BlastHit]:
    """按 evalue / bitscore / identity 排序。"""
    return sorted(
        hits,
        key=lambda hit: (hit.evalue, -hit.bitscore, -hit.identity),
    )


def summarize_blast_hits(hits: list[BlastHit], *, top_n: int = 5) -> dict[str, object]:
    """生成 BLAST 结果摘要。"""
    ranked_hits = _rank_hits(hits)
    best_hit = ranked_hits[0] if ranked_hits else None
    top_hits = ranked_hits[:top_n]

    return {
        "hit_count": len(hits),
        "best_hit": asdict(best_hit) if best_hit else None,
        "best_identity": best_hit.identity if best_hit else None,
        "best_bitscore": best_hit.bitscore if best_hit else None,
        "min_evalue": best_hit.evalue if best_hit else None,
        "top_hits": [asdict(hit) for hit in top_hits],
    }


def display_search_summary(summary: dict[str, object]) -> None:
    """使用 rich 表格展示 Top hits。"""
    hit_count = int(summary["hit_count"])
    console.print(
        t("search_summary_line", hits=hit_count),
        style="bold cyan",
    )

    best_hit = summary["best_hit"]
    if not isinstance(best_hit, dict):
        console.print(t("search_no_hits"), style="yellow")
        return

    console.print(
        t(
            "search_best_hit_line",
            subject=best_hit["subject_id"],
            identity=f"{float(best_hit['identity']):.2f}",
            evalue=f"{float(best_hit['evalue']):.2e}",
            bitscore=f"{float(best_hit['bitscore']):.2f}",
        ),
        style="bold green",
    )

    table = Table(
        title=t("search_top_hits_title"),
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column(t("search_col_query"), style="cyan")
    table.add_column(t("search_col_subject"), style="green")
    table.add_column(t("search_col_identity"), justify="right", style="magenta")
    table.add_column(t("search_col_evalue"), justify="right", style="yellow")
    table.add_column(t("search_col_bitscore"), justify="right", style="blue")

    for hit in summary["top_hits"]:
        table.add_row(
            str(hit["query_id"]),
            str(hit["subject_id"]),
            f"{float(hit['identity']):.2f}",
            f"{float(hit['evalue']):.2e}",
            f"{float(hit['bitscore']):.2f}",
        )

    console.print(table)


def run_blast_search(
    db_fasta: Path,
    query_fasta: Path,
    *,
    output: Path | None = None,
    outdir: Path | None = None,
    evalue: float = 10.0,
    max_target_seqs: int = 10,
    top_n: int = 5,
    cli_mode: bool = False,
    skip_preflight: bool = False,
) -> dict[str, object] | None:
    """执行 makeblastdb + blastn 基础检索流程。"""
    if not skip_preflight:
        if not preflight_check(SEARCH_REQUIRED_TOOLS, cli_mode=cli_mode):
            return None

    layout = create_run_layout("search", query_fasta, outdir=outdir)
    started_at = utc_now_iso()
    output = resolve_result_path(layout, output, f"{query_fasta.stem}.blast.tsv")
    write_metadata(
        layout,
        status="running",
        command="search",
        parameters={"evalue": evalue, "max_target_seqs": max_target_seqs, "top_n": top_n},
        inputs={"db": str(db_fasta), "query": str(query_fasta)},
        outputs={"root": str(layout.root), "tsv": str(output)},
        started_at=started_at,
    )

    quiet = cli_mode
    if not quiet:
        console.print(
            Panel(
                t("search_pipeline_start", file=str(query_fasta)),
                style="bold magenta",
            )
        )

    if _blast_db_ready(db_fasta):
        if not quiet:
            console.print(t("search_db_cached"), style="bold blue")
    else:
        if not quiet:
            console.print(t("search_step_makeblastdb"), style="bold blue")
        if not _run_makeblastdb(
            db_fasta,
            quiet=quiet,
            stdout_log=layout.stdout_log,
            stderr_log=layout.stderr_log,
        ):
            write_metadata(
                layout,
                status="failed",
                command="search",
                parameters={"evalue": evalue, "max_target_seqs": max_target_seqs, "top_n": top_n},
                inputs={"db": str(db_fasta), "query": str(query_fasta)},
                outputs={"root": str(layout.root), "tsv": str(output)},
                started_at=started_at,
                completed_at=utc_now_iso(),
            )
            return None

    if not quiet:
        console.print(t("search_step_blastn"), style="bold blue")
    if not _run_blastn(
        db_fasta,
        query_fasta,
        output,
        evalue=evalue,
        max_target_seqs=max_target_seqs,
        quiet=quiet,
        stdout_log=layout.stdout_log,
        stderr_log=layout.stderr_log,
    ):
        write_metadata(
            layout,
            status="failed",
            command="search",
            parameters={"evalue": evalue, "max_target_seqs": max_target_seqs, "top_n": top_n},
            inputs={"db": str(db_fasta), "query": str(query_fasta)},
            outputs={"root": str(layout.root), "tsv": str(output)},
            started_at=started_at,
            completed_at=utc_now_iso(),
        )
        return None

    hits = parse_blast_tsv(output)
    summary = summarize_blast_hits(hits, top_n=top_n)
    hit_count = int(summary["hit_count"])
    summary_path = layout.results_dir / "search_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    if not quiet:
        console.print(
            t("search_pipeline_done", output=str(output), hits=hit_count),
            style="bold green",
        )
        display_search_summary(summary)

    write_metadata(
        layout,
        status="success",
        command="search",
        parameters={"evalue": evalue, "max_target_seqs": max_target_seqs, "top_n": top_n},
        inputs={"db": str(db_fasta), "query": str(query_fasta)},
        outputs={"root": str(layout.root), "tsv": str(output), "summary": str(summary_path)},
        started_at=started_at,
        completed_at=utc_now_iso(),
        extra={"summary": summary},
    )

    return {
        "db": str(db_fasta),
        "query": str(query_fasta),
        "output": str(output),
        "outdir": str(layout.root),
        "hits": hit_count,
        "evalue": evalue,
        "max_target_seqs": max_target_seqs,
        "top_n": top_n,
        "summary": summary,
    }


def _parse_float(value: str | None, default: float) -> float:
    """解析浮点输入，非法值回退默认值。"""
    if value is None:
        return default
    try:
        return float(value.strip())
    except (TypeError, ValueError):
        return default


def _parse_int(value: str | None, default: int) -> int:
    """解析整数输入，非法值回退默认值。"""
    if value is None:
        return default
    try:
        return max(1, int(value.strip()))
    except (TypeError, ValueError):
        return default


def search_menu() -> None:
    """BLAST 检索交互菜单（TUI 模式）。"""
    console.print(Panel(t("search_title"), style="bold magenta"))

    if not preflight_check(SEARCH_REQUIRED_TOOLS, cli_mode=False):
        input(t("press_enter"))
        return

    try:
        db_path = questionary.path(t("search_db_prompt")).ask()
    except KeyboardInterrupt:
        return
    if not db_path:
        return

    db_fasta = Path(db_path)
    if not db_fasta.exists():
        console.print(t("seq_file_not_found", path=str(db_fasta)), style="bold red")
        input(t("press_enter"))
        return

    try:
        query_path = questionary.path(t("search_query_prompt")).ask()
    except KeyboardInterrupt:
        return
    if not query_path:
        return

    query_fasta = Path(query_path)
    if not query_fasta.exists():
        console.print(t("seq_file_not_found", path=str(query_fasta)), style="bold red")
        input(t("press_enter"))
        return

    default_output = query_fasta.parent / f"{query_fasta.stem}.blast.tsv"
    try:
        output_path = questionary.path(
            t("search_output_prompt"),
            default=str(default_output),
        ).ask()
    except KeyboardInterrupt:
        return
    if not output_path:
        return

    evalue = _parse_float(questionary.text(t("search_evalue_prompt"), default="10.0").ask(), 10.0)
    max_target_seqs = _parse_int(
        questionary.text(t("search_max_targets_prompt"), default="10").ask(),
        10,
    )
    top_n = _parse_int(
        questionary.text(t("search_top_n_prompt"), default="5").ask(),
        5,
    )

    run_blast_search(
        db_fasta,
        query_fasta,
        output=Path(output_path),
        evalue=evalue,
        max_target_seqs=max_target_seqs,
        top_n=top_n,
        cli_mode=False,
        skip_preflight=True,
    )
    input(t("press_enter"))

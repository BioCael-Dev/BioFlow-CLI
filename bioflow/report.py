"""BioFlow-CLI report module -- HTML report generation."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol

import questionary
from rich.console import Console
from rich.panel import Panel

from bioflow import __version__
from bioflow.i18n import t

console = Console()

# ---------------------------------------------------------------------------
# Report data model
# ---------------------------------------------------------------------------

@dataclass
class RunInfo:
    """Parsed metadata from a single run directory."""

    run_dir: Path
    workflow: str
    version: str
    status: str
    started_at: str
    completed_at: str | None
    command: str
    parameters: dict[str, Any]
    inputs: dict[str, Any]
    outputs: dict[str, Any]
    steps: dict[str, Any]
    logs: dict[str, Any]
    runtime: dict[str, Any]
    tool_versions: dict[str, Any]
    input_details: dict[str, Any]
    failure_summary: str
    failure_details: dict[str, Any]
    stats: dict[str, Any]
    summary: dict[str, Any]


@dataclass
class ReportOverview:
    """Aggregated summary for a report page."""

    total_runs: int
    status_counts: dict[str, int]
    workflow_counts: dict[str, int]
    workflow_status_counts: dict[str, dict[str, int]]


SUMMARY_TSV_COLUMNS = (
    "run_dir",
    "sample_id",
    "workflow",
    "status",
    "started_at",
    "completed_at",
    "version",
    "key_metric",
    "key_metric_value",
    "outputs",
    "metrics",
)


def parse_metadata(run_dir: Path) -> RunInfo:
    """Read *run_dir*/metadata.json and return a RunInfo.

    Raises ``FileNotFoundError`` if the file is absent,
    ``ValueError`` for malformed JSON or missing required keys.
    """
    meta_path = run_dir / "metadata.json"
    if not meta_path.exists():
        raise FileNotFoundError(f"metadata.json not found in {run_dir}")

    try:
        data: dict[str, Any] = json.loads(meta_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid metadata.json in {run_dir}: {exc}") from exc

    required = ("workflow", "status", "started_at")
    for key in required:
        if key not in data:
            raise ValueError(f"metadata.json missing required key '{key}' in {run_dir}")

    return RunInfo(
        run_dir=run_dir,
        workflow=data.get("workflow", "unknown"),
        version=data.get("version", ""),
        status=data.get("status", "unknown"),
        started_at=data.get("started_at", ""),
        completed_at=data.get("completed_at"),
        command=data.get("command", ""),
        parameters=data.get("parameters", {}),
        inputs=data.get("inputs", {}),
        outputs=data.get("outputs", {}),
        steps=data.get("steps", {}),
        logs=data.get("logs", {}),
        runtime=data.get("runtime", {}),
        tool_versions=data.get("tool_versions", {}),
        input_details=data.get("input_details", {}),
        failure_summary=data.get("failure_summary", ""),
        failure_details=data.get("failure_details", {}),
        stats=data.get("stats", {}) if isinstance(data.get("stats", {}), dict) else {},
        summary=data.get("summary", {}) if isinstance(data.get("summary", {}), dict) else {},
    )


def discover_runs(input_path: Path) -> list[RunInfo]:
    """Discover run directories under *input_path*.

    If *input_path* itself contains metadata.json it is treated as a single
    run directory.  Otherwise its immediate children are scanned.
    """
    if not input_path.is_dir():
        raise NotADirectoryError(str(input_path))

    if (input_path / "metadata.json").exists():
        return [parse_metadata(input_path)]

    runs: list[RunInfo] = []
    for child in sorted(input_path.iterdir()):
        if child.is_dir() and (child / "metadata.json").exists():
            runs.append(parse_metadata(child))
    return runs


# ---------------------------------------------------------------------------
# Template protocol (for future extensibility)
# ---------------------------------------------------------------------------

class ReportTemplate(Protocol):
    """Interface for report templates."""

    def render(self, runs: list[RunInfo], title: str) -> str:  # pragma: no cover
        ...


# ---------------------------------------------------------------------------
# Default HTML template
# ---------------------------------------------------------------------------

_CSS = """\
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
     line-height:1.6;color:#1a1a1a;background:#f6f8fa;padding:2rem}
.container{max-width:1180px;margin:0 auto}
h1{color:#2c3e50;margin-bottom:.5rem}
h2{font-size:1rem;color:#2c3e50;margin-bottom:.75rem}
.subtitle{color:#7f8c8d;margin-bottom:2rem;font-size:.9rem}
.overview,.workflow-summary{background:#fff;border:1px solid #e1e4e8;border-radius:8px;padding:1.25rem;margin-bottom:1.5rem;
          box-shadow:0 1px 3px rgba(0,0,0,.04)}
.stat-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:.75rem;margin-bottom:1rem}
.stat-card{border:1px solid #e5e7eb;border-radius:8px;padding:.9rem;background:#fbfcfd}
.stat-label{font-size:.82rem;color:#6b7280}
.stat-value{font-size:1.45rem;font-weight:700;color:#1f2937}
.stat-note{font-size:.78rem;color:#6b7280;margin-top:.2rem}
.overview-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:1rem}
.overview-panel{border:1px solid #e5e7eb;border-radius:8px;padding:1rem;background:#fcfcfd}
.filter-row{display:flex;flex-wrap:wrap;gap:.5rem}
.filter-btn,.nav-link{display:inline-flex;align-items:center;gap:.35rem;border:1px solid #d0d7de;
                      border-radius:999px;background:#fff;color:#334155;text-decoration:none;padding:.35rem .7rem;
                      font-size:.82rem;cursor:pointer}
.filter-btn.is-active{background:#0f766e;color:#fff;border-color:#0f766e}
.control-grid{display:grid;grid-template-columns:minmax(180px,1fr) minmax(150px,.5fr);gap:.6rem;margin:.5rem 0 1rem}
.control-input,.control-select{border:1px solid #d0d7de;border-radius:6px;background:#fff;padding:.45rem .6rem;font-size:.88rem;color:#1f2937}
.workflow-metric-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:.75rem}
.workflow-metric-card{border:1px solid #e5e7eb;border-radius:8px;background:#fff;padding:.8rem}
.workflow-metric-card h3{font-size:.9rem;color:#334155;margin-bottom:.5rem}
.metric-list{list-style:none;display:grid;gap:.25rem;font-size:.86rem}
.metric-list li{display:flex;justify-content:space-between;gap:.75rem;border-bottom:1px solid #f1f3f5;padding-bottom:.2rem}
.metric-list span:first-child{color:#64748b}
.metric-list span:last-child{font-weight:600;color:#1f2937;text-align:right}
.failure-list{display:grid;gap:.4rem;font-size:.86rem}
.failure-item{border-left:3px solid #e74c3c;padding:.35rem .5rem;background:#fff}
.empty-note{color:#6b7280;font-size:.86rem}
.nav-list{display:flex;flex-wrap:wrap;gap:.5rem}
.run-card{background:#fff;border:1px solid #e1e4e8;border-radius:8px;
          padding:1.5rem;margin-bottom:1.5rem;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.run-card.is-hidden{display:none}
.run-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem}
.run-title{font-size:1.15rem;font-weight:600;color:#2c3e50}
.badge{padding:.2rem .6rem;border-radius:12px;font-size:.8rem;font-weight:500;color:#fff}
.badge-success{background:#27ae60}
.badge-failed{background:#e74c3c}
.badge-running{background:#f39c12}
.badge-pending{background:#95a5a6}
.badge-skipped{background:#3498db}
table{width:100%;border-collapse:collapse;margin:.8rem 0;font-size:.9rem}
th,td{text-align:left;padding:.45rem .6rem;border-bottom:1px solid #eee}
th{background:#f1f3f5;font-weight:600;color:#495057}
.section-title{font-size:.95rem;font-weight:600;color:#34495e;margin:.8rem 0 .4rem;
               border-bottom:2px solid #3498db;display:inline-block;padding-bottom:.15rem}
.step-row td:first-child{font-weight:500}
.status-success{color:#27ae60}
.status-failed{color:#e74c3c}
.status-pending{color:#95a5a6}
.status-skipped{color:#3498db}
.status-running{color:#f39c12}
pre{white-space:pre-wrap;word-break:break-word;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:.82rem}
footer{text-align:center;color:#95a5a6;font-size:.8rem;margin-top:2rem;padding-top:1rem;
       border-top:1px solid #e1e4e8}
"""

_JS = """\
(() => {
  const cards = Array.from(document.querySelectorAll('.run-card'));
  const buttons = Array.from(document.querySelectorAll('.filter-btn'));
  const searchInput = document.querySelector('[data-report-search]');
  const sortSelect = document.querySelector('[data-report-sort]');
  const list = document.querySelector('[data-run-list]');
  let workflow = 'all';
  let status = 'all';
  let query = '';

  function syncButtons() {
    buttons.forEach((btn) => {
      const group = btn.dataset.group;
      const value = btn.dataset.value;
      const active = (group === 'workflow' && value === workflow) || (group === 'status' && value === status);
      btn.classList.toggle('is-active', active);
    });
  }

  function applyFilters() {
    const needle = query.trim().toLowerCase();
    cards.forEach((card) => {
      const workflowMatch = workflow === 'all' || card.dataset.workflow === workflow;
      const statusMatch = status === 'all' || card.dataset.status === status;
      const searchMatch = !needle || (card.dataset.search || '').includes(needle);
      card.classList.toggle('is-hidden', !(workflowMatch && statusMatch && searchMatch));
    });
    syncButtons();
  }

  function applySort() {
    if (!list || !sortSelect) return;
    const mode = sortSelect.value;
    const sorted = [...cards].sort((a, b) => {
      if (mode === 'sample') return (a.dataset.sample || '').localeCompare(b.dataset.sample || '');
      if (mode === 'workflow') return (a.dataset.workflow || '').localeCompare(b.dataset.workflow || '');
      if (mode === 'status') return (a.dataset.status || '').localeCompare(b.dataset.status || '');
      return (b.dataset.started || '').localeCompare(a.dataset.started || '');
    });
    sorted.forEach((card) => list.appendChild(card));
  }

  buttons.forEach((btn) => {
    btn.addEventListener('click', () => {
      const group = btn.dataset.group;
      const value = btn.dataset.value;
      if (group === 'workflow') workflow = value;
      if (group === 'status') status = value;
      applyFilters();
    });
  });
  if (searchInput) {
    searchInput.addEventListener('input', () => {
      query = searchInput.value || '';
      applyFilters();
    });
  }
  if (sortSelect) {
    sortSelect.addEventListener('change', () => {
      applySort();
      applyFilters();
    });
  }

  applySort();
  applyFilters();
})();
"""


def _esc(text: Any) -> str:
    """Basic HTML escaping."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _badge_class(status: str) -> str:
    if status == "success":
        return "badge-success"
    if status == "failed":
        return "badge-failed"
    if status == "skipped":
        return "badge-skipped"
    if status == "pending":
        return "badge-pending"
    return "badge-running"


def _status_class(status: str) -> str:
    return f"status-{status}" if status in ("success", "failed", "pending", "skipped", "running") else ""


def _format_report_value(value: Any) -> str:
    """格式化报告中的值，嵌套结构输出为 JSON。"""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
    return str(value)


def _format_metric_value(value: Any) -> str:
    """Format compact metric values for overview cards."""
    if value in (None, ""):
        return "-"
    if isinstance(value, float):
        if 0 <= value <= 1:
            return f"{value:.2%}"
        return f"{value:.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _render_kv_table(data: dict[str, Any]) -> str:
    """Render a simple key-value dictionary as an HTML table."""
    if not data:
        return "<p>-</p>"
    rows = "".join(
        f"<tr><td>{_esc(k)}</td><td><pre>{_esc(_format_report_value(v))}</pre></td></tr>" for k, v in data.items()
    )
    return f"<table><tr><th>{_esc(t('report_col_key'))}</th><th>{_esc(t('report_col_value'))}</th></tr>{rows}</table>"


def _render_steps_table(steps: dict[str, Any]) -> str:
    """Render steps dict as a status table."""
    if not steps:
        return "<p>-</p>"
    rows: list[str] = []
    for name, info in steps.items():
        if isinstance(info, dict):
            status = info.get("status", "pending")
            started = info.get("started_at", "-")
            completed = info.get("completed_at", "-")
        else:
            status, started, completed = "unknown", "-", "-"
        cls = _status_class(status)
        rows.append(
            f'<tr class="step-row"><td>{_esc(name)}</td>'
            f'<td class="{cls}">{_esc(status)}</td>'
            f"<td>{_esc(started)}</td><td>{_esc(completed)}</td></tr>"
        )
    header = (
        f"<tr><th>{_esc(t('report_col_step'))}</th>"
        f"<th>{_esc(t('report_col_status'))}</th>"
        f"<th>{_esc(t('report_col_start'))}</th>"
        f"<th>{_esc(t('report_col_end'))}</th></tr>"
    )
    return f"<table>{header}{''.join(rows)}</table>"


def _run_dom_id(run: RunInfo, index: int) -> str:
    """Stable DOM id for one run card."""
    return f"run-{run.workflow.lower()}-{index + 1}"


def _success_rate(overview: ReportOverview) -> float:
    """Return successful run ratio."""
    if overview.total_runs == 0:
        return 0.0
    return overview.status_counts.get("success", 0) / overview.total_runs


def _build_overview(runs: list[RunInfo]) -> ReportOverview:
    """Compute aggregate report stats."""
    status_counts: dict[str, int] = {}
    workflow_counts: dict[str, int] = {}
    workflow_status_counts: dict[str, dict[str, int]] = {}

    for run in runs:
        status_counts[run.status] = status_counts.get(run.status, 0) + 1
        workflow_counts[run.workflow] = workflow_counts.get(run.workflow, 0) + 1
        workflow_status = workflow_status_counts.setdefault(run.workflow, {})
        workflow_status[run.status] = workflow_status.get(run.status, 0) + 1

    return ReportOverview(
        total_runs=len(runs),
        status_counts=status_counts,
        workflow_counts=workflow_counts,
        workflow_status_counts=workflow_status_counts,
    )


def _sample_id(run: RunInfo) -> str:
    """Return the most stable sample id available for one run."""
    for source in (run.parameters, run.inputs, run.summary):
        value = source.get("sample_id") if isinstance(source, dict) else None
        if value not in (None, ""):
            return str(value)
    return run.run_dir.name


def _metric_payload(run: RunInfo) -> dict[str, Any]:
    """Return workflow metrics using stable top-level metric keys."""
    metrics: dict[str, Any] = {}
    if run.workflow == "qc":
        for key in ("reads", "trimmed_reads", "trimmed_bases", "avg_q", "q20_ratio", "q30_ratio"):
            if key in run.stats:
                metrics[key] = run.stats[key]
            elif key in run.summary:
                metrics[key] = run.summary[key]
    elif run.workflow == "align":
        for key in ("total", "mapped", "unmapped", "mapping_rate", "paired", "properly_paired"):
            if key in run.stats:
                metrics[key] = run.stats[key]
    elif run.workflow == "search":
        for key in ("hit_count", "best_identity", "best_bitscore", "min_evalue"):
            if key in run.summary:
                metrics[key] = run.summary[key]
        best_hit = run.summary.get("best_hit")
        if isinstance(best_hit, dict) and best_hit.get("subject_id") not in (None, ""):
            metrics["best_hit"] = best_hit["subject_id"]

    for key, value in run.stats.items():
        metrics.setdefault(key, value)
    for key, value in run.summary.items():
        if key != "top_hits":
            metrics.setdefault(key, value)
    return metrics


def _key_metric(run: RunInfo, metrics: dict[str, Any]) -> tuple[str, Any]:
    """Pick one compact metric for TSV scanning."""
    preferred_by_workflow = {
        "qc": ("trimmed_reads", "reads", "avg_q"),
        "align": ("mapping_rate", "mapped", "total"),
        "search": ("hit_count", "best_hit", "best_bitscore"),
    }
    for key in preferred_by_workflow.get(run.workflow, ()):
        if key in metrics:
            return key, metrics[key]
    for key, value in metrics.items():
        return key, value
    return "", ""


def _run_summary_record(run: RunInfo) -> dict[str, Any]:
    """Convert one run into the structured summary record."""
    outputs = _core_outputs(run)
    metrics = _metric_payload(run)
    key_metric, key_metric_value = _key_metric(run, metrics)
    return {
        "run_dir": str(run.run_dir),
        "sample_id": _sample_id(run),
        "workflow": run.workflow,
        "status": run.status,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "version": run.version,
        "command": run.command,
        "outputs": outputs,
        "metrics": metrics,
        "key_metric": key_metric,
        "key_metric_value": key_metric_value,
        "failure_summary": run.failure_summary,
    }


def _run_search_text(run: RunInfo) -> str:
    """Build lower-case search text for client-side filtering."""
    values = [
        _sample_id(run),
        run.workflow,
        run.status,
        run.command,
        str(run.run_dir),
        run.failure_summary,
    ]
    values.extend(str(value) for value in _core_outputs(run).values())
    return " ".join(value for value in values if value).lower()


def _json_cell(value: Any) -> str:
    """Serialize a value for one TSV cell."""
    if value in (None, ""):
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def collect_summary_data(input_path: Path, *, project: dict[str, Any] | None = None) -> dict[str, Any]:
    """Collect reusable aggregate summary data from run metadata."""
    runs = discover_runs(input_path)
    return collect_summary_data_from_runs(runs, source=input_path, project=project)


def collect_summary_data_from_runs(
    runs: list[RunInfo],
    *,
    source: Path | str,
    project: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Collect reusable aggregate summary data from already parsed runs."""
    if not runs:
        raise FileNotFoundError(t("report_no_runs", path=str(source)))

    overview = _build_overview(runs)
    run_records = [_run_summary_record(run) for run in runs]
    return {
        "schema_version": "bioflow.summary.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": str(source),
        "project": project or {},
        "total_runs": overview.total_runs,
        "status_counts": overview.status_counts,
        "workflow_counts": overview.workflow_counts,
        "workflow_status_counts": overview.workflow_status_counts,
        "runs": run_records,
    }


def write_summary_json(data: dict[str, Any], output_path: Path) -> Path:
    """Write structured aggregate summary JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def write_summary_tsv(data: dict[str, Any], output_path: Path) -> Path:
    """Write a stable TSV projection of aggregate summary data."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    runs = data.get("runs", [])
    if not isinstance(runs, Iterable):
        runs = []

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_TSV_COLUMNS, dialect="excel-tab")
        writer.writeheader()
        for record in runs:
            if not isinstance(record, dict):
                continue
            writer.writerow({column: _json_cell(record.get(column, "")) for column in SUMMARY_TSV_COLUMNS})
    return output_path


def _core_outputs(run: RunInfo) -> dict[str, Any]:
    """Return workflow-specific core outputs/summary for the report."""
    outputs = dict(run.outputs)

    if run.workflow == "qc":
        keys = (
            "trimmed",
            "trimmed_r1",
            "trimmed_r2",
            "unpaired_r1",
            "unpaired_r2",
            "fastqc_pre",
            "fastqc_post",
        )
        return {key: outputs[key] for key in keys if key in outputs}

    if run.workflow == "align":
        core = {key: outputs[key] for key in ("bam", "bai", "flagstat") if key in outputs}
        if "mapping_rate" in run.stats:
            core["mapping_rate"] = f"{float(run.stats['mapping_rate']):.2%}"
        if "mapped" in run.stats:
            core["mapped"] = run.stats["mapped"]
        return core

    if run.workflow == "search":
        core = {key: outputs[key] for key in ("tsv", "summary") if key in outputs}
        if "hit_count" in run.summary:
            core["hit_count"] = run.summary["hit_count"]
        best_hit = run.summary.get("best_hit")
        if isinstance(best_hit, dict) and "subject_id" in best_hit:
            core["best_hit"] = best_hit["subject_id"]
        return core

    return outputs


def _workflow_metric_rows(workflow: str, runs: list[RunInfo]) -> dict[str, Any]:
    """Summarize workflow-specific metrics across a run subset."""
    successful = [run for run in runs if run.status == "success"]
    metrics: dict[str, Any] = {
        t("report_overview_total"): len(runs),
        t("report_overview_success"): len(successful),
        t("report_overview_failed"): sum(1 for run in runs if run.status == "failed"),
    }

    if workflow == "align":
        rates = [
            float(run.stats["mapping_rate"])
            for run in successful
            if isinstance(run.stats.get("mapping_rate"), (int, float))
        ]
        mapped = [int(run.stats["mapped"]) for run in successful if isinstance(run.stats.get("mapped"), int)]
        if rates:
            metrics[t("report_metric_avg_mapping_rate")] = sum(rates) / len(rates)
        if mapped:
            metrics[t("report_metric_mapped_reads")] = sum(mapped)
    elif workflow == "search":
        hit_counts = [
            int(run.summary["hit_count"])
            for run in successful
            if isinstance(run.summary.get("hit_count"), int)
        ]
        if hit_counts:
            metrics[t("report_metric_total_hits")] = sum(hit_counts)
            metrics[t("report_metric_avg_hits")] = sum(hit_counts) / len(hit_counts)
    elif workflow == "qc":
        trimmed = []
        for run in successful:
            value = run.stats.get("trimmed_reads", run.summary.get("trimmed_reads"))
            if isinstance(value, int):
                trimmed.append(value)
        if trimmed:
            metrics[t("report_metric_trimmed_reads")] = sum(trimmed)

    return metrics


def _render_metric_list(metrics: dict[str, Any]) -> str:
    """Render compact metric rows."""
    rows = "".join(
        f"<li><span>{_esc(key)}</span><span>{_esc(_format_metric_value(value))}</span></li>"
        for key, value in metrics.items()
    )
    return f'<ul class="metric-list">{rows}</ul>' if rows else f'<p class="empty-note">{_esc(t("report_empty_value"))}</p>'


def _render_key_metric_cards(runs: list[RunInfo]) -> str:
    """Render workflow-specific metric cards."""
    cards: list[str] = []
    for workflow in sorted({run.workflow for run in runs}):
        subset = [run for run in runs if run.workflow == workflow]
        cards.append(
            "\n".join(
                [
                    '<div class="workflow-metric-card">',
                    f"<h3>{_esc(workflow.upper())}</h3>",
                    _render_metric_list(_workflow_metric_rows(workflow, subset)),
                    "</div>",
                ]
            )
        )
    return '<div class="workflow-metric-grid">' + "".join(cards) + "</div>" if cards else "<p>-</p>"


def _render_failure_summary(runs: list[RunInfo]) -> str:
    """Render a compact list of failed runs."""
    failed = [run for run in runs if run.status == "failed"]
    if not failed:
        return f'<p class="empty-note">{_esc(t("report_no_failures"))}</p>'

    items = []
    for run in failed[:8]:
        summary = run.failure_summary or t("report_failure_unknown")
        items.append(
            f'<div class="failure-item"><strong>{_esc(_sample_id(run))}</strong> '
            f'<span>({_esc(run.workflow.upper())})</span><br>{_esc(summary)}</div>'
        )
    if len(failed) > 8:
        items.append(f'<p class="empty-note">{_esc(t("report_failure_more", count=len(failed) - 8))}</p>')
    return '<div class="failure-list">' + "".join(items) + "</div>"


def _render_workflow_summaries(runs: list[RunInfo]) -> str:
    """Render the MultiQC-style workflow summary section."""
    return "\n".join(
        [
            '<section class="workflow-summary">',
            f'<h2>{_esc(t("report_section_workflow_summaries"))}</h2>',
            _render_key_metric_cards(runs),
            "</section>",
        ]
    )


def _render_overview(overview: ReportOverview, runs: list[RunInfo]) -> str:
    """Render report overview cards, workflow matrix, filters, and navigation."""
    workflows = sorted(overview.workflow_counts)
    workflow_rows = "".join(
        (
            f"<tr><td>{_esc(workflow.upper())}</td>"
            f"<td>{_esc(overview.workflow_counts[workflow])}</td>"
            f"<td>{_esc(overview.workflow_status_counts.get(workflow, {}).get('success', 0))}</td>"
            f"<td>{_esc(overview.workflow_status_counts.get(workflow, {}).get('failed', 0))}</td></tr>"
        )
        for workflow in workflows
    )
    workflow_table = (
        "<table>"
        f"<tr><th>{_esc(t('report_field_workflow'))}</th>"
        f"<th>{_esc(t('report_overview_total'))}</th>"
        f"<th>{_esc(t('report_overview_success'))}</th>"
        f"<th>{_esc(t('report_overview_failed'))}</th></tr>"
        f"{workflow_rows}</table>"
        if workflow_rows
        else "<p>-</p>"
    )

    workflow_filters = "".join(
        f'<button class="filter-btn" type="button" data-group="workflow" data-value="{_esc(workflow)}">{_esc(workflow.upper())}</button>'
        for workflow in workflows
    )
    status_filters = "".join(
        f'<button class="filter-btn" type="button" data-group="status" data-value="{status}">{_esc(t(f"report_status_{status}"))}</button>'
        for status in ("success", "failed", "running", "pending", "skipped")
        if overview.status_counts.get(status, 0) > 0
    )
    nav_links = "".join(
        (
            f'<a class="nav-link" href="#{_run_dom_id(run, index)}">'
            f"{_esc(run.workflow.upper())}"
            f'<span class="badge {_badge_class(run.status)}">{_esc(run.status)}</span>'
            "</a>"
        )
        for index, run in enumerate(runs)
    )

    return "\n".join(
        [
            '<section class="overview">',
            '<div class="stat-grid">',
            f'<div class="stat-card"><div class="stat-label">{_esc(t("report_overview_total"))}</div><div class="stat-value">{overview.total_runs}</div></div>',
            f'<div class="stat-card"><div class="stat-label">{_esc(t("report_overview_success"))}</div><div class="stat-value">{overview.status_counts.get("success", 0)}</div></div>',
            f'<div class="stat-card"><div class="stat-label">{_esc(t("report_overview_failed"))}</div><div class="stat-value">{overview.status_counts.get("failed", 0)}</div></div>',
            (
                f'<div class="stat-card"><div class="stat-label">{_esc(t("report_overview_success_rate"))}</div>'
                f'<div class="stat-value">{_success_rate(overview):.1%}</div>'
                f'<div class="stat-note">{_esc(t("report_overview_running"))}: {overview.status_counts.get("running", 0)}</div></div>'
            ),
            '</div>',
            '<div class="overview-grid">',
            f'<div class="overview-panel"><h2>{_esc(t("report_section_overview"))}</h2>{workflow_table}</div>',
            (
                f'<div class="overview-panel"><h2>{_esc(t("report_section_filters"))}</h2>'
                f'<div class="control-grid">'
                f'<input class="control-input" type="search" data-report-search placeholder="{_esc(t("report_search_placeholder"))}">'
                f'<select class="control-select" data-report-sort aria-label="{_esc(t("report_sort_label"))}">'
                f'<option value="started">{_esc(t("report_sort_started"))}</option>'
                f'<option value="sample">{_esc(t("report_sort_sample"))}</option>'
                f'<option value="workflow">{_esc(t("report_sort_workflow"))}</option>'
                f'<option value="status">{_esc(t("report_sort_status"))}</option>'
                f'</select></div>'
                f'<div class="section-title">{_esc(t("report_filter_workflow"))}</div><div class="filter-row">'
                f'<button class="filter-btn is-active" type="button" data-group="workflow" data-value="all">{_esc(t("report_filter_all"))}</button>{workflow_filters}'
                f'</div><div class="section-title">{_esc(t("report_filter_status"))}</div><div class="filter-row">'
                f'<button class="filter-btn is-active" type="button" data-group="status" data-value="all">{_esc(t("report_filter_all"))}</button>{status_filters}'
                '</div></div>'
            ),
            f'<div class="overview-panel"><h2>{_esc(t("report_section_failure_summary"))}</h2>{_render_failure_summary(runs)}</div>',
            f'<div class="overview-panel"><h2>{_esc(t("report_section_navigation"))}</h2><div class="nav-list">{nav_links}</div></div>',
            '</div>',
            '</section>',
        ]
    )


def _render_run_card(run: RunInfo, index: int) -> str:
    """Render a single RunInfo as an HTML card."""
    badge = f'<span class="badge {_badge_class(run.status)}">{_esc(run.status)}</span>'
    workflow_label = run.workflow.upper()

    summary_rows = {
        t("report_field_workflow"): run.workflow,
        t("report_field_version"): run.version,
        t("report_field_command"): run.command,
        t("report_field_started"): run.started_at,
        t("report_field_completed"): run.completed_at or "-",
        t("report_field_run_dir"): str(run.run_dir),
    }
    core_outputs = _core_outputs(run)
    run_id = _run_dom_id(run, index)
    sample_id = _sample_id(run)
    search_text = _run_search_text(run)

    sections = [
        (
            f'<div class="run-card" id="{_esc(run_id)}" data-workflow="{_esc(run.workflow)}" '
            f'data-status="{_esc(run.status)}" data-sample="{_esc(sample_id)}" '
            f'data-started="{_esc(run.started_at)}" data-search="{_esc(search_text)}">'
        ),
        f'<div class="run-header"><span class="run-title">{_esc(workflow_label)} · {_esc(sample_id)}</span>{badge}</div>',
        f'<div class="section-title">{_esc(t("report_section_summary"))}</div>',
        _render_kv_table(summary_rows),
        f'<div class="section-title">{_esc(t("report_section_params"))}</div>',
        _render_kv_table(run.parameters),
        f'<div class="section-title">{_esc(t("report_section_core_outputs"))}</div>',
        _render_kv_table(core_outputs),
        f'<div class="section-title">{_esc(t("report_section_inputs"))}</div>',
        _render_kv_table(run.inputs),
        f'<div class="section-title">{_esc(t("report_section_input_details"))}</div>',
        _render_kv_table(run.input_details),
        f'<div class="section-title">{_esc(t("report_section_outputs"))}</div>',
        _render_kv_table(run.outputs),
        f'<div class="section-title">{_esc(t("report_section_runtime"))}</div>',
        _render_kv_table(run.runtime),
        f'<div class="section-title">{_esc(t("report_section_tools"))}</div>',
        _render_kv_table(run.tool_versions),
        f'<div class="section-title">{_esc(t("report_section_logs"))}</div>',
        _render_kv_table(run.logs),
        f'<div class="section-title">{_esc(t("report_section_steps"))}</div>',
        _render_steps_table(run.steps),
    ]
    if run.failure_summary:
        sections.extend(
            [
                f'<div class="section-title">{_esc(t("report_section_diagnostics"))}</div>',
                f"<p>{_esc(run.failure_summary)}</p>",
            ]
        )
    if run.failure_details:
        sections.extend(
            [
                f'<div class="section-title">{_esc(t("report_section_failure_details"))}</div>',
                _render_kv_table(run.failure_details),
            ]
        )
    sections.append("</div>")
    return "\n".join(sections)


class DefaultHTMLTemplate:
    """Built-in HTML report template."""

    def render(self, runs: list[RunInfo], title: str) -> str:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        overview = _build_overview(runs)
        cards = "\n".join(_render_run_card(run, index) for index, run in enumerate(runs))
        subtitle = t("report_subtitle", count=len(runs), time=now)
        return (
            "<!DOCTYPE html>\n"
            '<html lang="en">\n<head>\n'
            '<meta charset="utf-8">\n'
            '<meta name="viewport" content="width=device-width,initial-scale=1">\n'
            f"<title>{_esc(title)}</title>\n"
            f"<style>{_CSS}</style>\n"
            "</head>\n<body>\n"
            '<div class="container">\n'
            f"<h1>{_esc(title)}</h1>\n"
            f'<p class="subtitle">{_esc(subtitle)}</p>\n'
            f"{_render_overview(overview, runs)}\n"
            f"{_render_workflow_summaries(runs)}\n"
            f'<section data-run-list>{cards}</section>\n'
            f'<footer>Generated by BioFlow-CLI v{__version__}</footer>\n'
            f"<script>{_JS}</script>\n"
            "</div>\n</body>\n</html>\n"
        )


# ---------------------------------------------------------------------------
# TUI helpers
# ---------------------------------------------------------------------------

def _default_report_output(input_path: Path) -> Path:
    """Return a sensible default HTML output path for interactive mode."""
    return input_path / "report.html"


def report_menu() -> None:
    """Interactive TUI entry for HTML report generation."""
    console.print(Panel(t("report_title"), style="bold magenta"))

    try:
        input_raw = questionary.path(t("report_input_prompt")).ask()
    except KeyboardInterrupt:
        return
    if not input_raw:
        return

    input_path = Path(input_raw)
    if not input_path.exists() or not input_path.is_dir():
        console.print(t("report_invalid_input", path=str(input_path)), style="bold red")
        input(t("press_enter"))
        return

    try:
        output_raw = questionary.path(
            t("report_output_prompt"),
            default=str(_default_report_output(input_path)),
        ).ask()
    except KeyboardInterrupt:
        return
    if not output_raw:
        return

    try:
        title_raw = questionary.text(t("report_title_prompt"), default="").ask()
    except KeyboardInterrupt:
        return

    console.print(t("report_generating"), style="cyan")
    try:
        output_path = generate_report(
            input_path,
            Path(output_raw),
            title=title_raw or None,
        )
    except FileNotFoundError as exc:
        console.print(str(exc), style="bold red")
        input(t("press_enter"))
        return
    except Exception as exc:
        console.print(t("error_unexpected", err=str(exc)), style="bold red")
        input(t("press_enter"))
        return

    console.print(t("report_done", path=str(output_path)), style="bold green")
    input(t("press_enter"))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_report(
    input_path: Path,
    output_path: Path,
    *,
    title: str | None = None,
    template: ReportTemplate | None = None,
) -> Path:
    """Generate an HTML report from run directory/directories.

    Parameters
    ----------
    input_path:
        A single run directory (containing metadata.json) or a parent
        directory that contains multiple run sub-directories.
    output_path:
        Destination HTML file.
    title:
        Custom report title. Defaults to a localised default.
    template:
        Optional custom template.  Falls back to *DefaultHTMLTemplate*.

    Returns
    -------
    Path
        The written output file path.

    Raises
    ------
    FileNotFoundError
        If no valid run directories are found.
    """
    runs = discover_runs(input_path)
    if not runs:
        raise FileNotFoundError(t("report_no_runs", path=str(input_path)))

    effective_title = title or t("report_default_title")
    tmpl = template or DefaultHTMLTemplate()
    html = tmpl.render(runs, effective_title)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return output_path

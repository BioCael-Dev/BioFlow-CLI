"""BioFlow-CLI report module -- HTML report generation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

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
     line-height:1.6;color:#1a1a1a;background:#f8f9fa;padding:2rem}
.container{max-width:960px;margin:0 auto}
h1{color:#2c3e50;margin-bottom:.5rem}
.subtitle{color:#7f8c8d;margin-bottom:2rem;font-size:.9rem}
.run-card{background:#fff;border:1px solid #e1e4e8;border-radius:8px;
          padding:1.5rem;margin-bottom:1.5rem;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.run-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem}
.run-title{font-size:1.15rem;font-weight:600;color:#2c3e50}
.badge{padding:.2rem .6rem;border-radius:12px;font-size:.8rem;font-weight:500;color:#fff}
.badge-success{background:#27ae60}
.badge-failed{background:#e74c3c}
.badge-running{background:#f39c12}
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
footer{text-align:center;color:#95a5a6;font-size:.8rem;margin-top:2rem;padding-top:1rem;
       border-top:1px solid #e1e4e8}
"""


def _esc(text: Any) -> str:
    """Basic HTML escaping."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _badge_class(status: str) -> str:
    if status == "success":
        return "badge-success"
    if status == "failed":
        return "badge-failed"
    return "badge-running"


def _status_class(status: str) -> str:
    return f"status-{status}" if status in ("success", "failed", "pending", "skipped", "running") else ""


def _render_kv_table(data: dict[str, Any]) -> str:
    """Render a simple key-value dictionary as an HTML table."""
    if not data:
        return "<p>-</p>"
    rows = "".join(
        f"<tr><td>{_esc(k)}</td><td>{_esc(v)}</td></tr>" for k, v in data.items()
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


def _render_run_card(run: RunInfo) -> str:
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

    sections = [
        f'<div class="run-card">',
        f'<div class="run-header"><span class="run-title">{_esc(workflow_label)}</span>{badge}</div>',
        f'<div class="section-title">{_esc(t("report_section_summary"))}</div>',
        _render_kv_table(summary_rows),
        f'<div class="section-title">{_esc(t("report_section_params"))}</div>',
        _render_kv_table(run.parameters),
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
        cards = "\n".join(_render_run_card(r) for r in runs)
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
            f"{cards}\n"
            f'<footer>Generated by BioFlow-CLI v{__version__}</footer>\n'
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

import json
from pathlib import Path

import bioflow.report as report


def _write_metadata(run_dir: Path, payload: dict[str, object]) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "metadata.json").write_text(json.dumps(payload), encoding="utf-8")


def test_parse_metadata_defaults_missing_stats_and_summary(tmp_path: Path) -> None:
    run_root = tmp_path / "runs" / "qc-001"
    _write_metadata(
        run_root,
        {
            "workflow": "qc",
            "status": "success",
            "started_at": "2026-05-15T00:00:00Z",
            "outputs": {},
            "steps": {},
        },
    )

    parsed = report.parse_metadata(run_root)
    assert parsed.stats == {}
    assert parsed.summary == {}


def test_generate_report_renders_overview_filters_and_navigation(tmp_path: Path) -> None:
    runs_root = tmp_path / "runs"
    _write_metadata(
        runs_root / "qc-001",
        {
            "workflow": "qc",
            "version": "0.7.1",
            "status": "success",
            "started_at": "2026-05-15T00:00:00Z",
            "completed_at": "2026-05-15T00:10:00Z",
            "command": "qc",
            "outputs": {"trimmed": "/tmp/reads.trimmed.fastq"},
            "steps": {},
        },
    )
    _write_metadata(
        runs_root / "align-001",
        {
            "workflow": "align",
            "version": "0.7.1",
            "status": "failed",
            "started_at": "2026-05-15T00:00:00Z",
            "completed_at": "2026-05-15T00:10:00Z",
            "command": "align",
            "outputs": {"bam": "/tmp/reads.sorted.bam", "flagstat": "/tmp/reads.sorted.flagstat.txt"},
            "steps": {},
        },
    )
    _write_metadata(
        runs_root / "search-001",
        {
            "workflow": "search",
            "version": "0.7.1",
            "status": "success",
            "started_at": "2026-05-15T00:00:00Z",
            "completed_at": "2026-05-15T00:10:00Z",
            "command": "search",
            "outputs": {"tsv": "/tmp/query.blast.tsv", "summary": "/tmp/search_summary.json"},
            "steps": {},
        },
    )

    html_path = tmp_path / "report.html"
    report.generate_report(runs_root, html_path, title="overview")
    html = html_path.read_text(encoding="utf-8")

    assert "Overview" in html
    assert "Filters" in html
    assert "Navigation" in html
    assert 'data-group="workflow"' in html
    assert 'data-group="status"' in html
    assert 'id="run-qc-' in html
    assert 'id="run-align-' in html
    assert 'href="#run-search-' in html
    assert "Total Runs" in html
    assert "Successful" in html
    assert "Failed" in html


def test_generate_report_renders_workflow_specific_core_outputs(tmp_path: Path) -> None:
    runs_root = tmp_path / "runs"
    _write_metadata(
        runs_root / "align-001",
        {
            "workflow": "align",
            "version": "0.7.1",
            "status": "success",
            "started_at": "2026-05-15T00:00:00Z",
            "completed_at": "2026-05-15T00:10:00Z",
            "command": "align",
            "outputs": {
                "bam": "/tmp/reads.sorted.bam",
                "bai": "/tmp/reads.sorted.bam.bai",
                "flagstat": "/tmp/reads.sorted.flagstat.txt",
            },
            "stats": {"mapped": 8, "mapping_rate": 0.8},
            "steps": {},
        },
    )
    _write_metadata(
        runs_root / "search-001",
        {
            "workflow": "search",
            "version": "0.7.1",
            "status": "success",
            "started_at": "2026-05-15T00:00:00Z",
            "completed_at": "2026-05-15T00:10:00Z",
            "command": "search",
            "outputs": {
                "tsv": "/tmp/query.blast.tsv",
                "summary": "/tmp/search_summary.json",
            },
            "summary": {"hit_count": 3, "best_hit": {"subject_id": "refA"}},
            "steps": {},
        },
    )
    _write_metadata(
        runs_root / "qc-001",
        {
            "workflow": "qc",
            "version": "0.7.1",
            "status": "success",
            "started_at": "2026-05-15T00:00:00Z",
            "completed_at": "2026-05-15T00:10:00Z",
            "command": "qc",
            "outputs": {
                "trimmed_r1": "/tmp/reads_1.paired.fastq",
                "trimmed_r2": "/tmp/reads_2.paired.fastq",
                "unpaired_r1": "/tmp/reads_1.unpaired.fastq",
                "unpaired_r2": "/tmp/reads_2.unpaired.fastq",
            },
            "steps": {},
        },
    )

    html_path = tmp_path / "report.html"
    report.generate_report(runs_root, html_path, title="core")
    html = html_path.read_text(encoding="utf-8")

    assert "Core Outputs" in html
    assert "reads.sorted.bam" in html
    assert "80.00%" in html
    assert "refA" in html
    assert "reads_1.paired.fastq" in html

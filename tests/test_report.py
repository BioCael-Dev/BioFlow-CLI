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


def test_collect_summary_data_and_write_structured_exports(tmp_path: Path) -> None:
    runs_root = tmp_path / "runs"
    _write_metadata(
        runs_root / "align-001",
        {
            "workflow": "align",
            "version": "0.9.2",
            "status": "success",
            "started_at": "2026-07-01T00:00:00Z",
            "completed_at": "2026-07-01T00:10:00Z",
            "command": "align",
            "parameters": {"sample_id": "sample-a"},
            "outputs": {
                "bam": "/tmp/reads.sorted.bam",
                "bai": "/tmp/reads.sorted.bam.bai",
                "flagstat": "/tmp/reads.sorted.flagstat.txt",
            },
            "stats": {"total": 10, "mapped": 8, "mapping_rate": 0.8},
            "steps": {},
        },
    )
    _write_metadata(
        runs_root / "search-001",
        {
            "workflow": "search",
            "version": "0.9.2",
            "status": "success",
            "started_at": "2026-07-01T00:20:00Z",
            "completed_at": "2026-07-01T00:25:00Z",
            "command": "search",
            "outputs": {"tsv": "/tmp/query.blast.tsv"},
            "summary": {"hit_count": 2, "best_hit": {"subject_id": "refA"}, "top_hits": []},
            "steps": {},
        },
    )

    data = report.collect_summary_data(runs_root, project={"project_root": str(runs_root)})

    assert data["schema_version"] == "bioflow.summary.v1"
    assert data["total_runs"] == 2
    assert data["workflow_counts"] == {"align": 1, "search": 1}
    assert data["runs"][0]["sample_id"] == "sample-a"
    assert data["runs"][0]["key_metric"] == "mapping_rate"
    assert data["runs"][0]["key_metric_value"] == 0.8
    assert data["runs"][1]["metrics"]["hit_count"] == 2
    assert data["runs"][1]["metrics"]["best_hit"] == "refA"

    json_path = tmp_path / "summary.json"
    tsv_path = tmp_path / "summary.tsv"
    report.write_summary_json(data, json_path)
    report.write_summary_tsv(data, tsv_path)

    saved = json.loads(json_path.read_text(encoding="utf-8"))
    assert saved["project"]["project_root"] == str(runs_root)
    tsv = tsv_path.read_text(encoding="utf-8")
    assert tsv.startswith("run_dir\tsample_id\tworkflow\tstatus")
    assert "sample-a\talign\tsuccess" in tsv
    assert '""mapping_rate"": 0.8' in tsv


def test_generate_report_renders_multiqc_style_sections(tmp_path: Path) -> None:
    runs_root = tmp_path / "runs"
    _write_metadata(
        runs_root / "qc-001",
        {
            "workflow": "qc",
            "version": "0.9.2",
            "status": "success",
            "started_at": "2026-07-08T00:00:00Z",
            "completed_at": "2026-07-08T00:03:00Z",
            "command": "qc",
            "parameters": {"sample_id": "sample-qc"},
            "outputs": {"trimmed": "/tmp/sample-qc.trimmed.fastq"},
            "stats": {"trimmed_reads": 1200},
            "steps": {},
        },
    )
    _write_metadata(
        runs_root / "align-001",
        {
            "workflow": "align",
            "version": "0.9.2",
            "status": "success",
            "started_at": "2026-07-08T00:05:00Z",
            "completed_at": "2026-07-08T00:15:00Z",
            "command": "align",
            "parameters": {"sample_id": "sample-align"},
            "outputs": {"bam": "/tmp/sample-align.bam"},
            "stats": {"mapped": 900, "mapping_rate": 0.75},
            "steps": {},
        },
    )
    _write_metadata(
        runs_root / "search-001",
        {
            "workflow": "search",
            "version": "0.9.2",
            "status": "failed",
            "started_at": "2026-07-08T00:20:00Z",
            "completed_at": "2026-07-08T00:21:00Z",
            "command": "search",
            "parameters": {"sample_id": "sample-search"},
            "outputs": {"tsv": "/tmp/sample-search.tsv"},
            "summary": {"hit_count": 0},
            "failure_summary": "blastn: database missing",
            "steps": {},
        },
    )

    html_path = tmp_path / "report.html"
    report.generate_report(runs_root, html_path, title="multiqc")
    html = html_path.read_text(encoding="utf-8")

    assert "Success Rate" in html
    assert "66.7%" in html
    assert "Workflow Summaries" in html
    assert "Failure Summary" in html
    assert "blastn: database missing" in html
    assert "Avg Mapping Rate" in html
    assert "75.00%" in html
    assert "Trimmed Reads" in html
    assert "1,200" in html
    assert 'data-report-search' in html
    assert 'data-report-sort' in html
    assert 'data-run-list' in html
    assert 'data-sample="sample-align"' in html
    assert 'data-search="' in html

from __future__ import annotations

import gzip
import json
import sys
from argparse import Namespace
from pathlib import Path

import pytest

import bioflow.cli as cli
import bioflow.project_batch as project_batch
import bioflow.variant as variant
from bioflow.config import ConfigError, load_project_config, load_workflow_config
from bioflow.execution import build_execution_context
from bioflow.report import collect_summary_data, generate_report


VCF_TEXT = """##fileformat=VCFv4.3
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tsample
chr1\t10\t.\tA\tG\t60\tPASS\t.\tGT:DP\t0/1:12
chr1\t20\t.\tAT\tA\t10\tLowQual\t.\tGT:DP\t0/1:2
chr1\t30\t.\tAC\tGT\t50\tPASS\t.\tGT:DP\t0/1:8
chr1\t40\t.\tC\tT,G\t40\tPASS\t.\tGT:DP\t1/2:20
"""


def _write_vcf_gz(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(VCF_TEXT)


def _install_fake_variant_tools(monkeypatch, calls: list[str]) -> None:
    def fake_run(
        command,
        *,
        stdout_log: Path,
        stderr_log: Path,
        stdout_path: Path | None = None,
    ) -> bool:
        raw = list(command.raw_command)
        calls.append(" ".join(raw))
        if raw[:2] == ["samtools", "faidx"]:
            Path(f"{raw[2]}.fai").write_text("chr1\t100\t0\t100\t101\n", encoding="utf-8")
        elif raw[:2] == ["samtools", "index"]:
            Path(f"{raw[-1]}.bai").write_bytes(b"index")
        elif raw[:2] == ["bcftools", "filter"]:
            _write_vcf_gz(Path(raw[raw.index("-o") + 1]))
        elif raw[:2] == ["bcftools", "index"]:
            Path(f"{raw[-1]}.tbi").write_bytes(b"tabix")
        elif raw[:2] == ["bcftools", "stats"]:
            assert stdout_path is not None
            stdout_path.write_text("SN\t0\tnumber of records:\t4\n", encoding="utf-8")
        return True

    def fake_pipeline(
        _ref: Path,
        _bam: Path,
        raw_bcf: Path,
        **_kwargs: object,
    ) -> bool:
        calls.append("bcftools mpileup | bcftools call")
        raw_bcf.write_bytes(b"BCF")
        return True

    monkeypatch.setattr(variant, "_run_command", fake_run)
    monkeypatch.setattr(variant, "_run_mpileup_call_pipeline", fake_pipeline)
    monkeypatch.setattr(variant, "collect_tool_versions", lambda _tools: {"samtools": "1.22", "bcftools": "1.22"})


def _write_variant_project_result(
    project_root: Path,
    *,
    sample_id: str,
    reference_sha256: str,
    stats: dict[str, object],
    include_index: bool = True,
) -> project_batch.ProjectJobResult:
    run_dir = project_root / f"{sample_id}-variant"
    results_dir = run_dir / "results"
    results_dir.mkdir(parents=True)
    vcf_path = results_dir / f"{sample_id}.vcf.gz"
    index_path = Path(f"{vcf_path}.tbi")
    vcf_path.write_bytes(b"vcf")
    if include_index:
        index_path.write_bytes(b"index")
    outputs = {
        "vcf": str(vcf_path),
        "vcf_index": str(index_path),
        "summary": str(results_dir / "variant_summary.json"),
    }
    (run_dir / "metadata.json").write_text(
        json.dumps(
            {
                "workflow": "variant",
                "version": "1.1.1",
                "status": "success",
                "started_at": "2026-10-07T00:00:00Z",
                "parameters": {
                    "sample_id": sample_id,
                    "caller": "bcftools",
                    "min_qual": 20.0,
                    "min_depth": 5,
                },
                "inputs": {"ref": "/data/ref.fa", "bam": f"/data/{sample_id}.bam"},
                "input_details": {"ref": {"sha256": reference_sha256}},
                "outputs": outputs,
                "stats": stats,
            }
        ),
        encoding="utf-8",
    )
    return project_batch.ProjectJobResult(
        sample_id=sample_id,
        workflow="variant",
        run_dir=run_dir,
        metadata_path=run_dir / "metadata.json",
        status="success",
        outputs=outputs,
    )


def test_summarize_vcf_counts_record_types_and_filters(tmp_path: Path) -> None:
    vcf_path = tmp_path / "calls.vcf.gz"
    _write_vcf_gz(vcf_path)

    stats = variant.summarize_vcf(vcf_path)

    assert stats == {
        "total_variants": 4,
        "pass_variants": 3,
        "filtered_variants": 1,
        "snp_count": 2,
        "indel_count": 1,
        "mnp_count": 1,
        "other_variant_count": 0,
        "multiallelic_count": 1,
        "mean_quality": 40.0,
    }


def test_mpileup_call_pipeline_streams_between_resolved_commands(
    tmp_path: Path,
    monkeypatch,
) -> None:
    ref = tmp_path / "ref.fa"
    bam = tmp_path / "sample.bam"
    raw_bcf = tmp_path / "results" / "sample.raw.bcf"
    raw_bcf.parent.mkdir()
    ref.write_text(">chr1\nACGT\n", encoding="utf-8")
    bam.write_bytes(b"bam")
    first = (
        sys.executable,
        "-c",
        "import sys; sys.stdout.buffer.write(b'fake-bcf')",
    )
    second = (
        sys.executable,
        "-c",
        f"import pathlib,sys; pathlib.Path({str(raw_bcf)!r}).write_bytes(sys.stdin.buffer.read())",
    )
    monkeypatch.setattr(
        variant,
        "resolve_pipeline_commands",
        lambda *args, **kwargs: [
            variant.ResolvedCommand(first, first, "system", "fingerprint"),
            variant.ResolvedCommand(second, second, "system", "fingerprint"),
        ],
    )

    assert variant._run_mpileup_call_pipeline(
        ref,
        bam,
        raw_bcf,
        threads=1,
        execution={},
        stdout_log=tmp_path / "stdout.log",
        stderr_log=tmp_path / "stderr.log",
    )
    assert raw_bcf.read_bytes() == b"fake-bcf"


def test_run_command_can_capture_stdout_as_result_file(tmp_path: Path) -> None:
    command = (
        sys.executable,
        "-c",
        "print('SN\\t0\\tnumber of records:\\t4')",
    )
    resolved = variant.ResolvedCommand(command, command, "system", "fingerprint")
    stats_path = tmp_path / "stats.txt"

    assert variant._run_command(
        resolved,
        stdout_log=tmp_path / "stdout.log",
        stderr_log=tmp_path / "stderr.log",
        stdout_path=stats_path,
    )
    assert "number of records" in stats_path.read_text(encoding="utf-8")


def test_variant_workflow_config_validates_schema(tmp_path: Path) -> None:
    config_path = tmp_path / "variant.yml"
    config_path.write_text(
        "\n".join(
            [
                "variant:",
                "  ref: ref.fa",
                "  bam: aligned.bam",
                "  caller: bcftools",
                "  min_qual: 25",
                "  min_depth: 4",
                "  threads: 2",
            ]
        ),
        encoding="utf-8",
    )

    config = load_workflow_config(config_path, "variant")

    assert config["caller"] == "bcftools"
    assert config["min_qual"] == 25
    assert config["min_depth"] == 4


def test_variant_workflow_config_rejects_unknown_caller(tmp_path: Path) -> None:
    config_path = tmp_path / "variant.yml"
    config_path.write_text("variant:\n  caller: unknown\n", encoding="utf-8")

    with pytest.raises(ConfigError, match="caller"):
        load_workflow_config(config_path, "variant")


def test_variant_workflow_config_requires_compressed_vcf_output(tmp_path: Path) -> None:
    config_path = tmp_path / "variant.yml"
    config_path.write_text("variant:\n  output: calls.vcf\n", encoding="utf-8")

    with pytest.raises(ConfigError, match=r"\.vcf\.gz"):
        load_workflow_config(config_path, "variant")


def test_project_variant_requires_reference_and_bam(tmp_path: Path) -> None:
    config_path = tmp_path / "project.yml"
    config_path.write_text(
        "\n".join(
            [
                "samples:",
                "  - sample_id: sample-variant",
                "    workflow: variant",
                "    ref: ref.fa",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="requires non-empty bam"):
        load_project_config(config_path)


def test_run_variant_pipeline_writes_outputs_metadata_and_stats(tmp_path: Path, monkeypatch) -> None:
    ref = tmp_path / "ref.fa"
    bam = tmp_path / "sample.bam"
    ref.write_text(">chr1\nACGT\n", encoding="utf-8")
    bam.write_bytes(b"bam")
    calls: list[str] = []
    _install_fake_variant_tools(monkeypatch, calls)
    execution = build_execution_context(
        {"backend": "conda", "conda_env": "bioflow-env", "threads": 4},
        source="test",
    )

    result = variant.run_variant_pipeline(
        ref,
        bam,
        outdir=tmp_path / "variant-run",
        min_qual=20,
        min_depth=5,
        threads=4,
        sample_id="sample-a",
        execution=execution,
        skip_preflight=True,
    )

    assert result is not None
    assert result["stats"]["total_variants"] == 4
    assert result["stats"]["pass_variants"] == 3
    assert Path(result["vcf"]).is_file()
    assert Path(result["vcf_index"]).is_file()
    assert Path(result["bcf"]).is_file()
    assert Path(result["bcftools_stats"]).is_file()
    assert Path(result["summary"]).is_file()
    metadata = json.loads((tmp_path / "variant-run" / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["workflow"] == "variant"
    assert metadata["status"] == "success"
    assert metadata["parameters"]["min_depth"] == 5
    assert metadata["execution"]["backend"] == "conda"
    assert metadata["tool_versions"]["bcftools"] == "1.22"
    assert all(step["status"] == "success" for step in metadata["steps"].values())
    assert "bcftools mpileup" in metadata["steps"]["variant_call"]["raw_command"]
    assert "bcftools call" in metadata["steps"]["variant_call"]["raw_command"]
    assert any("FORMAT/DP<5" in call for call in calls)


def test_variant_resume_reuses_outputs_and_input_change_invalidates(tmp_path: Path, monkeypatch) -> None:
    ref = tmp_path / "ref.fa"
    bam = tmp_path / "sample.bam"
    ref.write_text(">chr1\nACGT\n", encoding="utf-8")
    bam.write_bytes(b"bam-v1")
    calls: list[str] = []
    _install_fake_variant_tools(monkeypatch, calls)
    outdir = tmp_path / "variant-run"

    assert variant.run_variant_pipeline(ref, bam, outdir=outdir, skip_preflight=True) is not None
    initial_count = len(calls)
    assert variant.run_variant_pipeline(ref, bam, outdir=outdir, resume=True, skip_preflight=True) is not None
    assert len(calls) == initial_count
    metadata = json.loads((outdir / "metadata.json").read_text(encoding="utf-8"))
    assert all(step["status"] == "skipped" for step in metadata["steps"].values())

    bam.write_bytes(b"bam-v2")
    assert variant.run_variant_pipeline(ref, bam, outdir=outdir, resume=True, skip_preflight=True) is not None
    assert len(calls) > initial_count
    metadata = json.loads((outdir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["steps"]["variant_call"]["status"] == "success"


def test_variant_resume_invalidates_when_output_path_changes(tmp_path: Path, monkeypatch) -> None:
    ref = tmp_path / "ref.fa"
    bam = tmp_path / "sample.bam"
    ref.write_text(">chr1\nACGT\n", encoding="utf-8")
    bam.write_bytes(b"bam")
    calls: list[str] = []
    _install_fake_variant_tools(monkeypatch, calls)
    outdir = tmp_path / "variant-run"

    assert variant.run_variant_pipeline(
        ref,
        bam,
        output=Path("first.vcf.gz"),
        outdir=outdir,
        skip_preflight=True,
    )
    initial_count = len(calls)
    result = variant.run_variant_pipeline(
        ref,
        bam,
        output=Path("second.vcf.gz"),
        outdir=outdir,
        resume=True,
        skip_preflight=True,
    )

    assert result is not None
    assert len(calls) > initial_count
    assert Path(result["vcf"]).name == "second.vcf.gz"
    assert Path(result["vcf"]).is_file()


def test_variant_resume_invalidates_filter_and_execution_changes(tmp_path: Path, monkeypatch) -> None:
    ref = tmp_path / "ref.fa"
    bam = tmp_path / "sample.bam"
    ref.write_text(">chr1\nACGT\n", encoding="utf-8")
    bam.write_bytes(b"bam")
    calls: list[str] = []
    _install_fake_variant_tools(monkeypatch, calls)
    outdir = tmp_path / "variant-run"
    system_execution = build_execution_context(
        {"backend": "system", "threads": 1},
        source="test",
    )

    assert variant.run_variant_pipeline(
        ref,
        bam,
        outdir=outdir,
        min_qual=20,
        execution=system_execution,
        skip_preflight=True,
    )
    initial_count = len(calls)
    assert variant.run_variant_pipeline(
        ref,
        bam,
        outdir=outdir,
        min_qual=30,
        resume=True,
        execution=system_execution,
        skip_preflight=True,
    )
    assert len(calls) > initial_count

    after_filter_change = len(calls)
    conda_execution = build_execution_context(
        {"backend": "conda", "conda_env": "bioflow-env", "threads": 1},
        source="test",
    )
    assert variant.run_variant_pipeline(
        ref,
        bam,
        outdir=outdir,
        min_qual=30,
        resume=True,
        execution=conda_execution,
        skip_preflight=True,
    )
    assert len(calls) > after_filter_change


def test_variant_failure_records_diagnostics(tmp_path: Path, monkeypatch) -> None:
    ref = tmp_path / "ref.fa"
    bam = tmp_path / "sample.bam"
    ref.write_text(">chr1\nACGT\n", encoding="utf-8")
    bam.write_bytes(b"bam")
    calls: list[str] = []
    _install_fake_variant_tools(monkeypatch, calls)
    original_run = variant._run_command

    def fail_filter(
        command,
        *,
        stdout_log: Path,
        stderr_log: Path,
        stdout_path: Path | None = None,
    ) -> bool:
        if list(command.raw_command)[:2] == ["bcftools", "filter"]:
            stderr_log.write_text("filter failed\n", encoding="utf-8")
            return False
        return original_run(
            command,
            stdout_log=stdout_log,
            stderr_log=stderr_log,
            stdout_path=stdout_path,
        )

    monkeypatch.setattr(variant, "_run_command", fail_filter)
    outdir = tmp_path / "variant-run"

    result = variant.run_variant_pipeline(ref, bam, outdir=outdir, skip_preflight=True)

    assert result is None
    metadata = json.loads((outdir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["status"] == "failed"
    assert metadata["failure_details"]["failed_step"] == "variant_filter"
    assert "bcftools" in metadata["failure_details"]["failed_command"]


def test_project_dispatches_variant_workflow(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_variant(ref: Path, bam: Path, **kwargs: object) -> dict[str, object]:
        captured.update({"ref": ref, "bam": bam, **kwargs})
        outdir = kwargs["outdir"]
        assert isinstance(outdir, Path)
        outdir.mkdir(parents=True, exist_ok=True)
        (outdir / "metadata.json").write_text(
            json.dumps(
                {
                    "workflow": "variant",
                    "status": "success",
                    "started_at": "2026-09-17T00:00:00Z",
                    "outputs": {"vcf": str(outdir / "results" / "sample.vcf.gz")},
                    "stats": {"total_variants": 7, "pass_variants": 6},
                }
            ),
            encoding="utf-8",
        )
        return {"status": "success"}

    monkeypatch.setattr(project_batch, "run_variant_pipeline", fake_variant)
    run_dir = tmp_path / "001-sample-variant-variant"

    result = project_batch._run_project_job(
        run_dir,
        {
            "sample_id": "sample-variant",
            "workflow": "variant",
            "ref": "ref.fa",
            "bam": "sample.bam",
            "min_qual": 30,
            "min_depth": 8,
            "threads": 2,
        },
    )

    assert result.status == "success"
    assert captured["ref"] == Path("ref.fa")
    assert captured["bam"] == Path("sample.bam")
    assert captured["min_qual"] == 30.0
    assert captured["min_depth"] == 8
    assert captured["sample_id"] == "sample-variant"


def test_variant_report_exports_metrics_and_outputs(tmp_path: Path) -> None:
    run_dir = tmp_path / "variant-run"
    run_dir.mkdir()
    (run_dir / "metadata.json").write_text(
        json.dumps(
            {
                "workflow": "variant",
                "version": "1.1.1",
                "status": "success",
                "started_at": "2026-09-17T00:00:00Z",
                "parameters": {"sample_id": "sample-a"},
                "outputs": {
                    "vcf": str(run_dir / "results" / "sample-a.vcf.gz"),
                    "vcf_index": str(run_dir / "results" / "sample-a.vcf.gz.tbi"),
                    "summary": str(run_dir / "results" / "variant_summary.json"),
                },
                "stats": {
                    "total_variants": 12,
                    "pass_variants": 10,
                    "snp_count": 8,
                    "indel_count": 4,
                },
            }
        ),
        encoding="utf-8",
    )

    summary = collect_summary_data(run_dir)
    report_path = generate_report(run_dir, tmp_path / "report.html", title="Variant Report")
    html = report_path.read_text(encoding="utf-8")

    assert summary["runs"][0]["key_metric"] == "pass_variants"
    assert summary["runs"][0]["metrics"]["snp_count"] == 8
    assert summary["runs"][0]["outputs"]["vcf"].endswith("sample-a.vcf.gz")
    assert "VARIANT" in html
    assert "PASS Variants" in html
    assert "sample-a.vcf.gz" in html


def test_variant_project_exports_stable_comparison_and_merge_metadata(tmp_path: Path) -> None:
    project_root = tmp_path / "variant-project"
    project_root.mkdir()
    stats_a = {
        "total_variants": 12,
        "pass_variants": 10,
        "filtered_variants": 2,
        "snp_count": 8,
        "indel_count": 3,
        "mnp_count": 1,
        "other_variant_count": 0,
        "multiallelic_count": 1,
        "mean_quality": 42.5,
    }
    stats_b = {
        "total_variants": 7,
        "pass_variants": 6,
        "filtered_variants": 1,
        "snp_count": 5,
        "indel_count": 2,
        "mnp_count": 0,
        "other_variant_count": 0,
        "multiallelic_count": 0,
        "mean_quality": 38.0,
    }
    results = [
        _write_variant_project_result(
            project_root,
            sample_id="sample-a",
            reference_sha256="shared-reference",
            stats=stats_a,
        ),
        _write_variant_project_result(
            project_root,
            sample_id="sample-b",
            reference_sha256="shared-reference",
            stats=stats_b,
        ),
    ]
    planned_samples = [
        {"sample_id": "sample-a", "workflow": "variant", "ref": "/data/ref.fa"},
        {"sample_id": "sample-b", "workflow": "variant", "ref": "/data/ref.fa"},
    ]

    exported = project_batch._write_variant_project_exports(
        project_root,
        planned_samples=planned_samples,
        results=results,
    )

    assert exported["schema_version"] == "bioflow.variant-summary.v1"
    assert exported["planned_sample_count"] == 2
    assert exported["comparison_sample_count"] == 2
    assert exported["project_counts"]["total_variants"] == 19
    assert exported["project_counts"]["pass_variants"] == 16
    assert exported["project_counts"]["mnp_count"] == 1
    assert exported["cohort"]["reference_consistent"] is True
    assert exported["cohort"]["merge_ready"] is True
    assert [item["sample_id"] for item in exported["cohort"]["merge_inputs"]] == [
        "sample-a",
        "sample-b",
    ]

    saved = json.loads(Path(exported["variant_summary_json"]).read_text(encoding="utf-8"))
    assert saved["samples"][0]["snp_count"] == 8
    tsv_lines = Path(exported["variant_summary_tsv"]).read_text(encoding="utf-8").splitlines()
    assert tsv_lines[0].split("\t") == list(project_batch.VARIANT_SUMMARY_COLUMNS)
    assert "sample-a\tsuccess\tready" in tsv_lines[1]
    assert "\ttrue\t" in tsv_lines[1]

    (project_root / "project_summary.json").write_text(
        json.dumps({"project_root": str(project_root), "variant": exported}),
        encoding="utf-8",
    )
    html_path = generate_report(project_root, project_root / "project_report.html")
    html = html_path.read_text(encoding="utf-8")
    assert "Variant Project Summary" in html
    assert "Sample Variant Comparison" in html
    assert "sample-a" in html
    assert "sample-b" in html
    assert "variant_summary.tsv" in html
    assert "sample-a-variant/results/sample-a.vcf.gz" in html
    assert "data-variant-search" in html
    assert 'data-search="sample-a ready success' in html


def test_variant_project_exports_diagnostics_and_reference_mismatch(tmp_path: Path) -> None:
    project_root = tmp_path / "variant-project"
    project_root.mkdir()
    complete_stats = {
        "total_variants": 4,
        "pass_variants": 3,
        "filtered_variants": 1,
        "snp_count": 2,
        "indel_count": 1,
        "mnp_count": 1,
        "other_variant_count": 0,
        "multiallelic_count": 0,
        "mean_quality": 30.0,
    }
    result_a = _write_variant_project_result(
        project_root,
        sample_id="sample-a",
        reference_sha256="reference-a",
        stats=complete_stats,
    )
    result_b = _write_variant_project_result(
        project_root,
        sample_id="sample-b",
        reference_sha256="reference-b",
        stats=complete_stats,
    )
    result_missing = _write_variant_project_result(
        project_root,
        sample_id="sample-missing",
        reference_sha256="reference-a",
        stats=complete_stats,
        include_index=False,
    )
    result_no_reference = _write_variant_project_result(
        project_root,
        sample_id="sample-no-reference",
        reference_sha256="",
        stats=complete_stats,
    )
    failed_run = project_root / "sample-failed-variant"
    failed_run.mkdir()
    failed = project_batch.ProjectJobResult(
        sample_id="sample-failed",
        workflow="variant",
        run_dir=failed_run,
        metadata_path=failed_run / "metadata.json",
        status="failed",
        outputs={},
        error="variant call failed",
    )
    planned_samples = [
        {"sample_id": sample_id, "workflow": "variant", "ref": "/data/ref.fa"}
        for sample_id in (
            "sample-a",
            "sample-b",
            "sample-missing",
            "sample-no-reference",
            "sample-failed",
            "sample-not-run",
        )
    ]

    exported = project_batch._write_variant_project_exports(
        project_root,
        planned_samples=planned_samples,
        results=[result_a, result_b, result_missing, result_no_reference, failed],
    )

    assert exported["failed_samples"] == ["sample-failed"]
    assert exported["missing_result_samples"] == ["sample-missing"]
    assert exported["missing_reference_metadata_samples"] == ["sample-no-reference"]
    assert exported["not_run_samples"] == ["sample-not-run"]
    assert exported["reference_inconsistent_samples"] == ["sample-a", "sample-b"]
    assert exported["cohort"]["reference_consistent"] is False
    assert exported["cohort"]["merge_ready"] is False
    assert exported["cohort"]["merge_inputs"] == []
    by_sample = {record["sample_id"]: record for record in exported["samples"]}
    assert by_sample["sample-missing"]["comparison_status"] == "missing_result"
    assert "VCF index" in by_sample["sample-missing"]["error"]
    assert by_sample["sample-failed"]["comparison_status"] == "failed"
    assert by_sample["sample-not-run"]["comparison_status"] == "not_run"

    (project_root / "project_summary.json").write_text(
        json.dumps({"project_root": str(project_root), "variant": exported}),
        encoding="utf-8",
    )
    html = generate_report(project_root, project_root / "project_report.html").read_text(
        encoding="utf-8"
    )
    assert "Failed variant samples" in html
    assert "Missing variant results" in html
    assert "Not-run variant samples" in html
    assert "Missing reference metadata" in html
    assert "Reference-inconsistent samples" in html


def test_project_batch_persists_variant_exports_in_project_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root = tmp_path / "project-run"
    config_path = tmp_path / "project.yml"
    config_path.write_text("samples: []\n", encoding="utf-8")
    project_config = {
        "outdir": str(project_root),
        "continue_on_error": False,
        "report_title": "Variant Project",
        "profile": "local",
        "threads": 1,
        "memory": None,
        "queue": None,
        "time_limit": None,
        "backend": "system",
        "conda_env": None,
        "container_image": None,
        "samples": [
            {
                "sample_id": "sample-a",
                "workflow": "variant",
                "ref": "/data/ref.fa",
                "bam": "/data/sample-a.bam",
            },
            {
                "sample_id": "sample-b",
                "workflow": "variant",
                "ref": "/data/ref.fa",
                "bam": "/data/sample-b.bam",
            },
        ],
    }
    stats = {
        "total_variants": 5,
        "pass_variants": 4,
        "filtered_variants": 1,
        "snp_count": 3,
        "indel_count": 1,
        "mnp_count": 1,
        "other_variant_count": 0,
        "multiallelic_count": 0,
        "mean_quality": 35.0,
    }

    def fake_variant(_ref: Path, _bam: Path, **kwargs: object) -> dict[str, object]:
        outdir = kwargs["outdir"]
        sample_id = str(kwargs["sample_id"])
        assert isinstance(outdir, Path)
        results_dir = outdir / "results"
        results_dir.mkdir(parents=True)
        vcf_path = results_dir / f"{sample_id}.vcf.gz"
        vcf_index = Path(f"{vcf_path}.tbi")
        vcf_path.write_bytes(b"vcf")
        vcf_index.write_bytes(b"index")
        outputs = {"vcf": str(vcf_path), "vcf_index": str(vcf_index)}
        (outdir / "metadata.json").write_text(
            json.dumps(
                {
                    "workflow": "variant",
                    "version": "1.1.1",
                    "status": "success",
                    "started_at": "2026-10-07T00:00:00Z",
                    "parameters": {"sample_id": sample_id, "caller": "bcftools"},
                    "inputs": {"ref": "/data/ref.fa", "bam": f"/data/{sample_id}.bam"},
                    "input_details": {"ref": {"sha256": "shared-reference"}},
                    "outputs": outputs,
                    "stats": stats,
                }
            ),
            encoding="utf-8",
        )
        return {"status": "success", **outputs}

    def fake_report(_input: Path, output: Path, **_kwargs: object) -> Path:
        output.write_text("variant report", encoding="utf-8")
        return output

    monkeypatch.setattr(project_batch, "run_variant_pipeline", fake_variant)
    monkeypatch.setattr(project_batch, "generate_report", fake_report)

    result = project_batch.run_project_batch(
        config_path=config_path,
        project_config=project_config,
        quiet=True,
    )

    assert result["variant"]["comparison_sample_count"] == 2
    assert result["variant"]["project_counts"]["total_variants"] == 10
    assert Path(result["variant"]["variant_summary_json"]).is_file()
    assert Path(result["variant"]["variant_summary_tsv"]).is_file()
    project_summary = json.loads(Path(result["summary"]).read_text(encoding="utf-8"))
    assert project_summary["variant"]["cohort"]["merge_ready"] is True
    assert project_summary["variant"]["merge_candidate_count"] == 2


def test_cmd_variant_json_returns_success_payload(tmp_path: Path, monkeypatch, capsys) -> None:
    ref = tmp_path / "ref.fa"
    bam = tmp_path / "sample.bam"
    ref.write_text(">chr1\nACGT\n", encoding="utf-8")
    bam.write_bytes(b"bam")
    monkeypatch.setattr(
        cli,
        "run_variant_pipeline",
        lambda *_args, **_kwargs: {
            "run_dir": str(tmp_path / "variant-run"),
            "vcf": str(tmp_path / "variant-run" / "results" / "sample.vcf.gz"),
            "stats": {"total_variants": 3},
        },
    )

    exit_code = cli.cmd_variant(
        Namespace(
            json=True,
            config=None,
            ref=str(ref),
            bam=str(bam),
            output=None,
            outdir=None,
            caller=None,
            min_qual=None,
            min_depth=None,
            threads=None,
            sample_id=None,
            resume=None,
            profile=None,
            memory=None,
            queue=None,
            time_limit=None,
            backend=None,
            conda_env=None,
            container_image=None,
        )
    )

    assert exit_code == cli.EXIT_SUCCESS
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"
    assert payload["stats"]["total_variants"] == 3
    assert payload["execution"]["backend"] == "system"

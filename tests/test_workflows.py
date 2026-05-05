import json
import subprocess
from argparse import Namespace
from pathlib import Path

import bioflow.alignment as alignment
import bioflow.cli as cli
import bioflow.pipeline as pipeline
import bioflow.report as report
import bioflow.search as search
from bioflow.config import ConfigError, load_workflow_config


def test_qc_pipeline_uses_standard_outdir(tmp_path: Path, monkeypatch) -> None:
    reads = tmp_path / "reads.fastq"
    reads.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")
    run_root = tmp_path / "runs" / "qc-001"

    def fake_fastqc(input_file: Path, output_dir: Path, **_: object) -> bool:
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / f"{input_file.stem}_fastqc.html").write_text("ok", encoding="utf-8")
        return True

    def fake_trimmomatic(input_file: Path, output_file: Path, **_: object) -> bool:
        output_file.write_text(input_file.read_text(encoding="utf-8"), encoding="utf-8")
        return True

    monkeypatch.setattr(pipeline, "_run_fastqc", fake_fastqc)
    monkeypatch.setattr(pipeline, "_run_trimmomatic", fake_trimmomatic)

    assert pipeline.run_qc_pipeline(reads, outdir=run_root, skip_preflight=True) is True
    assert (run_root / "logs").is_dir()
    assert (run_root / "results" / "fastqc_pre").is_dir()
    assert (run_root / "results" / "fastqc_post").is_dir()
    assert (run_root / "results" / "reads.trimmed.fastq").exists()

    metadata = json.loads((run_root / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["status"] == "success"
    assert metadata["outputs"]["trimmed"].endswith("reads.trimmed.fastq")
    assert metadata["input_details"]["input"]["sha256"]
    assert "python_version" in metadata["runtime"]


def test_alignment_pipeline_writes_results_and_metadata(tmp_path: Path, monkeypatch) -> None:
    ref = tmp_path / "ref.fa"
    reads = tmp_path / "reads.fastq"
    ref.write_text(">ref\nACGT\n", encoding="utf-8")
    reads.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")
    run_root = tmp_path / "runs" / "align-001"

    monkeypatch.setattr(alignment, "_run_bwa_index", lambda *args, **kwargs: True)

    def fake_map(_ref: Path, _reads: Path, output_bam: Path, **_: object) -> bool:
        output_bam.write_text("bam", encoding="utf-8")
        return True

    def fake_index(bam: Path, **_: object) -> bool:
        bam.with_suffix(bam.suffix + ".bai").write_text("bai", encoding="utf-8")
        return True

    monkeypatch.setattr(alignment, "_run_bwa_mem_pipe_sort", fake_map)
    monkeypatch.setattr(alignment, "_run_samtools_index", fake_index)
    monkeypatch.setattr(
        alignment,
        "_run_samtools_flagstat",
        lambda *args, **kwargs: "10 + 0 in total (QC-passed reads + QC-failed reads)\n8 + 0 mapped (80.00% : N/A)\n",
    )
    monkeypatch.setattr(alignment, "display_alignment_stats", lambda stats: None)

    stats = alignment.run_alignment_pipeline(ref, reads, outdir=run_root, skip_preflight=True)

    assert stats is not None
    assert (run_root / "results" / "reads.sorted.bam").exists()
    assert (run_root / "results" / "reads.sorted.flagstat.txt").exists()

    metadata = json.loads((run_root / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["status"] == "success"
    assert metadata["stats"]["mapped"] == 8
    assert "bwa" in metadata["tool_versions"]


def test_qc_pipeline_paired_end_writes_results_and_metadata(tmp_path: Path, monkeypatch) -> None:
    reads_r1 = tmp_path / "reads_1.fastq"
    reads_r2 = tmp_path / "reads_2.fastq"
    reads_r1.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")
    reads_r2.write_text("@r1\nTGCA\n+\n!!!!\n", encoding="utf-8")
    run_root = tmp_path / "runs" / "qc-pe-001"
    fastqc_calls: list[str] = []

    def fake_fastqc(input_file: Path, output_dir: Path, **_: object) -> bool:
        fastqc_calls.append(input_file.name)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / f"{input_file.stem}_fastqc.html").write_text("ok", encoding="utf-8")
        return True

    def fake_trimmomatic_pe(
        input_r1: Path,
        input_r2: Path,
        output_r1_paired: Path,
        output_r1_unpaired: Path,
        output_r2_paired: Path,
        output_r2_unpaired: Path,
        **_: object,
    ) -> bool:
        output_r1_paired.write_text(input_r1.read_text(encoding="utf-8"), encoding="utf-8")
        output_r2_paired.write_text(input_r2.read_text(encoding="utf-8"), encoding="utf-8")
        output_r1_unpaired.write_text("", encoding="utf-8")
        output_r2_unpaired.write_text("", encoding="utf-8")
        return True

    monkeypatch.setattr(pipeline, "_run_fastqc", fake_fastqc)
    monkeypatch.setattr(pipeline, "_run_trimmomatic_pe", fake_trimmomatic_pe)

    assert pipeline.run_qc_pipeline(None, input_r1=reads_r1, input_r2=reads_r2, outdir=run_root, skip_preflight=True) is True
    assert fastqc_calls == ["reads_1.fastq", "reads_2.fastq", "reads_1.paired.fastq", "reads_2.paired.fastq"]

    metadata = json.loads((run_root / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["status"] == "success"
    assert metadata["parameters"]["paired"] is True
    assert metadata["inputs"]["input_r1"].endswith("reads_1.fastq")
    assert metadata["inputs"]["input_r2"].endswith("reads_2.fastq")
    assert metadata["outputs"]["trimmed_r1"].endswith("reads_1.paired.fastq")
    assert metadata["outputs"]["trimmed_r2"].endswith("reads_2.paired.fastq")
    assert metadata["outputs"]["unpaired_r1"].endswith("reads_1.unpaired.fastq")
    assert metadata["outputs"]["unpaired_r2"].endswith("reads_2.unpaired.fastq")
    assert metadata["input_details"]["input_r1"]["sha256"]
    assert metadata["input_details"]["input_r2"]["sha256"]


def test_alignment_pipeline_paired_end_writes_results_and_metadata(tmp_path: Path, monkeypatch) -> None:
    ref = tmp_path / "ref.fa"
    reads_r1 = tmp_path / "reads_1.fastq"
    reads_r2 = tmp_path / "reads_2.fastq"
    ref.write_text(">ref\nACGT\n", encoding="utf-8")
    reads_r1.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")
    reads_r2.write_text("@r1\nTGCA\n+\n!!!!\n", encoding="utf-8")
    run_root = tmp_path / "runs" / "align-pe-001"
    map_calls: list[tuple[str, str]] = []

    monkeypatch.setattr(alignment, "_run_bwa_index", lambda *args, **kwargs: True)

    def fake_map(_ref: Path, input_r1: Path, input_r2: Path, output_bam: Path, **_: object) -> bool:
        map_calls.append((input_r1.name, input_r2.name))
        output_bam.write_text("bam", encoding="utf-8")
        return True

    def fake_index(bam: Path, **_: object) -> bool:
        bam.with_suffix(bam.suffix + ".bai").write_text("bai", encoding="utf-8")
        return True

    monkeypatch.setattr(alignment, "_run_bwa_mem_pipe_sort_pe", fake_map)
    monkeypatch.setattr(alignment, "_run_samtools_index", fake_index)
    monkeypatch.setattr(
        alignment,
        "_run_samtools_flagstat",
        lambda *args, **kwargs: (
            "20 + 0 in total (QC-passed reads + QC-failed reads)\n"
            "18 + 0 mapped (90.00% : N/A)\n"
            "20 + 0 paired in sequencing\n"
            "16 + 0 properly paired (80.00% : N/A)\n"
        ),
    )
    monkeypatch.setattr(alignment, "display_alignment_stats", lambda stats: None)

    stats = alignment.run_alignment_pipeline(ref, None, input_r1=reads_r1, input_r2=reads_r2, outdir=run_root, skip_preflight=True)

    assert stats is not None
    assert map_calls == [("reads_1.fastq", "reads_2.fastq")]
    assert stats["paired"] == 20
    assert stats["properly_paired"] == 16

    metadata = json.loads((run_root / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["status"] == "success"
    assert metadata["parameters"]["paired"] is True
    assert metadata["inputs"]["input_r1"].endswith("reads_1.fastq")
    assert metadata["inputs"]["input_r2"].endswith("reads_2.fastq")
    assert metadata["outputs"]["bam"].endswith("reads_1.sorted.bam")
    assert metadata["outputs"]["bai"].endswith("reads_1.sorted.bam.bai")
    assert metadata["stats"]["paired"] == 20
    assert metadata["stats"]["properly_paired"] == 16


def test_search_pipeline_generates_summary_and_metadata(tmp_path: Path, monkeypatch) -> None:
    db = tmp_path / "ref.fa"
    query = tmp_path / "query.fa"
    db.write_text(">ref\nACGT\n", encoding="utf-8")
    query.write_text(">q1\nACGT\n", encoding="utf-8")
    run_root = tmp_path / "runs" / "search-001"

    monkeypatch.setattr(search, "_blast_db_ready", lambda _: False)
    monkeypatch.setattr(search, "_run_makeblastdb", lambda *args, **kwargs: True)

    def fake_blastn(_db: Path, _query: Path, output_path: Path, **_: object) -> bool:
        output_path.write_text(
            "q1\tref\t99.00\t4\t0\t0\t1\t4\t1\t4\t1e-20\t80\n",
            encoding="utf-8",
        )
        return True

    monkeypatch.setattr(search, "_run_blastn", fake_blastn)

    result = search.run_blast_search(db, query, outdir=run_root, skip_preflight=True)

    assert result is not None
    assert result["hits"] == 1
    assert (run_root / "results" / "query.blast.tsv").exists()
    assert (run_root / "results" / "search_summary.json").exists()

    metadata = json.loads((run_root / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["status"] == "success"
    assert metadata["summary"]["hit_count"] == 1
    assert metadata["input_details"]["query"]["size_bytes"] > 0


def test_search_run_cmd_retains_failure_logs(tmp_path: Path, monkeypatch) -> None:
    stdout_log = tmp_path / "logs" / "search.stdout.log"
    stderr_log = tmp_path / "logs" / "search.stderr.log"

    def fail_run(*args: object, **kwargs: object) -> None:
        raise subprocess.CalledProcessError(
            1,
            ["blastn"],
            output="partial stdout",
            stderr="partial stderr",
        )

    monkeypatch.setattr(search.subprocess, "run", fail_run)

    ok = search._run_cmd(
        ["blastn"],
        description="blastn",
        stdout_log=stdout_log,
        stderr_log=stderr_log,
    )

    assert ok is False
    assert "partial stdout" in stdout_log.read_text(encoding="utf-8")
    assert "partial stderr" in stderr_log.read_text(encoding="utf-8")


def test_qc_failure_writes_failure_details(tmp_path: Path, monkeypatch) -> None:
    reads = tmp_path / "reads.fastq"
    reads.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")
    run_root = tmp_path / "runs" / "qc-fail"

    monkeypatch.setattr(pipeline, "_run_fastqc", lambda *args, **kwargs: False)

    assert pipeline.run_qc_pipeline(reads, outdir=run_root, skip_preflight=True) is False
    metadata = json.loads((run_root / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["status"] == "failed"
    assert metadata["failure_details"]["failed_step"] == "fastqc_pre"
    assert "fastqc" in metadata["failure_details"]["failed_command"]
    assert metadata["failure_details"]["stderr_log"].endswith("qc.stderr.log")


def test_cmd_qc_json_reports_outdir(tmp_path: Path, monkeypatch, capsys) -> None:
    reads = tmp_path / "reads.fastq"
    reads.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")
    run_root = tmp_path / "runs" / "qc-001"

    monkeypatch.setattr(cli, "run_qc_pipeline", lambda *args, **kwargs: True)

    exit_code = cli.cmd_qc(
        Namespace(
            quiet=False,
            json=True,
            config=None,
            input=str(reads),
            output=None,
            outdir=str(run_root),
            adapter=None,
            minlen=36,
            resume=False,
        )
    )

    assert exit_code == cli.EXIT_SUCCESS
    payload = json.loads(capsys.readouterr().out)
    assert payload["outdir"] == str(run_root)
    assert payload["metadata"] == str(run_root / "metadata.json")


def test_qc_resume_skips_completed_steps(tmp_path: Path, monkeypatch) -> None:
    reads = tmp_path / "reads.fastq"
    reads.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")
    run_root = tmp_path / "runs" / "qc-001"
    (run_root / "results" / "fastqc_pre").mkdir(parents=True, exist_ok=True)
    (run_root / "results" / "fastqc_post").mkdir(parents=True, exist_ok=True)
    (run_root / "results" / "fastqc_pre" / "reads_fastqc.html").write_text("ok", encoding="utf-8")
    (run_root / "results" / "fastqc_post" / "reads.trimmed_fastqc.html").write_text("ok", encoding="utf-8")
    trimmed = run_root / "results" / "reads.trimmed.fastq"
    trimmed.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")
    (run_root / "metadata.json").write_text(
        json.dumps(
            {
                "steps": {
                    "fastqc_pre": {"status": "success", "outputs": {"dir": str(run_root / "results" / "fastqc_pre")}},
                    "trimmomatic": {"status": "success", "outputs": {"trimmed": str(trimmed)}},
                    "fastqc_post": {"status": "success", "outputs": {"dir": str(run_root / "results" / "fastqc_post")}},
                }
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(pipeline, "_run_fastqc", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should skip fastqc")))
    monkeypatch.setattr(pipeline, "_run_trimmomatic", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should skip trimmomatic")))

    assert pipeline.run_qc_pipeline(reads, outdir=run_root, resume=True, skip_preflight=True) is True
    metadata = json.loads((run_root / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["steps"]["fastqc_pre"]["status"] == "skipped"
    assert metadata["steps"]["trimmomatic"]["status"] == "skipped"
    assert metadata["steps"]["fastqc_post"]["status"] == "skipped"


def test_qc_resume_recomputes_invalid_trimmed_output(tmp_path: Path, monkeypatch) -> None:
    reads = tmp_path / "reads.fastq"
    reads.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")
    run_root = tmp_path / "runs" / "qc-002"
    (run_root / "results" / "fastqc_pre").mkdir(parents=True, exist_ok=True)
    (run_root / "results" / "fastqc_pre" / "reads_fastqc.html").write_text("ok", encoding="utf-8")
    trimmed = run_root / "results" / "reads.trimmed.fastq"
    trimmed.parent.mkdir(parents=True, exist_ok=True)
    trimmed.write_text("", encoding="utf-8")
    (run_root / "metadata.json").write_text(
        json.dumps(
            {
                "steps": {
                    "fastqc_pre": {"status": "success", "outputs": {"dir": str(run_root / "results" / "fastqc_pre")}},
                    "trimmomatic": {"status": "success", "outputs": {"trimmed": str(trimmed)}},
                }
            }
        ),
        encoding="utf-8",
    )

    calls = {"trim": 0, "post": 0}

    def fake_fastqc(input_file: Path, output_dir: Path, **_: object) -> bool:
        calls["post"] += 1
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / f"{input_file.stem}_fastqc.html").write_text("ok", encoding="utf-8")
        return True

    def fake_trimmomatic(input_file: Path, output_file: Path, **_: object) -> bool:
        calls["trim"] += 1
        output_file.write_text(input_file.read_text(encoding="utf-8"), encoding="utf-8")
        return True

    monkeypatch.setattr(pipeline, "_run_fastqc", fake_fastqc)
    monkeypatch.setattr(pipeline, "_run_trimmomatic", fake_trimmomatic)

    assert pipeline.run_qc_pipeline(reads, outdir=run_root, resume=True, skip_preflight=True) is True
    assert calls["trim"] == 1
    assert calls["post"] == 1


def test_qc_resume_recomputes_invalid_paired_outputs(tmp_path: Path, monkeypatch) -> None:
    reads_r1 = tmp_path / "reads_1.fastq"
    reads_r2 = tmp_path / "reads_2.fastq"
    reads_r1.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")
    reads_r2.write_text("@r1\nTGCA\n+\n!!!!\n", encoding="utf-8")
    run_root = tmp_path / "runs" / "qc-pe-002"
    pre_dir = run_root / "results" / "fastqc_pre"
    pre_dir.mkdir(parents=True, exist_ok=True)
    (pre_dir / "reads_1_fastqc.html").write_text("ok", encoding="utf-8")
    (pre_dir / "reads_2_fastqc.html").write_text("ok", encoding="utf-8")
    trimmed_r1 = run_root / "results" / "reads_1.paired.fastq"
    trimmed_r2 = run_root / "results" / "reads_2.paired.fastq"
    unpaired_r1 = run_root / "results" / "reads_1.unpaired.fastq"
    unpaired_r2 = run_root / "results" / "reads_2.unpaired.fastq"
    trimmed_r1.parent.mkdir(parents=True, exist_ok=True)
    trimmed_r1.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")
    trimmed_r2.write_text("@r1\nTGCA\n+\n!!!!\n", encoding="utf-8")
    unpaired_r1.write_text("", encoding="utf-8")
    (run_root / "metadata.json").write_text(
        json.dumps(
            {
                "steps": {
                    "fastqc_pre": {"status": "success", "outputs": {"dir": str(pre_dir)}},
                    "trimmomatic": {
                        "status": "success",
                        "outputs": {
                            "trimmed_r1": str(trimmed_r1),
                            "trimmed_r2": str(trimmed_r2),
                            "unpaired_r1": str(unpaired_r1),
                            "unpaired_r2": str(unpaired_r2),
                        },
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    calls = {"trim": 0, "post": 0}

    def fake_fastqc(input_file: Path, output_dir: Path, **_: object) -> bool:
        calls["post"] += 1
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / f"{input_file.stem}_fastqc.html").write_text("ok", encoding="utf-8")
        return True

    def fake_trimmomatic_pe(
        input_r1: Path,
        input_r2: Path,
        output_r1_paired: Path,
        output_r1_unpaired: Path,
        output_r2_paired: Path,
        output_r2_unpaired: Path,
        **_: object,
    ) -> bool:
        calls["trim"] += 1
        output_r1_paired.write_text(input_r1.read_text(encoding="utf-8"), encoding="utf-8")
        output_r2_paired.write_text(input_r2.read_text(encoding="utf-8"), encoding="utf-8")
        output_r1_unpaired.write_text("", encoding="utf-8")
        output_r2_unpaired.write_text("", encoding="utf-8")
        return True

    monkeypatch.setattr(pipeline, "_run_fastqc", fake_fastqc)
    monkeypatch.setattr(pipeline, "_run_trimmomatic_pe", fake_trimmomatic_pe)

    assert pipeline.run_qc_pipeline(None, input_r1=reads_r1, input_r2=reads_r2, outdir=run_root, resume=True, skip_preflight=True) is True
    assert calls["trim"] == 1
    assert calls["post"] == 2


def test_alignment_resume_skips_completed_steps(tmp_path: Path, monkeypatch) -> None:
    ref = tmp_path / "ref.fa"
    reads = tmp_path / "reads.fastq"
    ref.write_text(">ref\nACGT\n", encoding="utf-8")
    reads.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")
    for suffix in (".amb", ".ann", ".bwt", ".pac", ".sa"):
        ref.with_suffix(ref.suffix + suffix).write_text("idx", encoding="utf-8")
    run_root = tmp_path / "runs" / "align-001"
    bam = run_root / "results" / "reads.sorted.bam"
    bam.parent.mkdir(parents=True, exist_ok=True)
    bam.write_text("bam", encoding="utf-8")
    bam.with_suffix(".bam.bai").write_text("bai", encoding="utf-8")
    (run_root / "results" / "reads.sorted.flagstat.txt").write_text(
        "10 + 0 in total (QC-passed reads + QC-failed reads)\n8 + 0 mapped (80.00% : N/A)\n",
        encoding="utf-8",
    )
    (run_root / "metadata.json").write_text(
        json.dumps(
            {
                "steps": {
                    "bwa_index": {"status": "success", "outputs": {"index_files": [str(ref.with_suffix(ref.suffix + suffix)) for suffix in (".amb", ".ann", ".bwt", ".pac", ".sa")]}},
                    "map_sort": {"status": "success", "outputs": {"bam": str(bam)}},
                    "bam_index": {"status": "success", "outputs": {"bai": str(bam.with_suffix('.bam.bai'))}},
                    "flagstat": {"status": "success", "outputs": {"flagstat": str(run_root / "results" / "reads.sorted.flagstat.txt")}},
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(alignment, "_run_bwa_index", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should skip index")))
    monkeypatch.setattr(alignment, "_run_bwa_mem_pipe_sort", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should skip map")))
    monkeypatch.setattr(alignment, "_run_samtools_index", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should skip bam index")))
    monkeypatch.setattr(alignment, "_run_samtools_flagstat", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should skip flagstat")))
    monkeypatch.setattr(alignment, "display_alignment_stats", lambda stats: None)

    stats = alignment.run_alignment_pipeline(ref, reads, outdir=run_root, resume=True, skip_preflight=True)
    assert stats is not None
    metadata = json.loads((run_root / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["steps"]["bwa_index"]["status"] == "skipped"
    assert metadata["steps"]["flagstat"]["status"] == "skipped"


def test_alignment_resume_recomputes_invalid_flagstat(tmp_path: Path, monkeypatch) -> None:
    ref = tmp_path / "ref.fa"
    reads = tmp_path / "reads.fastq"
    ref.write_text(">ref\nACGT\n", encoding="utf-8")
    reads.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")
    for suffix in (".amb", ".ann", ".bwt", ".pac", ".sa"):
        ref.with_suffix(ref.suffix + suffix).write_text("idx", encoding="utf-8")
    run_root = tmp_path / "runs" / "align-002"
    bam = run_root / "results" / "reads.sorted.bam"
    bam.parent.mkdir(parents=True, exist_ok=True)
    bam.write_text("bam", encoding="utf-8")
    bam.with_suffix(".bam.bai").write_text("bai", encoding="utf-8")
    (run_root / "results" / "reads.sorted.flagstat.txt").write_text("", encoding="utf-8")
    (run_root / "metadata.json").write_text(
        json.dumps(
            {
                "steps": {
                    "bwa_index": {"status": "success", "outputs": {"index_files": [str(ref.with_suffix(ref.suffix + suffix)) for suffix in (".amb", ".ann", ".bwt", ".pac", ".sa")]}},
                    "map_sort": {"status": "success", "outputs": {"bam": str(bam)}},
                    "bam_index": {"status": "success", "outputs": {"bai": str(bam.with_suffix('.bam.bai'))}},
                    "flagstat": {"status": "success", "outputs": {"flagstat": str(run_root / "results" / "reads.sorted.flagstat.txt")}},
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(alignment, "_run_bwa_index", lambda *args, **kwargs: True)
    monkeypatch.setattr(alignment, "_run_bwa_mem_pipe_sort", lambda *args, **kwargs: True)
    monkeypatch.setattr(alignment, "_run_samtools_index", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        alignment,
        "_run_samtools_flagstat",
        lambda *args, **kwargs: "10 + 0 in total (QC-passed reads + QC-failed reads)\n8 + 0 mapped (80.00% : N/A)\n",
    )
    monkeypatch.setattr(alignment, "display_alignment_stats", lambda stats: None)

    stats = alignment.run_alignment_pipeline(ref, reads, outdir=run_root, resume=True, skip_preflight=True)
    assert stats is not None
    assert stats["mapped"] == 8


def test_alignment_resume_skips_completed_steps_for_paired_end(tmp_path: Path, monkeypatch) -> None:
    ref = tmp_path / "ref.fa"
    reads_r1 = tmp_path / "reads_1.fastq"
    reads_r2 = tmp_path / "reads_2.fastq"
    ref.write_text(">ref\nACGT\n", encoding="utf-8")
    reads_r1.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")
    reads_r2.write_text("@r1\nTGCA\n+\n!!!!\n", encoding="utf-8")
    for suffix in (".amb", ".ann", ".bwt", ".pac", ".sa"):
        ref.with_suffix(ref.suffix + suffix).write_text("idx", encoding="utf-8")
    run_root = tmp_path / "runs" / "align-pe-002"
    bam = run_root / "results" / "reads_1.sorted.bam"
    bam.parent.mkdir(parents=True, exist_ok=True)
    bam.write_text("bam", encoding="utf-8")
    bam.with_suffix(".bam.bai").write_text("bai", encoding="utf-8")
    flagstat = run_root / "results" / "reads_1.sorted.flagstat.txt"
    flagstat.write_text(
        "20 + 0 in total (QC-passed reads + QC-failed reads)\n"
        "18 + 0 mapped (90.00% : N/A)\n"
        "20 + 0 paired in sequencing\n"
        "16 + 0 properly paired (80.00% : N/A)\n",
        encoding="utf-8",
    )
    (run_root / "metadata.json").write_text(
        json.dumps(
            {
                "steps": {
                    "bwa_index": {"status": "success", "outputs": {"index_files": [str(ref.with_suffix(ref.suffix + suffix)) for suffix in (".amb", ".ann", ".bwt", ".pac", ".sa")]}},
                    "map_sort": {"status": "success", "outputs": {"bam": str(bam)}},
                    "bam_index": {"status": "success", "outputs": {"bai": str(bam.with_suffix('.bam.bai'))}},
                    "flagstat": {"status": "success", "outputs": {"flagstat": str(flagstat)}},
                }
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(alignment, "_run_bwa_index", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should skip index")))
    monkeypatch.setattr(alignment, "_run_bwa_mem_pipe_sort_pe", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should skip paired map")))
    monkeypatch.setattr(alignment, "_run_samtools_index", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should skip bam index")))
    monkeypatch.setattr(alignment, "_run_samtools_flagstat", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should skip flagstat")))
    monkeypatch.setattr(alignment, "display_alignment_stats", lambda stats: None)

    stats = alignment.run_alignment_pipeline(ref, None, input_r1=reads_r1, input_r2=reads_r2, outdir=run_root, resume=True, skip_preflight=True)
    assert stats is not None
    assert stats["properly_paired"] == 16
    metadata = json.loads((run_root / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["steps"]["map_sort"]["status"] == "skipped"
    assert metadata["steps"]["flagstat"]["status"] == "skipped"


def test_search_resume_skips_completed_steps(tmp_path: Path, monkeypatch) -> None:
    db = tmp_path / "ref.fa"
    query = tmp_path / "query.fa"
    db.write_text(">ref\nACGT\n", encoding="utf-8")
    query.write_text(">q1\nACGT\n", encoding="utf-8")
    for suffix in (".nhr", ".nin", ".nsq"):
        db.with_suffix(db.suffix + suffix).write_text("db", encoding="utf-8")
    run_root = tmp_path / "runs" / "search-001"
    tsv = run_root / "results" / "query.blast.tsv"
    tsv.parent.mkdir(parents=True, exist_ok=True)
    tsv.write_text("q1\tref\t99.00\t4\t0\t0\t1\t4\t1\t4\t1e-20\t80\n", encoding="utf-8")
    (run_root / "results" / "search_summary.json").write_text(
        json.dumps({"hit_count": 1, "best_hit": {"subject_id": "ref"}, "top_hits": []}),
        encoding="utf-8",
    )
    (run_root / "metadata.json").write_text(
        json.dumps(
            {
                "steps": {
                    "makeblastdb": {"status": "success", "outputs": {"db": str(db)}},
                    "blastn": {"status": "success", "outputs": {"tsv": str(tsv)}},
                    "summary": {"status": "success", "outputs": {"summary": str(run_root / "results" / "search_summary.json")}},
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(search, "_run_makeblastdb", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should skip db")))
    monkeypatch.setattr(search, "_run_blastn", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should skip blastn")))

    result = search.run_blast_search(db, query, outdir=run_root, resume=True, skip_preflight=True)
    assert result is not None
    metadata = json.loads((run_root / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["steps"]["makeblastdb"]["status"] == "skipped"
    assert metadata["steps"]["summary"]["status"] == "skipped"


def test_cmd_search_json_reports_resume_used(tmp_path: Path, monkeypatch, capsys) -> None:
    db = tmp_path / "ref.fa"
    query = tmp_path / "query.fa"
    db.write_text(">ref\nACGT\n", encoding="utf-8")
    query.write_text(">q1\nACGT\n", encoding="utf-8")
    run_root = tmp_path / "runs" / "search-001"

    monkeypatch.setattr(
        cli,
        "run_blast_search",
        lambda *args, **kwargs: {
            "db": str(db),
            "query": str(query),
            "output": str(run_root / "results" / "query.blast.tsv"),
            "outdir": str(run_root),
            "hits": 1,
            "evalue": 10.0,
            "max_target_seqs": 10,
            "top_n": 5,
            "resume_used": True,
            "summary": {"hit_count": 1, "best_hit": None, "top_hits": []},
        },
    )

    exit_code = cli.cmd_search(
        Namespace(
            quiet=False,
            json=True,
            config=None,
            db=str(db),
            query=str(query),
            output=None,
            outdir=str(run_root),
            evalue=10.0,
            max_target_seqs=10,
            top=5,
            resume=True,
        )
    )

    assert exit_code == cli.EXIT_SUCCESS
    payload = json.loads(capsys.readouterr().out)
    assert payload["resume_used"] is True


def test_cmd_search_failure_prints_diagnostics(tmp_path: Path, monkeypatch, capsys) -> None:
    db = tmp_path / "ref.fa"
    query = tmp_path / "query.fa"
    db.write_text(">ref\nACGT\n", encoding="utf-8")
    query.write_text(">q1\nACGT\n", encoding="utf-8")
    run_root = tmp_path / "runs" / "search-fail"
    run_root.mkdir(parents=True, exist_ok=True)
    (run_root / "metadata.json").write_text(
        json.dumps(
            {
                "failure_details": {
                    "failed_step": "blastn",
                    "failed_command": "blastn -query query.fa -db ref.fa",
                    "stdout_log": str(run_root / "logs" / "search.stdout.log"),
                    "stderr_log": str(run_root / "logs" / "search.stderr.log"),
                    "stderr_tail": "mock tail",
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(cli, "run_blast_search", lambda *args, **kwargs: None)

    exit_code = cli.cmd_search(
        Namespace(
            quiet=False,
            json=False,
            config=None,
            db=str(db),
            query=str(query),
            output=None,
            outdir=str(run_root),
            evalue=10.0,
            max_target_seqs=10,
            top=5,
            resume=False,
        )
    )

    assert exit_code == cli.EXIT_RUNTIME_ERROR
    assert "Failed Step: blastn" in capsys.readouterr().err


def test_cmd_qc_rejects_mixed_single_and_paired_inputs(tmp_path: Path, capsys) -> None:
    reads = tmp_path / "reads.fastq"
    reads_r1 = tmp_path / "reads_1.fastq"
    reads_r2 = tmp_path / "reads_2.fastq"
    for path in (reads, reads_r1, reads_r2):
        path.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")

    exit_code = cli.cmd_qc(
        Namespace(
            quiet=False,
            json=True,
            config=None,
            input=str(reads),
            input_r1=str(reads_r1),
            input_r2=str(reads_r2),
            output=None,
            outdir=None,
            adapter=None,
            minlen=36,
            resume=False,
        )
    )

    assert exit_code == cli.EXIT_ARGUMENT_ERROR
    payload = json.loads(capsys.readouterr().out)
    assert payload["error"] == "invalid_input_combination"
    assert "cannot mix" in payload["message"]


def test_cmd_qc_rejects_incomplete_paired_inputs(tmp_path: Path, capsys) -> None:
    reads_r1 = tmp_path / "reads_1.fastq"
    reads_r1.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")

    exit_code = cli.cmd_qc(
        Namespace(
            quiet=False,
            json=True,
            config=None,
            input=None,
            input_r1=str(reads_r1),
            input_r2=None,
            output=None,
            outdir=None,
            adapter=None,
            minlen=36,
            resume=False,
        )
    )

    assert exit_code == cli.EXIT_ARGUMENT_ERROR
    payload = json.loads(capsys.readouterr().out)
    assert payload["error"] == "invalid_input_combination"
    assert "requires both input_r1 and input_r2" in payload["message"]


def test_cmd_align_rejects_incomplete_paired_inputs(tmp_path: Path, capsys) -> None:
    ref = tmp_path / "ref.fa"
    reads_r1 = tmp_path / "reads_1.fastq"
    ref.write_text(">ref\nACGT\n", encoding="utf-8")
    reads_r1.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")

    exit_code = cli.cmd_align(
        Namespace(
            quiet=False,
            json=True,
            config=None,
            ref=str(ref),
            input=None,
            input_r1=str(reads_r1),
            input_r2=None,
            output=None,
            outdir=None,
            threads=1,
            resume=False,
        )
    )

    assert exit_code == cli.EXIT_ARGUMENT_ERROR
    payload = json.loads(capsys.readouterr().out)
    assert payload["error"] == "invalid_input_combination"
    assert "requires both input_r1 and input_r2" in payload["message"]


def test_qc_config_rejects_mixed_single_and_paired_inputs(tmp_path: Path) -> None:
    config_path = tmp_path / "qc.yml"
    config_path.write_text(
        "qc:\n  input: reads.fastq\n  input_r1: reads_1.fastq\n  input_r2: reads_2.fastq\n",
        encoding="utf-8",
    )

    try:
        load_workflow_config(config_path, "qc")
    except ConfigError as exc:
        assert "cannot mix" in str(exc)
    else:
        raise AssertionError("expected ConfigError")


def test_align_config_rejects_incomplete_paired_inputs(tmp_path: Path) -> None:
    config_path = tmp_path / "align.yml"
    config_path.write_text(
        "align:\n  ref: ref.fa\n  input_r1: reads_1.fastq\n",
        encoding="utf-8",
    )

    try:
        load_workflow_config(config_path, "align")
    except ConfigError as exc:
        assert "requires both 'input_r1' and 'input_r2'" in str(exc)
    else:
        raise AssertionError("expected ConfigError")


def test_report_renders_nested_paired_metadata(tmp_path: Path) -> None:
    run_root = tmp_path / "runs" / "qc-pe-003"
    run_root.mkdir(parents=True, exist_ok=True)
    (run_root / "metadata.json").write_text(
        json.dumps(
            {
                "workflow": "qc",
                "version": "0.7.0",
                "status": "success",
                "started_at": "2026-05-06T00:00:00Z",
                "completed_at": "2026-05-06T00:10:00Z",
                "command": "qc",
                "parameters": {"paired": True},
                "inputs": {"input_r1": "/tmp/reads_1.fastq", "input_r2": "/tmp/reads_2.fastq"},
                "outputs": {"trimmed_r1": "/tmp/reads_1.paired.fastq", "trimmed_r2": "/tmp/reads_2.paired.fastq"},
                "steps": {},
                "logs": {},
                "runtime": {},
                "tool_versions": {},
                "input_details": {
                    "input_r1": {"size_bytes": 12, "sha256": "abc"},
                    "input_r2": {"size_bytes": 12, "sha256": "def"},
                },
                "failure_summary": "",
                "failure_details": {},
            }
        ),
        encoding="utf-8",
    )

    html_path = tmp_path / "report.html"
    report.generate_report(run_root, html_path, title="paired")
    html = html_path.read_text(encoding="utf-8")
    assert "input_r1" in html
    assert "sha256" in html
    assert "reads_1.paired.fastq" in html

import json
import subprocess
from argparse import Namespace
from pathlib import Path

import bioflow.alignment as alignment
import bioflow.cli as cli
import bioflow.pipeline as pipeline
import bioflow.search as search


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

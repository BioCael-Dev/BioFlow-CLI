import json
from argparse import Namespace
from pathlib import Path

import bioflow.cli as cli
import bioflow.project_batch as project_batch
from bioflow.config import ConfigError, load_project_config


def test_load_project_config_accepts_wrapped_project_section(tmp_path: Path) -> None:
    config_path = tmp_path / "project.yml"
    config_path.write_text(
        "\n".join(
            [
                "project:",
                "  outdir: runs/project-001",
                "  continue_on_error: true",
                "  report_title: Demo",
                "  samples:",
                "    - sample_id: sample-qc",
                "      workflow: qc",
                "      input: reads.fastq",
                "    - sample_id: sample-search",
                "      workflow: search",
                "      db: ref.fa",
                "      query: query.fa",
            ]
        ),
        encoding="utf-8",
    )

    config = load_project_config(config_path)

    assert config["outdir"] == "runs/project-001"
    assert config["continue_on_error"] is True
    assert config["report_title"] == "Demo"
    assert len(config["samples"]) == 2


def test_load_project_config_rejects_duplicate_sample_id(tmp_path: Path) -> None:
    config_path = tmp_path / "project.yml"
    config_path.write_text(
        "\n".join(
            [
                "samples:",
                "  - sample_id: dup",
                "    workflow: qc",
                "    input: reads.fastq",
                "  - sample_id: dup",
                "    workflow: search",
                "    db: ref.fa",
                "    query: query.fa",
            ]
        ),
        encoding="utf-8",
    )

    try:
        load_project_config(config_path)
    except ConfigError as exc:
        assert "Duplicate sample_id" in str(exc)
    else:
        raise AssertionError("expected ConfigError")


def test_load_project_config_rejects_mixed_single_and_paired_inputs(tmp_path: Path) -> None:
    config_path = tmp_path / "project.yml"
    config_path.write_text(
        "\n".join(
            [
                "samples:",
                "  - sample_id: mixed",
                "    workflow: qc",
                "    input: reads.fastq",
                "    input_r1: reads_1.fastq",
                "    input_r2: reads_2.fastq",
            ]
        ),
        encoding="utf-8",
    )

    try:
        load_project_config(config_path)
    except ConfigError as exc:
        assert "cannot mix" in str(exc)
    else:
        raise AssertionError("expected ConfigError")


def test_load_project_config_rejects_missing_align_ref(tmp_path: Path) -> None:
    config_path = tmp_path / "project.yml"
    config_path.write_text(
        "\n".join(
            [
                "samples:",
                "  - sample_id: no-ref",
                "    workflow: align",
                "    input: reads.fastq",
            ]
        ),
        encoding="utf-8",
    )

    try:
        load_project_config(config_path)
    except ConfigError as exc:
        assert "requires non-empty ref" in str(exc)
    else:
        raise AssertionError("expected ConfigError")


def test_run_project_batch_writes_summary_and_report(tmp_path: Path, monkeypatch) -> None:
    project_root = tmp_path / "runs" / "project-001"
    config_path = tmp_path / "project.yml"
    config_path.write_text("samples: []\n", encoding="utf-8")
    project_config = {
        "outdir": str(project_root),
        "continue_on_error": False,
        "report_title": "Demo Project",
        "samples": [
            {"sample_id": "sample-qc", "workflow": "qc", "input": "reads.fastq"},
            {"sample_id": "sample-search", "workflow": "search", "db": "ref.fa", "query": "query.fa"},
        ],
    }

    def fake_qc(_input: Path | None, **kwargs: object) -> bool:
        outdir = kwargs["outdir"]
        outdir.mkdir(parents=True, exist_ok=True)
        (outdir / "metadata.json").write_text(
            json.dumps(
                {
                    "workflow": "qc",
                    "status": "success",
                    "started_at": "2026-05-21T00:00:00Z",
                    "outputs": {"trimmed": str(outdir / "results" / "reads.trimmed.fastq")},
                }
            ),
            encoding="utf-8",
        )
        return True

    def fake_search(_db: Path, _query: Path, **kwargs: object) -> dict[str, object]:
        outdir = kwargs["outdir"]
        outdir.mkdir(parents=True, exist_ok=True)
        (outdir / "metadata.json").write_text(
            json.dumps(
                {
                    "workflow": "search",
                    "status": "success",
                    "started_at": "2026-05-21T00:01:00Z",
                    "outputs": {"tsv": str(outdir / "results" / "hits.tsv")},
                }
            ),
            encoding="utf-8",
        )
        return {"status": "success"}

    def fake_report(input_path: Path, output_path: Path, title: str | None = None) -> Path:
        output_path.write_text(f"{input_path.name}|{title}", encoding="utf-8")
        return output_path

    monkeypatch.setattr(project_batch, "run_qc_pipeline", fake_qc)
    monkeypatch.setattr(project_batch, "run_blast_search", fake_search)
    monkeypatch.setattr(project_batch, "generate_report", fake_report)

    result = project_batch.run_project_batch(
        config_path=config_path,
        project_config=project_config,
        quiet=True,
    )

    assert result["status"] == "success"
    assert result["planned_sample_count"] == 2
    assert result["sample_count"] == 2
    assert Path(result["summary"]).exists()
    assert Path(result["report"]).exists()
    assert Path(result["report"]).read_text(encoding="utf-8") == "project-001|Demo Project"

    summary = json.loads(Path(result["summary"]).read_text(encoding="utf-8"))
    assert summary["status"] == "success"
    assert summary["planned_sample_count"] == 2
    assert summary["report"] == result["report"]
    assert [sample["workflow"] for sample in summary["samples"]] == ["qc", "search"]


def test_run_project_batch_continue_on_error_keeps_running(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "project.yml"
    project_root = tmp_path / "runs" / "project-continue"
    project_config = {
        "outdir": str(project_root),
        "continue_on_error": True,
        "report_title": None,
        "samples": [
            {"sample_id": "sample-qc", "workflow": "qc", "input": "reads.fastq"},
            {"sample_id": "sample-align", "workflow": "align", "ref": "ref.fa", "input": "reads.fastq"},
        ],
    }
    calls: list[str] = []

    def fake_qc(_input: Path | None, **kwargs: object) -> bool:
        outdir = kwargs["outdir"]
        calls.append(outdir.name)
        outdir.mkdir(parents=True, exist_ok=True)
        (outdir / "metadata.json").write_text(
            json.dumps(
                {
                    "workflow": "qc",
                    "status": "failed",
                    "started_at": "2026-05-21T00:00:00Z",
                    "failure_summary": "qc failed",
                    "outputs": {},
                }
            ),
            encoding="utf-8",
        )
        return False

    def fake_align(_ref: Path, _input: Path | None, **kwargs: object) -> dict[str, object]:
        outdir = kwargs["outdir"]
        calls.append(outdir.name)
        outdir.mkdir(parents=True, exist_ok=True)
        (outdir / "metadata.json").write_text(
            json.dumps(
                {
                    "workflow": "align",
                    "status": "success",
                    "started_at": "2026-05-21T00:01:00Z",
                    "outputs": {"bam": str(outdir / "results" / "reads.sorted.bam")},
                }
            ),
            encoding="utf-8",
        )
        return {"mapped": 1}

    def fake_report(_input_path: Path, output_path: Path, title: str | None = None) -> Path:
        output_path.write_text(title or "", encoding="utf-8")
        return output_path

    monkeypatch.setattr(project_batch, "run_qc_pipeline", fake_qc)
    monkeypatch.setattr(project_batch, "run_alignment_pipeline", fake_align)
    monkeypatch.setattr(project_batch, "generate_report", fake_report)

    result = project_batch.run_project_batch(
        config_path=config_path,
        project_config=project_config,
        quiet=True,
    )

    assert result["status"] == "partial_failed"
    assert result["continue_on_error"] is True
    assert result["failed_count"] == 1
    assert result["success_count"] == 1
    assert len(result["samples"]) == 2
    assert calls == ["001-sample-qc-qc", "002-sample-align-align"]


def test_cmd_project_json_outputs_payload(tmp_path: Path, monkeypatch, capsys) -> None:
    config_path = tmp_path / "project.yml"
    config_path.write_text("samples: []\n", encoding="utf-8")
    project_root = tmp_path / "runs" / "project-001"

    monkeypatch.setattr(
        cli,
        "load_project_config",
        lambda _path: {
            "outdir": None,
            "continue_on_error": False,
            "report_title": None,
            "samples": [{"sample_id": "sample-qc", "workflow": "qc", "input": "reads.fastq"}],
        },
    )
    monkeypatch.setattr(
        cli,
        "run_project_batch",
        lambda **kwargs: {
            "status": "success",
            "project_root": str(project_root),
            "config": str(config_path),
            "summary": str(project_root / "project_summary.json"),
            "report": str(project_root / "project_report.html"),
            "sample_count": 1,
            "planned_sample_count": 1,
            "success_count": 1,
            "failed_count": 0,
            "continue_on_error": False,
            "samples": [],
        },
    )

    exit_code = cli.cmd_project(
        Namespace(
            quiet=False,
            json=True,
            config=str(config_path),
            outdir=None,
            continue_on_error=False,
        )
    )

    assert exit_code == cli.EXIT_SUCCESS
    payload = json.loads(capsys.readouterr().out)
    assert payload["project_root"] == str(project_root)
    assert payload["planned_sample_count"] == 1


def test_cmd_project_returns_runtime_error_when_failure_stops_batch(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "project.yml"
    config_path.write_text("samples: []\n", encoding="utf-8")

    monkeypatch.setattr(
        cli,
        "load_project_config",
        lambda _path: {
            "outdir": None,
            "continue_on_error": False,
            "report_title": None,
            "samples": [{"sample_id": "sample-qc", "workflow": "qc", "input": "reads.fastq"}],
        },
    )
    monkeypatch.setattr(
        cli,
        "run_project_batch",
        lambda **kwargs: {
            "status": "failed",
            "project_root": str(tmp_path / "runs" / "project-stop"),
            "config": str(config_path),
            "summary": str(tmp_path / "runs" / "project-stop" / "project_summary.json"),
            "report": "",
            "sample_count": 1,
            "planned_sample_count": 1,
            "success_count": 0,
            "failed_count": 1,
            "continue_on_error": False,
            "samples": [],
        },
    )

    exit_code = cli.cmd_project(
        Namespace(
            quiet=True,
            json=False,
            config=str(config_path),
            outdir=None,
            continue_on_error=False,
        )
    )

    assert exit_code == cli.EXIT_RUNTIME_ERROR

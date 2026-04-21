import json
from argparse import Namespace
from pathlib import Path

import bioflow.cli as cli
from bioflow.inspect import inspect_run, render_inspection_text


def test_inspect_run_collects_failed_steps_and_outputs(tmp_path: Path) -> None:
    run_root = tmp_path / "runs" / "qc-001"
    results_dir = run_root / "results"
    logs_dir = run_root / "logs"
    results_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    trimmed = results_dir / "reads.trimmed.fastq"
    trimmed.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")
    stderr_log = logs_dir / "qc.stderr.log"
    stderr_log.write_text("trim step failed", encoding="utf-8")
    stdout_log = logs_dir / "qc.stdout.log"
    stdout_log.write_text("partial output", encoding="utf-8")
    (run_root / "metadata.json").write_text(
        json.dumps(
            {
                "workflow": "qc",
                "version": "0.6.0",
                "status": "failed",
                "started_at": "2026-04-21T00:00:00+00:00",
                "completed_at": "2026-04-21T00:02:00+00:00",
                "command": "qc",
                "outputs": {"trimmed": str(trimmed)},
                "logs": {"stdout": str(stdout_log), "stderr": str(stderr_log)},
                "failure_summary": "trimmomatic: trim step failed",
                "steps": {
                    "fastqc_pre": {"status": "success"},
                    "trimmomatic": {"status": "failed", "error": "trim step failed"},
                },
            }
        ),
        encoding="utf-8",
    )

    payload = inspect_run(run_root)
    assert payload["status"] == "failed"
    assert payload["failed_steps"][0]["name"] == "trimmomatic"
    assert payload["critical_outputs"][0]["exists"] is True
    text = render_inspection_text(payload)
    assert "Failure Summary: trimmomatic: trim step failed" in text
    assert "stderr:" in text


def test_cmd_inspect_json_outputs_payload(tmp_path: Path, capsys) -> None:
    run_root = tmp_path / "runs" / "search-001"
    run_root.mkdir(parents=True, exist_ok=True)
    (run_root / "metadata.json").write_text(
        json.dumps(
            {
                "workflow": "search",
                "status": "success",
                "started_at": "2026-04-21T00:00:00+00:00",
                "outputs": {},
                "steps": {},
            }
        ),
        encoding="utf-8",
    )

    exit_code = cli.cmd_inspect(Namespace(input=str(run_root), json=True))

    assert exit_code == cli.EXIT_SUCCESS
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"
    assert payload["workflow"] == "search"
    assert payload["run_dir"] == str(run_root)


def test_cmd_inspect_rejects_missing_metadata(tmp_path: Path) -> None:
    run_root = tmp_path / "runs" / "empty"
    run_root.mkdir(parents=True, exist_ok=True)

    exit_code = cli.cmd_inspect(Namespace(input=str(run_root), json=False))

    assert exit_code == cli.EXIT_ARGUMENT_ERROR

import json
from pathlib import Path

from bioflow.execution import build_environment_fingerprint
from bioflow.run_layout import create_run_layout, step_resume_ready, write_metadata


def test_create_run_layout_and_metadata(tmp_path: Path) -> None:
    anchor = tmp_path / "reads.fastq"
    anchor.write_text("@r1\nACGT\n+\n!!!!\n", encoding="utf-8")

    layout = create_run_layout("qc", anchor)

    assert layout.root == tmp_path / "qc_run"
    assert layout.logs_dir.is_dir()
    assert layout.results_dir.is_dir()
    assert layout.tmp_dir.is_dir()

    write_metadata(
        layout,
        status="success",
        command="qc",
        parameters={
            "minlen": 36,
            "execution": {
                "profile": "workstation",
                "resources": {"threads": 4, "memory": "8G", "queue": "short", "time_limit": "02:00:00"},
                "source": "test",
            },
        },
        inputs={"input": str(anchor)},
        outputs={"root": str(layout.root)},
        started_at="2026-04-08T00:00:00+00:00",
        completed_at="2026-04-08T00:01:00+00:00",
        extra={"failure_summary": "", "input_details": {"input": {"path": str(anchor)}}},
    )

    metadata = json.loads(layout.metadata_path.read_text(encoding="utf-8"))
    assert metadata["workflow"] == "qc"
    assert metadata["status"] == "success"
    assert metadata["logs"]["stdout"].endswith("qc.stdout.log")
    assert metadata["runtime"]["bioflow_version"]
    assert metadata["input_details"]["input"]["path"] == str(anchor)
    assert metadata["execution"]["profile"] == "workstation"
    assert metadata["execution"]["resources"]["threads"] == 4


def test_step_resume_ready_rejects_execution_fingerprint_mismatch() -> None:
    metadata = {
        "execution": {
            "profile": "local",
            "backend": "system",
            "conda_env": None,
            "container_image": None,
            "resources": {"threads": 1, "memory": None, "queue": None, "time_limit": None},
        },
        "steps": {
            "blastn": {
                "status": "success",
                "outputs": {"tsv": "hits.tsv"},
                "environment_fingerprint": build_environment_fingerprint(
                    {
                        "profile": "local",
                        "backend": "system",
                        "conda_env": None,
                        "container_image": None,
                        "resources": {"threads": 1, "memory": None, "queue": None, "time_limit": None},
                    }
                ),
            }
        },
    }

    ok = step_resume_ready(
        metadata,
        "blastn",
        validator=lambda: True,
        required_outputs=("tsv",),
        current_execution={
            "profile": "workstation",
            "backend": "conda",
            "conda_env": "bioflow-env",
            "container_image": None,
            "resources": {"threads": 4, "memory": "8G", "queue": None, "time_limit": None},
        },
    )

    assert ok is False

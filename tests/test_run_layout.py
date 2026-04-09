from pathlib import Path

from bioflow.run_layout import create_run_layout, write_metadata


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
        parameters={"minlen": 36},
        inputs={"input": str(anchor)},
        outputs={"root": str(layout.root)},
        started_at="2026-04-08T00:00:00+00:00",
        completed_at="2026-04-08T00:01:00+00:00",
    )

    metadata = layout.metadata_path.read_text(encoding="utf-8")
    assert '"workflow": "qc"' in metadata
    assert '"status": "success"' in metadata

from bioflow.registry import get_workflow_manifest, list_workflow_manifests


def test_builtin_workflow_manifests_cover_existing_workflows() -> None:
    manifests = list_workflow_manifests()
    workflow_ids = [manifest.workflow_id for manifest in manifests]

    assert workflow_ids == ["align", "qc", "search"]
    assert get_workflow_manifest("qc").display_name == "Quality Control"
    assert "paired-end" in get_workflow_manifest("align").supported_inputs
    assert "hpc-slurm" in get_workflow_manifest("search").supported_profiles
    assert "summary" in get_workflow_manifest("search").key_outputs


def test_manifest_exposes_config_schema_fields() -> None:
    align = get_workflow_manifest("align")
    search = get_workflow_manifest("search")

    assert "input_r1" in align.allowed_keys
    assert align.project_fields["ref"].required_for_project is True
    assert search.fields["evalue"].kind == "number"
    assert search.fields["max_target_seqs"].positive is True

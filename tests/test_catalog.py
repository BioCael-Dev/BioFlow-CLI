"""Catalog consistency and compatibility across public entry points."""

import importlib
import json
import sys
from dataclasses import fields
from pathlib import Path

import pytest

from bioflow import cli, i18n, main
from bioflow.catalog import render_catalog
from bioflow.config import load_workflow_config
from bioflow.locales.en import STRINGS as EN
from bioflow.locales.zh import STRINGS as ZH
from bioflow.registry import list_workflow_manifests, workflow_catalog
from bioflow.report import RunInfo, _core_outputs

WORKFLOWS = ["align", "longread", "qc", "rnaseq", "search", "variant"]
MODULES = {"align": ("alignment", "ALIGN"), "qc": ("pipeline", "QC"),
           "longread": ("longread", "LONGREAD"), "rnaseq": ("rnaseq", "RNASEQ"),
           "search": ("search", "SEARCH"), "variant": ("variant", "VARIANT")}


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_catalog_matches_execution_schema_and_report(workflow):
    entry, = workflow_catalog(workflow)
    module, prefix = MODULES[workflow]
    assert tuple(entry["required_tools"]) == getattr(importlib.import_module(f"bioflow.{module}"), f"{prefix}_REQUIRED_TOOLS")
    assert entry["supported_backends"] == ["system", "conda", "container"]
    assert load_workflow_config(Path(__file__).parents[1] / entry["example_config"], workflow)
    assert entry["tui_menu_key"] in EN and entry["tui_menu_key"] in ZH
    data = {field.name: {} for field in fields(RunInfo)}
    data.update(workflow=workflow, outputs={key: key for key in entry["key_outputs"]})
    assert _core_outputs(RunInfo(**data)) == data["outputs"]


@pytest.mark.parametrize("argv", [["workflows", "--json"], ["--json", "workflows"]])
def test_catalog_json_is_parseable_and_complete(argv, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["bioflow", *argv])
    monkeypatch.setattr(cli, "init_language", lambda: "en")
    assert cli.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["schema_version"] == "bioflow.workflow-catalog.v1"
    assert [entry["workflow_id"] for entry in result["workflows"]] == WORKFLOWS
    assert "does not submit" in result["execution_notes"]


def test_catalog_single_and_invalid_workflow(monkeypatch, capsys):
    monkeypatch.setattr(cli, "init_language", lambda: "en")
    monkeypatch.setattr(sys, "argv", ["bioflow", "workflows", "rnaseq", "--json"])
    assert cli.main() == 0
    assert len(json.loads(capsys.readouterr().out)["workflows"]) == 1
    monkeypatch.setattr(sys, "argv", ["bioflow", "workflows", "missing"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 2


@pytest.mark.parametrize("manifest", list_workflow_manifests(), ids=lambda m: m.workflow_id)
def test_existing_cli_help_uses_manifest(manifest, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["bioflow", manifest.workflow_id, "--help"])
    with pytest.raises(SystemExit) as exc:
        cli.main()
    assert exc.value.code == 0
    assert "--config" in capsys.readouterr().out


def test_locale_parity_and_bilingual_catalog(monkeypatch):
    assert EN.keys() == ZH.keys()
    for lang, heading in [("en", "Workflow Catalog"), ("zh", "工作流目录")]:
        monkeypatch.setattr(i18n, "_current_lang", lang)
        text = render_catalog(workflow_catalog())
        assert heading in text
        for workflow in WORKFLOWS:
            assert f"bioflow {workflow} --config examples/{workflow}.yml" in text


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_tui_routes_each_existing_workflow(workflow, monkeypatch):
    calls = []
    handler_name = "align_menu" if workflow == "align" else f"{workflow}_menu"
    monkeypatch.setattr(main, handler_name, lambda: calls.append(workflow))
    answers = iter([i18n.t(f"menu_{workflow}"), i18n.t("menu_exit")])
    class Prompt:
        def __init__(self, value):
            self.value = value
        def ask(self):
            return self.value
    monkeypatch.setattr(main, "show_banner", lambda: None)
    monkeypatch.setattr(main.questionary, "select", lambda *a, **kw: Prompt(next(answers)))
    monkeypatch.setattr(main.questionary, "confirm", lambda *a, **kw: Prompt(True))
    with pytest.raises(SystemExit):
        main.main_menu()
    assert calls == [workflow]


def test_report_unknown_workflow_preserves_outputs():
    data = {field.name: {} for field in fields(RunInfo)}
    data.update(workflow="legacy-custom", outputs={"custom": "result.txt"})
    assert _core_outputs(RunInfo(**data)) == {"custom": "result.txt"}


def test_tui_catalog_returns_to_main_menu(monkeypatch, capsys):
    answers = iter([i18n.t("menu_catalog"), i18n.t("menu_exit")])
    class Prompt:
        def __init__(self, value):
            self.value = value
        def ask(self):
            return self.value
    monkeypatch.setattr(main, "show_banner", lambda: None)
    monkeypatch.setattr(main.questionary, "select", lambda *a, **kw: Prompt(next(answers)))
    monkeypatch.setattr(main.questionary, "confirm", lambda *a, **kw: Prompt(True))
    with pytest.raises(SystemExit):
        main.main_menu()
    text = capsys.readouterr().out
    assert "Workflow Catalog" in text
    assert "bioflow rnaseq" in text


def test_catalog_queries_do_not_launch_tools(monkeypatch, capsys):
    def forbidden(*args, **kwargs):
        raise AssertionError("Catalog must not start external tools")
    monkeypatch.setattr(cli.subprocess, "run", forbidden)
    monkeypatch.setattr(cli, "init_language", lambda: "en")
    monkeypatch.setattr(sys, "argv", ["bioflow", "workflows", "qc", "--json"])
    assert cli.main() == 0
    assert json.loads(capsys.readouterr().out)["workflows"][0]["required_tools"] == ["fastqc", "trimmomatic"]

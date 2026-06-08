from __future__ import annotations

import pytest

import bioflow.preflight as preflight


def test_preflight_check_system_success(monkeypatch) -> None:
    monkeypatch.setattr(preflight, "check_tool", lambda _name: True)

    assert preflight.preflight_check(["fastqc"], cli_mode=True) is True


def test_preflight_check_conda_backend_requires_runtime(monkeypatch) -> None:
    monkeypatch.setattr(preflight, "check_tool", lambda _name: True)
    monkeypatch.setattr(preflight, "_check_conda", lambda: False)

    with pytest.raises(preflight.PreflightError) as exc_info:
        preflight.preflight_check(["fastqc"], backend="conda", conda_env="bioflow-env", cli_mode=True)

    exc = exc_info.value
    assert exc.backend == "conda"
    assert exc.reason == "missing_runtime"
    assert exc.missing_runtime == "conda"
    assert exc.conda_env == "bioflow-env"


def test_preflight_check_conda_backend_requires_env(monkeypatch) -> None:
    monkeypatch.setattr(preflight, "check_tool", lambda _name: True)
    monkeypatch.setattr(preflight, "_check_conda", lambda: True)
    monkeypatch.setattr(preflight, "_check_conda_env", lambda _name: False)

    with pytest.raises(preflight.PreflightError) as exc_info:
        preflight.preflight_check(["fastqc"], backend="conda", conda_env="bioflow-env", cli_mode=True)

    exc = exc_info.value
    assert exc.backend == "conda"
    assert exc.reason == "missing_conda_env"
    assert exc.conda_env == "bioflow-env"


def test_preflight_check_container_backend_requires_runtime(monkeypatch) -> None:
    monkeypatch.setattr(preflight, "check_tool", lambda _name: True)
    monkeypatch.setattr(preflight, "_check_container_runtime", lambda _runtime: False)

    with pytest.raises(preflight.PreflightError) as exc_info:
        preflight.preflight_check(
            ["fastqc"],
            backend="container",
            container_image="ghcr.io/demo/bioflow:latest",
            cli_mode=True,
        )

    exc = exc_info.value
    assert exc.backend == "container"
    assert exc.reason == "missing_runtime"
    assert exc.missing_runtime == "docker/apptainer"
    assert exc.container_image == "ghcr.io/demo/bioflow:latest"


def test_preflight_check_container_backend_requires_image(monkeypatch) -> None:
    monkeypatch.setattr(preflight, "check_tool", lambda _name: True)
    monkeypatch.setattr(preflight, "_check_container_runtime", lambda _runtime: True)

    with pytest.raises(preflight.PreflightError) as exc_info:
        preflight.preflight_check(["fastqc"], backend="container", container_image=None, cli_mode=True)

    exc = exc_info.value
    assert exc.backend == "container"
    assert exc.reason == "missing_container_image"


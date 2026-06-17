"""BioFlow-CLI execution wrappers for system, conda, and container backends."""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class ResolvedCommand:
    """A raw tool command plus the backend-specific command actually executed."""

    raw_command: tuple[str, ...]
    resolved_command: tuple[str, ...]
    backend: str
    environment_fingerprint: str
    runtime: str | None = None


def build_execution_context(params: Mapping[str, Any], *, source: str) -> dict[str, Any]:
    """Construct the normalized execution metadata payload."""
    return {
        "profile": str(params.get("profile") or "local"),
        "backend": str(params.get("backend") or "system"),
        "conda_env": str(params["conda_env"]) if params.get("conda_env") is not None else None,
        "container_image": str(params["container_image"]) if params.get("container_image") is not None else None,
        "resources": {
            "threads": int(params["threads"]) if params.get("threads") is not None else None,
            "memory": str(params["memory"]) if params.get("memory") is not None else None,
            "queue": str(params["queue"]) if params.get("queue") is not None else None,
            "time_limit": str(params["time_limit"]) if params.get("time_limit") is not None else None,
        },
        "source": source,
    }


def build_environment_fingerprint(execution: Mapping[str, Any] | None) -> str:
    """Return a stable fingerprint for resume safety checks."""
    payload = execution or {}
    normalized = {
        "profile": payload.get("profile") or "local",
        "backend": payload.get("backend") or "system",
        "conda_env": payload.get("conda_env"),
        "container_image": payload.get("container_image"),
        "resources": {
            "threads": (payload.get("resources") or {}).get("threads") if isinstance(payload.get("resources"), dict) else None,
            "memory": (payload.get("resources") or {}).get("memory") if isinstance(payload.get("resources"), dict) else None,
            "queue": (payload.get("resources") or {}).get("queue") if isinstance(payload.get("resources"), dict) else None,
            "time_limit": (payload.get("resources") or {}).get("time_limit") if isinstance(payload.get("resources"), dict) else None,
        },
    }
    encoded = json.dumps(normalized, sort_keys=True, ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def stringify_command(command: Sequence[str]) -> str:
    """Render a command list as a shell-safe string."""
    return shlex.join([str(part) for part in command])


def summarize_commands(
    commands: Sequence[ResolvedCommand],
    *,
    separator: str,
) -> tuple[str, str]:
    """Render raw and resolved command bundles for metadata."""
    raw = separator.join(stringify_command(command.raw_command) for command in commands)
    resolved = separator.join(stringify_command(command.resolved_command) for command in commands)
    return raw, resolved


def choose_container_runtime() -> str | None:
    """Pick the first available supported container runtime."""
    if shutil.which("docker"):
        return "docker"
    if shutil.which("apptainer"):
        return "apptainer"
    return None


def resolve_command(
    command: Sequence[str],
    execution: Mapping[str, Any] | None,
    *,
    path_hints: Sequence[str | Path] = (),
    workdir: Path | None = None,
) -> ResolvedCommand:
    """Resolve a raw command into the backend-specific command to execute."""
    execution_payload = execution or {
        "profile": "local",
        "backend": "system",
        "conda_env": None,
        "container_image": None,
        "resources": {},
    }
    backend = str(execution_payload.get("backend") or "system")
    raw = tuple(str(part) for part in command)
    fingerprint = build_environment_fingerprint(execution_payload)

    if backend == "conda":
        conda_env = str(execution_payload.get("conda_env") or "")
        resolved = (
            "conda",
            "run",
            "--no-capture-output",
            "-n",
            conda_env,
            *raw,
        )
        return ResolvedCommand(raw, resolved, backend, fingerprint, runtime="conda")

    if backend == "container":
        image = str(execution_payload.get("container_image") or "")
        runtime = choose_container_runtime() or "docker"
        if runtime == "docker":
            resolved = tuple(
                _build_docker_command(
                    raw,
                    image=image,
                    path_hints=path_hints,
                    workdir=workdir,
                )
            )
        else:
            resolved = tuple(
                _build_apptainer_command(
                    raw,
                    image=image,
                    path_hints=path_hints,
                    workdir=workdir,
                )
            )
        return ResolvedCommand(raw, resolved, backend, fingerprint, runtime=runtime)

    return ResolvedCommand(raw, raw, backend, fingerprint, runtime=None)


def resolve_pipeline_commands(
    commands: Sequence[Sequence[str]],
    execution: Mapping[str, Any] | None,
    *,
    path_hints: Sequence[str | Path] = (),
    workdir: Path | None = None,
) -> list[ResolvedCommand]:
    """Resolve a command pipeline under the same execution backend."""
    return [
        resolve_command(
            command,
            execution,
            path_hints=path_hints,
            workdir=workdir,
        )
        for command in commands
    ]


def _build_docker_command(
    command: Sequence[str],
    *,
    image: str,
    path_hints: Sequence[str | Path],
    workdir: Path | None,
) -> list[str]:
    """Wrap a command in docker run using host paths mounted in place."""
    cmd: list[str] = ["docker", "run", "--rm"]
    uid = getattr(os, "getuid", None)
    gid = getattr(os, "getgid", None)
    if callable(uid) and callable(gid):
        cmd.extend(["--user", f"{uid()}:{gid()}"])

    mounts = _resolve_mount_targets(path_hints, workdir=workdir)
    for mount in mounts:
        cmd.extend(["-v", f"{mount}:{mount}"])

    resolved_workdir = workdir.resolve() if workdir is not None else Path.cwd().resolve()
    if resolved_workdir is not None:
        cmd.extend(["-w", str(resolved_workdir)])

    cmd.append(image)
    cmd.extend(command)
    return cmd


def _build_apptainer_command(
    command: Sequence[str],
    *,
    image: str,
    path_hints: Sequence[str | Path],
    workdir: Path | None,
) -> list[str]:
    """Wrap a command in apptainer exec."""
    cmd: list[str] = ["apptainer", "exec"]
    for mount in _resolve_mount_targets(path_hints, workdir=workdir):
        cmd.extend(["--bind", f"{mount}:{mount}"])
    resolved_workdir = workdir.resolve() if workdir is not None else Path.cwd().resolve()
    if resolved_workdir is not None:
        cmd.extend(["--pwd", str(resolved_workdir)])
    cmd.append(image)
    cmd.extend(command)
    return cmd


def _resolve_mount_targets(
    path_hints: Sequence[str | Path],
    *,
    workdir: Path | None,
) -> list[Path]:
    """Choose host directories that must be visible inside a container."""
    targets: set[Path] = set()
    base_dir = workdir.resolve() if workdir is not None else Path.cwd().resolve()
    targets.add(base_dir)
    for hint in path_hints:
        candidate = Path(hint)
        if not candidate.is_absolute():
            candidate = (base_dir / candidate).resolve(strict=False)
        else:
            candidate = candidate.resolve(strict=False)

        if candidate.exists() and candidate.is_dir():
            targets.add(candidate)
        else:
            targets.add(candidate.parent)

    if workdir is not None:
        targets.add(workdir.resolve())

    return sorted(targets)

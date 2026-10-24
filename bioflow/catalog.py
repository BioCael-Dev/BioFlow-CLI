"""Localized presentation of the shared workflow catalog."""

from __future__ import annotations

from typing import Any

from bioflow.i18n import t


def render_catalog(catalog: list[dict[str, Any]]) -> str:
    lines = [t("catalog_title"), t("catalog_scope")]
    for item in catalog:
        lines.append(f"\n{item['workflow_id']} — {t(item['tui_menu_key'])}")
        for key in ("supported_inputs", "key_outputs", "required_tools", "supported_profiles", "supported_backends"):
            lines.append(f"  {t('catalog_' + key)}: {', '.join(item[key])}")
        lines.append(f"  CLI: {item['cli_command']} --help")
        lines.append(f"  {t('catalog_example')}: {item['cli_command']} --config {item['example_config']}")
        lines.append(f"  TUI: {t(item['tui_menu_key'])}; {t('catalog_project')}")
    return "\n".join(lines)

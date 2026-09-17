#!/usr/bin/env python3
"""BioFlow-CLI 主入口 — TUI 交互界面。"""

from __future__ import annotations

import logging
import sys

import questionary
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from bioflow import __version__
from bioflow.alignment import align_menu
from bioflow.bio_tasks import seq_menu
from bioflow.env_manager import env_menu
from bioflow.pipeline import qc_menu
from bioflow.i18n import init_language, load_config, set_language, t
from bioflow.longread import longread_menu
from bioflow.report import report_menu
from bioflow.rnaseq import rnaseq_menu
from bioflow.search import search_menu
from bioflow.variant import variant_menu

console = Console()

LANG_OPTIONS = {"English": "en", "中文": "zh"}


def _setup_logging() -> None:
    """配置日志，避免重复 handler。"""
    root = logging.getLogger("bioflow")
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        root.addHandler(handler)
        root.setLevel(logging.WARNING)


def select_language() -> None:
    """语言选择交互。"""
    try:
        choice = questionary.select(
            t("lang_prompt"), choices=list(LANG_OPTIONS.keys())
        ).ask()
    except KeyboardInterrupt:
        return
    if choice:
        set_language(LANG_OPTIONS[choice])
        console.print(t("lang_saved"), style="bold green")


def first_run_setup() -> None:
    """首次运行时引导用户选择语言。"""
    cfg = load_config()
    if "language" not in cfg:
        console.print(
            Panel(
                "Welcome to BioFlow-CLI / 欢迎使用 BioFlow-CLI",
                style="bold cyan",
            )
        )
        try:
            choice = questionary.select(
                "Please select your language / 请选择语言：",
                choices=list(LANG_OPTIONS.keys()),
            ).ask()
        except KeyboardInterrupt:
            return
        if choice:
            set_language(LANG_OPTIONS[choice])


def show_banner() -> None:
    """显示应用横幅。"""
    banner = Text()
    banner.append("BioFlow-CLI", style="bold cyan")
    banner.append(f"  v{__version__}", style="dim")
    console.print(Panel(banner, subtitle=t("app_title"), style="blue"))


def main_menu() -> None:
    """主菜单循环。"""
    while True:
        show_banner()
        choices = [
            t("menu_env"),
            t("menu_seq"),
            t("menu_qc"),
            t("menu_align"),
            t("menu_longread"),
            t("menu_variant"),
            t("menu_search"),
            t("menu_rnaseq"),
            t("menu_report"),
            t("menu_settings"),
            t("menu_exit"),
        ]

        try:
            answer = questionary.select(t("menu_prompt"), choices=choices).ask()
        except KeyboardInterrupt:
            continue  # Ctrl+C 回到菜单

        if answer is None:
            continue  # 其他取消情况，回到菜单

        if answer == t("menu_exit"):
            try:
                if questionary.confirm(t("confirm_exit"), default=True).ask():
                    console.print(t("goodbye"), style="bold cyan")
                    sys.exit(0)
            except KeyboardInterrupt:
                continue
        elif answer == t("menu_env"):
            env_menu()
        elif answer == t("menu_seq"):
            seq_menu()
        elif answer == t("menu_qc"):
            qc_menu()
        elif answer == t("menu_align"):
            align_menu()
        elif answer == t("menu_longread"):
            longread_menu()
        elif answer == t("menu_variant"):
            variant_menu()
        elif answer == t("menu_search"):
            search_menu()
        elif answer == t("menu_rnaseq"):
            rnaseq_menu()
        elif answer == t("menu_report"):
            report_menu()
        elif answer == t("menu_settings"):
            select_language()


def main() -> None:
    """程序入口 — 根据参数决定 TUI 或 CLI 模式。"""
    # 检测是否有命令行参数（除了脚本名）
    if len(sys.argv) > 1:
        # CLI 模式：导入并执行 CLI 入口
        from bioflow.cli import main as cli_main
        sys.exit(cli_main())

    # TUI 模式：交互式界面
    _setup_logging()
    try:
        init_language()
        first_run_setup()
        init_language()  # 重新加载（首次设置后刷新）
        main_menu()
    except KeyboardInterrupt:
        console.print("\n" + t("goodbye"), style="bold cyan")
        sys.exit(0)
    except Exception as exc:
        console.print(t("error_unexpected", err=str(exc)), style="bold red")
        sys.exit(1)


if __name__ == "__main__":
    main()

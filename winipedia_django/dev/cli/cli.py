"""This module contains the CLI entrypoint."""

import typer
from winipedia_utils.utils.modules.function import get_all_functions_from_module

from winipedia_django.dev.cli import subcommands

app = typer.Typer()

sub_cmds = get_all_functions_from_module(subcommands)

for sub_cmd in sub_cmds:
    app.command()(sub_cmd)


def main() -> None:
    """Entry point for the CLI."""
    app()

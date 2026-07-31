"""
Omega Entry Point

Usage:
    python -m omega start --config omega/config.yaml
    python -m omega status
    python -m omega checkpoint
"""

import asyncio
import argparse
import json
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.json import JSON

console = Console()


def _load_runtime(config_path: str):
    from omega.core.runtime import OmegaRuntime
    return OmegaRuntime(config_path)


@click.group()
def cli():
    """Omega Federation Core — Sovereign Runtime Foundation."""
    pass


@cli.command()
@click.option("--config", "-c", default="omega/config.yaml", help="Path to config file")
def start(config):
    """Start the Omega runtime daemon."""
    runtime = _load_runtime(config)
    
    console.print(Panel(
        f"[bold cyan]Ω Omega Runtime[/bold cyan]\n"
        f"Starting sovereign orchestration daemon...\n"
        f"Config: [yellow]{config}[/yellow]",
        border_style="cyan"
    ))
    
    try:
        asyncio.run(runtime.start())
    except KeyboardInterrupt:
        console.print("[red]Interrupted.[/red]")


@cli.command()
@click.option("--config", "-c", default="omega/config.yaml")
def status(config):
    """Show runtime status."""
    console.print(Panel(
        "[yellow]Status check requires running runtime.[/yellow]\n"
        "Use: omega status --node http://localhost:7777",
        border_style="yellow"
    ))


@cli.command()
@click.option("--config", "-c", default="omega/config.yaml")
def checkpoint(config):
    """Trigger a manual checkpoint."""
    runtime = _load_runtime(config)
    asyncio.run(runtime.initialize())
    
    if runtime.ledger and runtime.checkpoint:
        path = runtime.checkpoint.save(ledger_sequence=runtime.ledger.get_last_sequence())
        console.print(Panel(
            f"[green]Checkpoint created[/green]\n{path}",
            border_style="green"
        ))
    else:
        console.print("[red]Runtime not fully initialized.[/red]")


def main():
    cli()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import argparse
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

sys.path.insert(0, str(_PROJECT_ROOT))

from src.blue_green.state import StateManager
from src.blue_green.switch import BlueGreenSwitcher
from src.blue_green.validator import BlueGreenValidator

console = Console()


def _build_db_config() -> dict:
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
    }


def _fmt(value) -> str:
    return str(value) if value is not None else "[dim]—[/dim]"


def cmd_status(_args) -> None:
    sm = StateManager()
    state = sm.read()

    console.print("\n[bold magenta]Blue-Green Database — Status[/bold magenta]\n")

    active = state.get("active")
    staging = state.get("staging")

    if not active and not staging:
        console.print("[yellow]Nenhum estado registrado. Execute o ETL para começar.[/yellow]")
        return

    t = Table(show_header=True, header_style="bold cyan")
    t.add_column("Campo")
    t.add_column("Ativo (receita_federal)", style="green")
    t.add_column("Staging (receita_federal_staging)", style="yellow")

    fields = [
        ("source_month", "Mês dos dados"),
        ("downloaded_at", "Download em"),
        ("processed_at", "Processado em"),
        ("switched_at", "Switch em"),
    ]
    for key, label in fields:
        t.add_row(
            label,
            _fmt((active or {}).get(key)),
            _fmt((staging or {}).get(key)),
        )

    console.print(t)
    if state.get("last_switch"):
        console.print(f"\n[dim]Último switch: {state['last_switch']}[/dim]")


async def _cmd_validate_async(_args) -> int:
    config = _build_db_config()
    validator = BlueGreenValidator(config)
    console.print("\n[bold]Validando receita_federal_staging...[/bold]\n")
    result = await validator.validate()

    if result.is_valid:
        console.print(f"[bold green]✅ {result.summary}[/bold green]")
        return 0

    console.print(f"[bold red]❌ {result.summary}[/bold red]")

    if result.missing_tables:
        console.print(f"[red]  Tabelas ausentes: {', '.join(result.missing_tables)}[/red]")
    if result.empty_tables:
        console.print(f"[yellow]  Tabelas vazias: {', '.join(result.empty_tables)}[/yellow]")
    if result.missing_indexes:
        console.print(f"[red]  Índices ausentes: {', '.join(result.missing_indexes)}[/red]")
    return 1


def cmd_validate(args) -> None:
    sys.exit(asyncio.run(_cmd_validate_async(args)))


async def _cmd_switch_async(args) -> int:
    config = _build_db_config()
    sm = StateManager()
    switcher = BlueGreenSwitcher(config, sm)

    console.print("\n[bold]Executando blue-green switch...[/bold]\n")
    result = await switcher.switch(force=args.force)

    if result.success:
        console.print(f"[bold green]✅ {result.message}[/bold green]")
        if result.source_month:
            console.print(f"[green]   Mês dos dados: {result.source_month}[/green]")
        return 0

    console.print(f"[bold red]❌ {result.message}[/bold red]")
    return 1


def cmd_switch(args) -> None:
    sys.exit(asyncio.run(_cmd_switch_async(args)))


async def _cmd_cleanup_async(_args) -> None:
    config = _build_db_config()
    sm = StateManager()
    switcher = BlueGreenSwitcher(config, sm)
    await switcher.cleanup_old()


def cmd_cleanup(args) -> None:
    asyncio.run(_cmd_cleanup_async(args))


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="blue_green",
        description="Gerenciamento blue-green de banco de dados CNPJ",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="Exibe estado atual dos slots ativo e staging")

    sub.add_parser("validate", help="Valida receita_federal_staging antes do switch")

    switch_p = sub.add_parser("switch", help="Valida e promove staging para ativo")
    switch_p.add_argument(
        "--force", action="store_true", help="Pula validação e força o switch"
    )

    sub.add_parser("cleanup", help="Dropa receita_federal_old se existir")

    args = parser.parse_args()
    {
        "status": cmd_status,
        "validate": cmd_validate,
        "switch": cmd_switch,
        "cleanup": cmd_cleanup,
    }[args.command](args)


if __name__ == "__main__":
    main()

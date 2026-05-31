#!/usr/bin/env python3
uv """
Script de deploy em produção — ciclo completo blue-green:
  1. Executa ETL na staging
  2. Valida staging
  3. Confirma switch
  4. Executa switch (rename + drop old)

Uso:
  uv run run_prod.py                  # baixa versão mais recente
  uv run run_prod.py 01-2025          # baixa versão específica
  uv run run_prod.py --skip-etl       # pula ETL, só valida e faz switch
  uv run run_prod.py --auto-switch    # não pede confirmação antes do switch
"""

import argparse
import asyncio
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table

_ROOT = Path(__file__).resolve().parent
load_dotenv(_ROOT / ".env")

sys.path.insert(0, str(_ROOT))

from src.blue_green.state import StateManager
from src.blue_green.switch import BlueGreenSwitcher
from src.blue_green.validator import BlueGreenValidator

console = Console()


def _step(n: int, total: int, title: str) -> None:
    console.print(Rule(f"[bold cyan]Passo {n}/{total}: {title}[/bold cyan]"))


def _ok(msg: str) -> None:
    console.print(f"[bold green]✅ {msg}[/bold green]")


def _fail(msg: str) -> None:
    console.print(f"[bold red]❌ {msg}[/bold red]")


def _warn(msg: str) -> None:
    console.print(f"[bold yellow]⚠  {msg}[/bold yellow]")


def _build_db_config() -> dict:
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deploy blue-green em produção",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "date",
        nargs="?",
        help="Mês no formato MM-AAAA. Se omitido, usa --last automaticamente.",
    )
    parser.add_argument(
        "--skip-etl",
        action="store_true",
        help="Pula o ETL — assume que staging já foi carregada",
    )
    parser.add_argument(
        "--auto-switch",
        action="store_true",
        help="Executa o switch sem pedir confirmação interativa",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Executa ETL e validação mas não faz o switch",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Passo 1 — ETL
# ---------------------------------------------------------------------------

def run_etl(args: argparse.Namespace) -> bool:
    etl_script = _ROOT / "src" / "etl" / "ETL_dados_publicos_empresas.py"

    cmd = ["uv", "run", str(etl_script), "--db-target", "receita_federal_staging"]
    if args.date:
        cmd.append(args.date)
    else:
        cmd.append("--last")

    console.print(f"[dim]Comando: {' '.join(cmd)}[/dim]\n")

    result = subprocess.run(cmd, cwd=_ROOT)
    return result.returncode == 0


# ---------------------------------------------------------------------------
# Passo 2 — Validação
# ---------------------------------------------------------------------------

async def run_validation() -> tuple[bool, object]:
    config = _build_db_config()
    validator = BlueGreenValidator(config)
    result = await validator.validate("receita_federal_staging")
    return result.is_valid, result


def print_validation_report(result) -> None:
    t = Table(show_header=True, header_style="bold")
    t.add_column("Verificação")
    t.add_column("Status")
    t.add_column("Detalhes")

    tables_ok = not result.missing_tables and not result.empty_tables
    indexes_ok = not result.missing_indexes

    t.add_row(
        "Tabelas existentes",
        "[green]OK[/green]" if not result.missing_tables else "[red]FALHOU[/red]",
        ", ".join(result.missing_tables) if result.missing_tables else "todas presentes",
    )
    t.add_row(
        "Tabelas com dados",
        "[green]OK[/green]" if not result.empty_tables else "[red]FALHOU[/red]",
        ", ".join(result.empty_tables) if result.empty_tables else "todas com registros",
    )
    t.add_row(
        "Índices",
        "[green]OK[/green]" if indexes_ok else "[red]FALHOU[/red]",
        ", ".join(result.missing_indexes) if result.missing_indexes else "todos presentes",
    )

    console.print(t)


# ---------------------------------------------------------------------------
# Passo 3 — Switch
# ---------------------------------------------------------------------------

async def run_switch() -> bool:
    config = _build_db_config()
    sm = StateManager()
    switcher = BlueGreenSwitcher(config, sm)
    result = await switcher.switch(force=True)  # validação já foi feita no passo 2
    return result.success, result


# ---------------------------------------------------------------------------
# Resumo final
# ---------------------------------------------------------------------------

def print_summary(state: StateManager, start: datetime, success: bool) -> None:
    active = state.get_active() or {}
    elapsed = (datetime.now(timezone.utc) - start).seconds

    status = "[bold green]SUCESSO[/bold green]" if success else "[bold red]FALHOU[/bold red]"

    content = "\n".join([
        f"Status:        {status}",
        f"Tempo total:   {elapsed // 60}m {elapsed % 60}s",
        f"Banco ativo:   receita_federal",
        f"Mês dos dados: {active.get('source_month', '—')}",
        f"Switch em:     {active.get('switched_at', '—')}",
    ])
    console.print(Panel(content, title="Resumo do Deploy", border_style="green" if success else "red"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main_async(args: argparse.Namespace) -> int:
    start = datetime.now(timezone.utc)
    total_steps = 2 if args.skip_etl else 3
    if args.dry_run:
        total_steps -= 1

    console.print(Panel(
        "[bold]Deploy blue-green — Receita Federal CNPJ[/bold]\n"
        f"Modo: {'dry-run' if args.dry_run else 'completo'} | "
        f"ETL: {'pulado' if args.skip_etl else 'ativo'} | "
        f"Switch: {'automático' if args.auto_switch else 'confirmar'}",
        border_style="magenta",
    ))

    step = 0

    # ------------------------------------------------------------------
    # Passo ETL
    # ------------------------------------------------------------------
    if not args.skip_etl:
        step += 1
        _step(step, total_steps, "ETL — carregando dados na staging")
        ok = run_etl(args)
        if not ok:
            _fail("ETL falhou — abortando deploy")
            return 1
        _ok("ETL concluído com sucesso")
    else:
        _warn("ETL pulado (--skip-etl) — usando staging existente")

    # ------------------------------------------------------------------
    # Passo Validação
    # ------------------------------------------------------------------
    step += 1
    _step(step, total_steps, "Validação da staging")

    is_valid, result = await run_validation()
    print_validation_report(result)

    if not is_valid:
        _fail(f"Staging inválida: {result.summary}")
        _warn("Corrija os problemas antes de fazer o switch")
        return 1

    _ok("Staging validada — pronta para switch")

    if args.dry_run:
        _warn("--dry-run ativo — switch não será executado")
        console.print("\n[dim]Para fazer o switch, rode sem --dry-run[/dim]")
        return 0

    # ------------------------------------------------------------------
    # Passo Switch
    # ------------------------------------------------------------------
    step += 1
    _step(step, total_steps, "Switch blue-green")

    if not args.auto_switch:
        sm = StateManager()
        staging = sm.get_staging() or {}
        console.print(
            f"\n[yellow]Prestes a promover staging para produção:[/yellow]\n"
            f"  Mês dos dados: [bold]{staging.get('source_month', '?')}[/bold]\n"
            f"  Processado em: [bold]{staging.get('processed_at', '?')}[/bold]\n"
        )
        resposta = console.input("[bold]Confirma o switch? (s/N): [/bold]").strip().lower()
        if resposta not in ("s", "sim", "y", "yes"):
            _warn("Switch cancelado pelo usuário")
            return 0

    ok, switch_result = await run_switch()

    if not ok:
        _fail(f"Switch falhou: {switch_result.message}")
        return 1

    _ok(switch_result.message)

    sm = StateManager()
    print_summary(sm, start, success=True)
    return 0


def main() -> None:
    args = parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()

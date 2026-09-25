#!/usr/bin/env python3
"""
Deploy em produção via dump/restore (com downtime):

  1. Para a API (opcional, PROD_API_SERVICE no .env)
  2. Dropa bancos existentes (libera ~35 GB antes do restore — padrão)
  3. Cria receita_federal vazio e restaura o dump
  4. Valida tabelas/índices
  5. Atualiza blue_green_state.json
  6. Sobe a API

Uso (no servidor de produção):
  # 1) Liberar disco ANTES do rsync do dump (só drop, sem restore):
  ./scripts/drop_prod_databases.sh --yes

  # 2) Enviar dump e restaurar:
  uv run deploy_dump_prod.py dumps/receita_federal_09-2026.dump --source-month 09-2026 --yes

  # Rollback via rename (dobra uso de disco — só se couber):
  uv run deploy_dump_prod.py dumps/arquivo.dump --source-month 09-2026 --rename-for-rollback
"""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
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
from src.blue_green.validator import BlueGreenValidator

console = Console()

_ACTIVE_DB = "receita_federal"
_OLD_DB = "receita_federal_old"
_STAGING_DB = "receita_federal_staging"


def _build_db_config(database: str = "postgres") -> dict:
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": database,
    }


def _pg_env() -> dict:
    env = os.environ.copy()
    env["PGPASSWORD"] = os.getenv("DB_PASSWORD", "")
    return env


def _run_psql(sql: str, database: str = "postgres") -> None:
    cfg = _build_db_config(database)
    cmd = [
        "psql",
        "-h",
        cfg["host"],
        "-p",
        str(cfg["port"]),
        "-U",
        cfg["user"],
        "-d",
        database,
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    result = subprocess.run(cmd, env=_pg_env(), capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())


def _api_service() -> str:
    return os.getenv("PROD_API_SERVICE", "").strip()


def _service_ctl(action: str) -> None:
    service = _api_service()
    if not service:
        console.print(f"[dim]PROD_API_SERVICE não definido — pulando {action} da API[/dim]")
        return
    console.print(f"[yellow]{'Parando' if action == 'stop' else 'Iniciando'} {service}...[/yellow]")
    result = subprocess.run(
        ["systemctl", action, service],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"systemctl {action} {service} falhou: {result.stderr.strip()}"
        )


def _terminate_connections(*db_names: str) -> None:
    names = ", ".join(f"'{n}'" for n in db_names)
    _run_psql(
        f"""
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname IN ({names}) AND pid <> pg_backend_pid();
        """
    )


def _database_exists(name: str) -> bool:
    cfg = _build_db_config("postgres")
    result = subprocess.run(
        [
            "psql",
            "-h",
            cfg["host"],
            "-p",
            str(cfg["port"]),
            "-U",
            cfg["user"],
            "-d",
            "postgres",
            "-tAc",
            f"SELECT 1 FROM pg_database WHERE datname = '{name}'",
        ],
        env=_pg_env(),
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() == "1"


def _drop_database(name: str) -> None:
    if not _database_exists(name):
        return
    _terminate_connections(name)
    _run_psql(f'DROP DATABASE IF EXISTS "{name}";')


def _show_disk(label: str) -> None:
    result = subprocess.run(["df", "-h", "/"], capture_output=True, text=True, check=True)
    console.print(f"[dim]{label}[/dim]\n{result.stdout.strip()}")


def _drop_all_cnpj_databases() -> None:
    """Remove todos os bancos CNPJ — libera disco antes do restore."""
    _terminate_connections(_ACTIVE_DB, _OLD_DB, _STAGING_DB)
    for name in (_ACTIVE_DB, _OLD_DB, _STAGING_DB):
        if _database_exists(name):
            _drop_database(name)
            console.print(f"[green]✅ {name} removido[/green]")
        else:
            console.print(f"[dim]{name} não existia[/dim]")


def _rename_to_old() -> None:
    if not _database_exists(_ACTIVE_DB):
        console.print(f"[dim]{_ACTIVE_DB} não existe — restore em banco novo[/dim]")
        return
    _terminate_connections(_ACTIVE_DB, _OLD_DB)
    _drop_database(_OLD_DB)
    _run_psql(f'ALTER DATABASE "{_ACTIVE_DB}" RENAME TO "{_OLD_DB}";')
    console.print(f"[green]✅ {_ACTIVE_DB} renomeado para {_OLD_DB} (rollback disponível)[/green]")


def _create_active_database() -> None:
    if _database_exists(_ACTIVE_DB):
        raise RuntimeError(f"Banco {_ACTIVE_DB} ainda existe — abortando")
    _run_psql(f'CREATE DATABASE "{_ACTIVE_DB}";')
    console.print(f"[green]✅ Banco {_ACTIVE_DB} criado[/green]")


def _restore_dump(dump_path: Path, jobs: int) -> None:
    cfg = _build_db_config(_ACTIVE_DB)
    cmd = [
        "pg_restore",
        "-h",
        cfg["host"],
        "-p",
        str(cfg["port"]),
        "-U",
        cfg["user"],
        "-d",
        _ACTIVE_DB,
        "--no-owner",
        "--no-acl",
        "--verbose",
        f"--jobs={jobs}",
        str(dump_path),
    ]
    console.print(f"[dim]{' '.join(cmd)}[/dim]\n")
    console.print("[yellow]⏳ Restaurando dump (pode levar 30–90 min)...[/yellow]")
    result = subprocess.run(cmd, env=_pg_env())
    # pg_restore retorna 1 para warnings não fatais (objeto já existe, etc.)
    if result.returncode not in (0, 1):
        raise RuntimeError(f"pg_restore falhou com código {result.returncode}")


async def _validate_active() -> tuple[bool, object]:
    config = _build_db_config()
    validator = BlueGreenValidator(config)
    result = await validator.validate(_ACTIVE_DB)
    return result.is_valid, result


def _print_validation(result) -> None:
    t = Table(show_header=True, header_style="bold")
    t.add_column("Verificação")
    t.add_column("Status")
    t.add_column("Detalhes")
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
        "[green]OK[/green]" if not result.missing_indexes else "[red]FALHOU[/red]",
        ", ".join(result.missing_indexes) if result.missing_indexes else "todos presentes",
    )
    console.print(t)


def _rollback_from_old() -> None:
    if not _database_exists(_OLD_DB):
        console.print("[red]❌ Rollback impossível: receita_federal_old não existe[/red]")
        return
    _drop_database(_ACTIVE_DB)
    _run_psql(f'ALTER DATABASE "{_OLD_DB}" RENAME TO "{_ACTIVE_DB}";')
    console.print("[green]✅ Rollback concluído — receita_federal_old restaurado como ativo[/green]")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deploy em produção via dump/restore",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "dump",
        nargs="?",
        type=Path,
        help="Caminho do arquivo .dump (formato custom pg_dump; omita com --prepare-only)",
    )
    parser.add_argument(
        "--source-month",
        metavar="MM-AAAA",
        help="Mês dos dados (ex: 09-2026) — gravado em blue_green_state.json",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=int(os.getenv("DUMP_RESTORE_JOBS", "4")),
        help="Paralelismo do pg_restore (padrão: 4)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Não pedir confirmação",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Só valida o dump e exibe o plano, sem alterar produção",
    )
    parser.add_argument(
        "--skip-api-stop",
        action="store_true",
        help="Não para/inicia a API via systemctl",
    )
    parser.add_argument(
        "--rename-for-rollback",
        action="store_true",
        help=(
            "Renomeia receita_federal → receita_federal_old em vez de dropar "
            "(dobra uso de disco durante o restore — só use se houver espaço)"
        ),
    )
    parser.add_argument(
        "--prepare-only",
        action="store_true",
        help=(
            "Só dropa os bancos CNPJ para liberar disco — não restaura. "
            "Use antes de enviar o .dump via rsync."
        ),
    )
    return parser.parse_args()


def _preflight(dump_path: Path) -> None:
    if not dump_path.is_file():
        raise FileNotFoundError(f"Dump não encontrado: {dump_path}")
    for bin_name in ("psql", "pg_restore"):
        if not shutil.which(bin_name):
            raise RuntimeError(f"Comando '{bin_name}' não encontrado no PATH")
    if not os.getenv("DB_PASSWORD"):
        raise RuntimeError("DB_PASSWORD não definido no .env")


def _prepare_disk(args: argparse.Namespace) -> None:
    """Dropa bancos CNPJ para liberar espaço (sem restore)."""
    if not args.yes:
        resp = console.input(
            "\n[bold red]Confirma DROP de receita_federal (+ old/staging)? (s/N): [/bold red]"
        ).strip().lower()
        if resp not in ("s", "sim", "y", "yes"):
            console.print("[yellow]Cancelado[/yellow]")
            sys.exit(0)

    _show_disk("Disco ANTES")
    api_stopped = False
    try:
        if not args.skip_api_stop:
            _service_ctl("stop")
            api_stopped = True
        _drop_all_cnpj_databases()
    finally:
        if api_stopped and not args.skip_api_stop:
            _service_ctl("start")
    _show_disk("Disco DEPOIS")
    console.print(
        "[green]✅ Disco liberado. Envie o dump e rode deploy_dump_prod.py[/green]"
    )


def main() -> None:
    args = parse_args()
    dump_path = args.dump if args.dump.is_absolute() else _ROOT / args.dump

    if args.prepare_only:
        try:
            if not os.getenv("DB_PASSWORD"):
                raise RuntimeError("DB_PASSWORD não definido no .env")
            if not shutil.which("psql"):
                raise RuntimeError("psql não encontrado no PATH")
        except RuntimeError as e:
            console.print(f"[red]❌ {e}[/red]")
            sys.exit(1)
        console.print(
            Panel(
                "[bold]Prepare disk — drop bancos CNPJ[/bold]\n"
                "Libera ~35 GB no disco raiz antes do rsync do dump.",
                border_style="yellow",
            )
        )
        _prepare_disk(args)
        sys.exit(0)

    if not args.dump:
        console.print("[red]❌ Informe o caminho do .dump ou use --prepare-only[/red]")
        sys.exit(1)

    if not args.source_month:
        console.print("[red]❌ --source-month é obrigatório para restore (ex: 09-2026)[/red]")
        sys.exit(1)

    try:
        _preflight(dump_path)
    except (FileNotFoundError, RuntimeError) as e:
        console.print(f"[red]❌ {e}[/red]")
        sys.exit(1)

    size_gb = dump_path.stat().st_size / (1024**3)
    api = _api_service() or "(não configurado)"
    prep_mode = "drop (libera disco)" if not args.rename_for_rollback else "rename (rollback)"

    console.print(
        Panel(
            "[bold]Deploy via dump/restore — Receita Federal CNPJ[/bold]\n"
            f"Dump:         {dump_path}\n"
            f"Tamanho:      {size_gb:.2f} GB\n"
            f"Mês:          {args.source_month}\n"
            f"API service:  {api}\n"
            f"Preparo DB:   {prep_mode}\n"
            f"Modo:         {'dry-run' if args.dry_run else 'execução real'}",
            border_style="magenta",
        )
    )

    if args.dry_run:
        console.print("[yellow]--dry-run: nenhuma alteração será feita[/yellow]")
        sys.exit(0)

    if not args.yes:
        resp = console.input("\n[bold]Confirma deploy (downtime)? (s/N): [/bold]").strip().lower()
        if resp not in ("s", "sim", "y", "yes"):
            console.print("[yellow]Cancelado[/yellow]")
            sys.exit(0)

    start = datetime.now(timezone.utc)
    api_stopped = False
    used_rename = args.rename_for_rollback

    try:
        console.print(Rule("[bold cyan]1/5 — Parar API[/bold cyan]"))
        if not args.skip_api_stop:
            _service_ctl("stop")
            api_stopped = True

        console.print(Rule("[bold cyan]2/5 — Liberar disco (drop bancos)[/bold cyan]"))
        _show_disk("Disco ANTES")
        if used_rename:
            _terminate_connections(_ACTIVE_DB, _OLD_DB, _STAGING_DB)
            _rename_to_old()
        else:
            _drop_all_cnpj_databases()
        _show_disk("Disco após preparo")

        console.print(Rule("[bold cyan]3/5 — Criar banco e restaurar dump[/bold cyan]"))
        _create_active_database()
        _restore_dump(dump_path, args.jobs)

        console.print(Rule("[bold cyan]4/5 — Validar[/bold cyan]"))
        is_valid, result = asyncio.run(_validate_active())
        _print_validation(result)
        if not is_valid:
            raise RuntimeError(f"Banco inválido após restore: {result.summary}")

        console.print(Rule("[bold cyan]5/5 — Finalizar[/bold cyan]"))
        StateManager().promote_from_dump(args.source_month)
        console.print("[green]✅ blue_green_state.json atualizado[/green]")

        if used_rename:
            _drop_database(_OLD_DB)
            console.print(f"[green]✅ {_OLD_DB} removido[/green]")

        elapsed = int((datetime.now(timezone.utc) - start).total_seconds())
        console.print(
            Panel(
                f"[bold green]Deploy concluído[/bold green]\n"
                f"Tempo: {elapsed // 60}m {elapsed % 60}s\n"
                f"Banco: {_ACTIVE_DB}\n"
                f"Mês:   {args.source_month}",
                border_style="green",
            )
        )

    except Exception as e:
        console.print(f"[red]❌ Falha no deploy: {e}[/red]")
        if used_rename:
            console.print("[yellow]Tentando rollback para receita_federal_old...[/yellow]")
            try:
                _rollback_from_old()
            except Exception as rb:
                console.print(f"[red]Rollback também falhou: {rb}[/red]")
        else:
            console.print(
                "[red]Sem rollback — banco anterior foi dropado. "
                "Restaure o dump ou restaure backup externo.[/red]"
            )
        sys.exit(1)

    finally:
        if api_stopped and not args.skip_api_stop:
            try:
                _service_ctl("start")
            except Exception as e:
                console.print(
                    f"[red]❌ Banco restaurado, mas falha ao subir API: {e}[/red]\n"
                    f"[yellow]Suba manualmente: systemctl start {_api_service()}[/yellow]"
                )
                sys.exit(1)


if __name__ == "__main__":
    main()

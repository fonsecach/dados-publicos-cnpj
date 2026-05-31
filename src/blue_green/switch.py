import asyncpg
from dataclasses import dataclass

from src.blue_green.state import StateManager
from src.blue_green.validator import BlueGreenValidator

_ACTIVE_DB = "receita_federal"
_STAGING_DB = "receita_federal_staging"
_OLD_DB = "receita_federal_old"


@dataclass
class SwitchResult:
    success: bool
    message: str
    active_db: str = _ACTIVE_DB
    source_month: str | None = None


class BlueGreenSwitcher:
    def __init__(self, db_config: dict, state_manager: StateManager):
        self._config = db_config
        self._state = state_manager

    async def _admin_conn(self):
        return await asyncpg.connect(**self._config, database="postgres", timeout=30)

    async def _db_exists(self, conn, name: str) -> bool:
        return bool(
            await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", name)
        )

    async def _terminate_connections(self, conn, db_name: str) -> None:
        await conn.execute(
            """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = $1 AND pid <> pg_backend_pid()
            """,
            db_name,
        )

    async def _drop_db(self, conn, db_name: str) -> None:
        await self._terminate_connections(conn, db_name)
        await conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')

    async def switch(self, force: bool = False) -> SwitchResult:
        if not force:
            validator = BlueGreenValidator(self._config)
            result = await validator.validate(_STAGING_DB)
            if not result.is_valid:
                return SwitchResult(success=False, message=result.summary)

        admin = await self._admin_conn()
        try:
            if not await self._db_exists(admin, _STAGING_DB):
                return SwitchResult(
                    success=False,
                    message=f"Staging '{_STAGING_DB}' não encontrada — execute o ETL primeiro",
                )

            # Invariante: nunca mais de 2 bancos — dropa old se existir
            if await self._db_exists(admin, _OLD_DB):
                await self._drop_db(admin, _OLD_DB)

            # Encerra conexões no banco ativo antes do rename
            await self._terminate_connections(admin, _ACTIVE_DB)

            await admin.execute(f'ALTER DATABASE "{_ACTIVE_DB}" RENAME TO "{_OLD_DB}"')
            await admin.execute(f'ALTER DATABASE "{_STAGING_DB}" RENAME TO "{_ACTIVE_DB}"')

            # Atualiza estado antes do drop do old para não perder metadados se drop falhar
            self._state.promote_staging()
            staging_info = (self._state.get_active() or {})

            # Drop imediato do old — mantém invariante dos 2 bancos
            await self._drop_db(admin, _OLD_DB)

            return SwitchResult(
                success=True,
                message=f"Switch concluído — '{_ACTIVE_DB}' agora contém os dados novos",
                source_month=staging_info.get("source_month"),
            )
        except Exception as e:
            return SwitchResult(success=False, message=f"Erro durante o switch: {e}")
        finally:
            await admin.close()

    async def cleanup_old(self) -> None:
        admin = await self._admin_conn()
        try:
            if await self._db_exists(admin, _OLD_DB):
                await self._drop_db(admin, _OLD_DB)
                print(f"[blue-green] '{_OLD_DB}' dropado com sucesso")
            else:
                print(f"[blue-green] '{_OLD_DB}' não existe — nada a fazer")
        finally:
            await admin.close()

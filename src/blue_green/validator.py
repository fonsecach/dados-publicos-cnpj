import asyncpg
from dataclasses import dataclass, field

from src.blue_green.constants import EXPECTED_INDEXES, EXPECTED_TABLES


@dataclass
class ValidationResult:
    is_valid: bool
    missing_tables: list = field(default_factory=list)
    empty_tables: list = field(default_factory=list)
    missing_indexes: list = field(default_factory=list)

    @property
    def summary(self) -> str:
        if self.is_valid:
            return "VALID — staging pronta para switch"
        issues = []
        if self.missing_tables:
            issues.append(f"Tabelas ausentes: {', '.join(self.missing_tables)}")
        if self.empty_tables:
            issues.append(f"Tabelas vazias: {', '.join(self.empty_tables)}")
        if self.missing_indexes:
            issues.append(f"Índices ausentes: {', '.join(self.missing_indexes)}")
        return "INVALID — " + "; ".join(issues)


class BlueGreenValidator:
    def __init__(self, db_config: dict):
        self._config = db_config

    async def validate(self, db_name: str = "receita_federal_staging") -> ValidationResult:
        try:
            conn = await asyncpg.connect(**self._config, database=db_name, timeout=30)
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                missing_tables=EXPECTED_TABLES[:],
                missing_indexes=EXPECTED_INDEXES[:],
            )

        try:
            missing_tables = []
            empty_tables = []
            for table in EXPECTED_TABLES:
                exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
                    table,
                )
                if not exists:
                    missing_tables.append(table)
                    continue
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                if count == 0:
                    empty_tables.append(table)

            missing_indexes = []
            for index in EXPECTED_INDEXES:
                exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname = $1)",
                    index,
                )
                if not exists:
                    missing_indexes.append(index)

            is_valid = not missing_tables and not empty_tables and not missing_indexes
            return ValidationResult(
                is_valid=is_valid,
                missing_tables=missing_tables,
                empty_tables=empty_tables,
                missing_indexes=missing_indexes,
            )
        finally:
            await conn.close()

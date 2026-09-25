#!/usr/bin/env bash
# Libera espaço em disco dropando os bancos CNPJ em produção.
# Use ANTES de enviar o .dump (rsync) quando o disco raiz estiver cheio.
#
# Uso (no servidor Hetzner):
#   ./scripts/drop_prod_databases.sh           # pede confirmação
#   ./scripts/drop_prod_databases.sh --yes     # sem confirmação
#
# Remove: receita_federal, receita_federal_old, receita_federal_staging

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

YES=false
for arg in "$@"; do
  [[ "$arg" == "--yes" || "$arg" == "-y" ]] && YES=true
done

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"

if [[ -z "${DB_PASSWORD:-}" ]]; then
  echo "❌ DB_PASSWORD não definido no .env"
  exit 1
fi

export PGPASSWORD="$DB_PASSWORD"

DATABASES=("receita_federal" "receita_federal_old" "receita_federal_staging")

echo "=== Espaço ANTES ==="
df -h /

echo ""
echo "Bancos a remover:"
for db in "${DATABASES[@]}"; do
  exists="$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -tAc \
    "SELECT pg_size_pretty(pg_database_size('$db')) FROM pg_database WHERE datname='$db'" 2>/dev/null || true)"
  if [[ -n "$exists" ]]; then
    echo "  • $db ($exists)"
  else
    echo "  • $db (não existe)"
  fi
done

if [[ "$YES" != true ]]; then
  echo ""
  read -r -p "Confirma DROP de todos os bancos acima? (s/N): " resp
  if [[ ! "$resp" =~ ^[sSyY] ]]; then
    echo "Cancelado."
    exit 0
  fi
fi

echo ""
echo "Encerrando conexões..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -v ON_ERROR_STOP=1 <<SQL
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname IN ('receita_federal', 'receita_federal_old', 'receita_federal_staging')
  AND pid <> pg_backend_pid();
SQL

for db in "${DATABASES[@]}"; do
  echo "DROP DATABASE IF EXISTS \"$db\" ..."
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -v ON_ERROR_STOP=1 \
    -c "DROP DATABASE IF EXISTS \"$db\";"
done

echo ""
echo "=== Espaço DEPOIS ==="
df -h /

echo ""
echo "✅ Bancos removidos. Próximos passos:"
echo "  1. rsync do dump para ~/repo/dados-publicos-cnpj/dumps/"
echo "  2. uv run deploy_dump_prod.py dumps/arquivo.dump --source-month MM-AAAA --yes"

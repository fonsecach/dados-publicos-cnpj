#!/usr/bin/env bash
# Gera dump custom (-Fc) do banco local para enviar à produção.
#
# Uso:
#   ./scripts/dump_local.sh                    # usa DB_NAME do .env
#   ./scripts/dump_local.sh 09-2026            # inclui mês no nome do arquivo
#   ./scripts/dump_local.sh 09-2026 ./dumps    # diretório de saída customizado
#
# Requer: pg_dump, .env com credenciais locais (docker: DB_PORT=5436)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

SOURCE_MONTH="${1:-}"
OUT_DIR="${2:-${DUMP_OUTPUT_DIR:-./dumps}}"
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5436}"
DB_NAME="${DB_NAME:-receita_federal}"
DB_USER="${DB_USER:-postgres}"

if ! command -v pg_dump >/dev/null 2>&1; then
  echo "❌ pg_dump não encontrado. Instale postgresql-client."
  exit 1
fi

if [[ -z "${DB_PASSWORD:-}" ]]; then
  echo "❌ DB_PASSWORD não definido no .env"
  exit 1
fi

mkdir -p "$OUT_DIR"

TS="$(date +%Y%m%d_%H%M%S)"
if [[ -n "$SOURCE_MONTH" ]]; then
  OUT_FILE="${OUT_DIR}/receita_federal_${SOURCE_MONTH}_${TS}.dump"
else
  OUT_FILE="${OUT_DIR}/receita_federal_${TS}.dump"
fi

echo "Origem:  ${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
echo "Saída:   ${OUT_FILE}"
echo ""

export PGPASSWORD="$DB_PASSWORD"

if ! pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" >/dev/null 2>&1; then
  echo "❌ PostgreSQL indisponível em ${DB_HOST}:${DB_PORT}"
  echo "   Suba o banco: docker compose up -d"
  exit 1
fi

echo "⏳ Gerando dump (pode levar vários minutos)..."
pg_dump \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$DB_USER" \
  -d "$DB_NAME" \
  --format=custom \
  --compress=9 \
  --no-owner \
  --no-acl \
  --verbose \
  -f "$OUT_FILE"

SIZE="$(du -h "$OUT_FILE" | cut -f1)"
echo ""
echo "✅ Dump gerado: ${OUT_FILE} (${SIZE})"
echo ""
echo "Enviar para produção:"
echo "  rsync -avP \"${OUT_FILE}\" hetzner:~/repo/dados-publicos-cnpj/dumps/"
echo ""
echo "Restaurar em produção:"
echo "  uv run deploy_dump_prod.py \"dumps/$(basename "$OUT_FILE")\" --source-month ${SOURCE_MONTH:-MM-AAAA}"

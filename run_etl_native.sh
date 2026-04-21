#!/usr/bin/env bash
# Script para PostgreSQL nativo (VM/bare-metal) — sem Docker
# Uso:          ./run_etl_native.sh
# Forçar reset: ./run_etl_native.sh --force
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION_FILE="${SCRIPT_DIR}/.last_etl_version"
FORCE=false

for arg in "$@"; do
    case "${arg}" in
        --force) FORCE=true ;;
        *) echo "Argumento desconhecido: ${arg}"; exit 1 ;;
    esac
done

# ─────────────────────────────────────────────
# Carregar .env
# ─────────────────────────────────────────────
if [ ! -f "${SCRIPT_DIR}/.env" ]; then
    echo "ERRO: .env não encontrado."
    echo "      cp .env.example .env  →  edite DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME"
    exit 1
fi

set -a
source "${SCRIPT_DIR}/.env"
set +a

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-receita_federal}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:?DB_PASSWORD nao configurado no .env}"

# Executar psql como superusuário (root → su, outro usuário → sudo)
pg_admin() {
    if [ "$(id -u)" = "0" ]; then
        su -c "psql -v ON_ERROR_STOP=1 $*" - postgres
    else
        sudo -u postgres psql -v ON_ERROR_STOP=1 "$@"
    fi
}

pg_admin_pipe() {
    if [ "$(id -u)" = "0" ]; then
        su - postgres -c "psql -v ON_ERROR_STOP=1"
    else
        sudo -u postgres psql -v ON_ERROR_STOP=1
    fi
}

# ─────────────────────────────────────────────
# 1. Verificar PostgreSQL
# ─────────────────────────────────────────────
echo "=== Verificando PostgreSQL ==="

if ! command -v psql &>/dev/null; then
    echo "ERRO: psql não encontrado. Instale o PostgreSQL:"
    echo "  Debian/Ubuntu : sudo apt install postgresql"
    echo "  RHEL/Fedora   : sudo dnf install postgresql-server"
    exit 1
fi

if ! pg_admin -c '\q' &>/dev/null 2>&1; then
    echo "ERRO: não foi possível conectar ao PostgreSQL como superusuário."
    echo "  Verifique : sudo systemctl status postgresql"
    echo "  Inicie    : sudo systemctl start postgresql"
    exit 1
fi

PG_VERSION=$(pg_admin -tAc "SHOW server_version" | tr -d '[:space:]')
echo "PostgreSQL ${PG_VERSION} acessível."

if [ "${DB_PORT}" != "5432" ]; then
    echo ""
    echo "AVISO: DB_PORT=${DB_PORT} no .env — porta padrão do PostgreSQL nativo é 5432."
    echo "       Atualize DB_PORT=5432 no .env se o PostgreSQL estiver na porta padrão."
fi

# ─────────────────────────────────────────────
# 2. Detectar versão mais recente
# ─────────────────────────────────────────────
echo ""
echo "=== Detectando versão disponível na Receita Federal ==="

LATEST_VERSION=$(uv run python3 - <<'PYEOF'
import httpx, re, ssl, sys, datetime

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def fallback_version():
    now = datetime.datetime.now()
    if now.month == 1:
        return f"{now.year - 1}-12"
    return f"{now.year}-{now.month - 1:02d}"

try:
    with httpx.Client(verify=ctx, timeout=30, follow_redirects=True,
                      headers={"User-Agent": "Mozilla/5.0"}) as c:
        r = c.get("https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9")
        html = r.text
        for pattern in [
            r'data-file="(\d{4}-\d{2})"',
            r'"basename":"(\d{4}-\d{2})"',
            r'"filename":"(\d{4}-\d{2})"',
            r'href="[^"]*dir=%2F(\d{4}-\d{2})[^"]*"',
        ]:
            matches = re.findall(pattern, html)
            if matches:
                matches.sort(reverse=True)
                print(matches[0])
                sys.exit(0)
    fb = fallback_version()
    print(f"AVISO: versao nao encontrada na pagina, usando fallback: {fb}", file=sys.stderr)
    print(fb)
except Exception as e:
    fb = fallback_version()
    print(f"AVISO: erro ao acessar pagina ({e}), usando fallback: {fb}", file=sys.stderr)
    print(fb)
PYEOF
)

echo "Versão disponível: ${LATEST_VERSION}"

# ─────────────────────────────────────────────
# 3. Comparar com versão anterior
# ─────────────────────────────────────────────
LAST_VERSION=""
if [ -f "${VERSION_FILE}" ]; then
    LAST_VERSION=$(cat "${VERSION_FILE}")
fi

echo "Versão anterior:   ${LAST_VERSION:-'nenhuma'}"

DB_EXISTS=$(pg_admin -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" 2>/dev/null | tr -d '[:space:]' || echo "")

if [ "${FORCE}" = "false" ] && [ "${LATEST_VERSION}" = "${LAST_VERSION}" ] && [ "${DB_EXISTS}" = "1" ]; then
    echo ""
    echo "Banco já está na versão mais recente (${LATEST_VERSION}). Nada a fazer."
    echo "Para reprocessar: ./run_etl_native.sh --force"
    exit 0
fi

# ─────────────────────────────────────────────
# 4. Criar/atualizar usuário do banco
# ─────────────────────────────────────────────
echo ""
echo "=== Configurando usuário '${DB_USER}' ==="

if [ "${DB_USER}" = "postgres" ]; then
    pg_admin -c "ALTER ROLE postgres WITH PASSWORD '${DB_PASSWORD}';"
    echo "Senha do superusuário postgres atualizada."
else
    pg_admin_pipe <<SQL
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${DB_USER}') THEN
        CREATE ROLE "${DB_USER}" WITH LOGIN PASSWORD '${DB_PASSWORD}';
        RAISE NOTICE 'Usuario ${DB_USER} criado.';
    ELSE
        ALTER ROLE "${DB_USER}" WITH PASSWORD '${DB_PASSWORD}';
        RAISE NOTICE 'Senha do usuario ${DB_USER} atualizada.';
    END IF;
END
\$\$;
SQL
fi

# ─────────────────────────────────────────────
# 5. Recriar banco de dados
# ─────────────────────────────────────────────
echo ""
if [ -n "${LAST_VERSION}" ] && [ "${LATEST_VERSION}" != "${LAST_VERSION}" ]; then
    echo "=== Nova versão detectada (${LAST_VERSION} → ${LATEST_VERSION}) — Recriando banco ==="
else
    echo "=== Criando banco '${DB_NAME}' ==="
fi

pg_admin_pipe <<SQL
-- Encerrar conexões ativas antes de dropar
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '${DB_NAME}' AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS "${DB_NAME}";
CREATE DATABASE "${DB_NAME}" OWNER "${DB_USER}" ENCODING 'UTF8';
GRANT ALL PRIVILEGES ON DATABASE "${DB_NAME}" TO "${DB_USER}";
SQL

echo "Banco '${DB_NAME}' pronto."

# ─────────────────────────────────────────────
# 6. Executar ETL
# ─────────────────────────────────────────────
echo ""
echo "=== Iniciando ETL — versão ${LATEST_VERSION} ==="
echo ""
uv run "${SCRIPT_DIR}/src/etl/ETL_dados_publicos_empresas.py" --last

# ─────────────────────────────────────────────
# 7. Registrar versão processada
# ─────────────────────────────────────────────
echo "${LATEST_VERSION}" > "${VERSION_FILE}"
echo ""
echo "Concluído! Versão ${LATEST_VERSION} processada e registrada."

#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION_FILE="${SCRIPT_DIR}/.last_etl_version"

# Carregar .env
if [ -f "${SCRIPT_DIR}/.env" ]; then
    set -a
    # shellcheck source=.env
    source "${SCRIPT_DIR}/.env"
    set +a
fi

DB_USER="${DB_USER:-postgres}"
DB_NAME="${DB_NAME:-receita_federal}"

# ─────────────────────────────────────────────
# 1. Detectar versão mais recente na Receita Federal
# ─────────────────────────────────────────────
echo "Detectando versão mais recente disponível na Receita Federal..."

LATEST_VERSION=$(uv run python3 - <<'PYEOF'
import httpx, re, ssl, sys, datetime

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def fallback_version():
    now = datetime.datetime.now()
    # Mês anterior é o mais provável de estar disponível
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
    print(f"AVISO: falha ao acessar pagina ({e}), usando fallback: {fb}", file=sys.stderr)
    print(fb)
PYEOF
)

echo "Versão mais recente: ${LATEST_VERSION}"

# ─────────────────────────────────────────────
# 2. Comparar com versão anteriormente processada
# ─────────────────────────────────────────────
LAST_VERSION=""
if [ -f "${VERSION_FILE}" ]; then
    LAST_VERSION=$(cat "${VERSION_FILE}")
fi

echo "Versão anterior: ${LAST_VERSION:-'nenhuma'}"

CONTAINER_RUNNING=$(docker ps --filter "name=receita_postgres" --filter "status=running" -q)

if [ "${LATEST_VERSION}" = "${LAST_VERSION}" ] && [ -n "${CONTAINER_RUNNING}" ]; then
    echo "Banco já está na versão mais recente (${LATEST_VERSION}) e em execução."
    echo "Use 'docker compose down -v && ./run_etl.sh' para forçar reprocessamento."
    exit 0
fi

# ─────────────────────────────────────────────
# 3. Derrubar banco e volume existentes
# ─────────────────────────────────────────────
if [ -n "${LAST_VERSION}" ] && [ "${LATEST_VERSION}" != "${LAST_VERSION}" ]; then
    echo "Nova versão detectada (${LAST_VERSION} → ${LATEST_VERSION}). Removendo banco antigo..."
else
    echo "Preparando banco de dados..."
fi
# Sempre derruba container + volume para garantir senha e estado limpos
docker compose down -v 2>/dev/null || true

# ─────────────────────────────────────────────
# 4. Iniciar banco de dados
# ─────────────────────────────────────────────
echo "Iniciando banco de dados PostgreSQL..."
docker compose up -d

echo "Aguardando banco de dados ficar pronto..."
MAX_RETRIES=20
count=0
until docker compose exec -T postgres pg_isready -U "${DB_USER}" -d "${DB_NAME}" > /dev/null 2>&1; do
    count=$((count + 1))
    if [ "${count}" -ge "${MAX_RETRIES}" ]; then
        echo "Banco não ficou pronto após ${MAX_RETRIES} tentativas. Logs:"
        docker compose logs postgres
        exit 1
    fi
    echo "  Tentativa ${count}/${MAX_RETRIES}..."
    sleep 5
done
echo "Banco pronto!"

# ─────────────────────────────────────────────
# 5. Executar ETL
# ─────────────────────────────────────────────
echo ""
echo "Iniciando ETL - versão ${LATEST_VERSION}..."
echo ""
uv run "${SCRIPT_DIR}/src/etl/ETL_dados_publicos_empresas.py" --last

# ─────────────────────────────────────────────
# 6. Registrar versão processada com sucesso
# ─────────────────────────────────────────────
echo "${LATEST_VERSION}" > "${VERSION_FILE}"
echo ""
echo "Concluído! Versão ${LATEST_VERSION} processada e registrada."

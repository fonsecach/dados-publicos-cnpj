# Atualização da base Receita (blue-green) com disco separado

Guia para atualizar a base CNPJ em produção mantendo **o banco principal no disco
principal** e **tudo transitório (download, temp, staging) num volume separado**.

## Contexto de disco (servidor CX33)

- Disco raiz: ~76 GB — hospeda o SO e o banco principal `receita_federal` (~35 GB).
- Volume anexado (ex.: 50 GB, `/dev/sdb`) — hospeda o transitório do ETL.

O deploy é **blue-green**: o ETL carrega em `receita_federal_staging`, valida, e o switch
faz `RENAME` (prod→old, staging→prod) + `DROP old`. Como `RENAME` **não move dados**, a
staging precisa nascer no volume e, após o switch, o banco promovido é trazido de volta ao
disco principal (`--relocate-to-main`).

## Setup único do volume

```bash
lsblk                                   # identifique o volume (ex.: /dev/sdb)
sudo mkdir -p /mnt/pg_staging
sudo mount /dev/sdb /mnt/pg_staging     # se ainda não montado
echo "UUID=$(sudo blkid -s UUID -o value /dev/sdb) /mnt/pg_staging ext4 defaults,nofail 0 2" \
  | sudo tee -a /etc/fstab
sudo chown postgres:postgres /mnt/pg_staging

# tablespace do Postgres no volume (para a staging e para os temporários)
sudo -u postgres psql -c "CREATE TABLESPACE staging LOCATION '/mnt/pg_staging';"

# diretório de trabalho do ETL no volume (dono = usuário que roda o ETL)
sudo mkdir -p /mnt/pg_staging/etl/{downloads,tmp}
sudo chown -R $USER:$USER /mnt/pg_staging/etl
```

## Configuração

### `.env`
```
DOWNLOAD_DIR=/mnt/pg_staging/etl/downloads   # ZIPs da Receita -> volume
TEMP_DIR=/mnt/pg_staging/etl/tmp             # extração/processamento -> volume
STAGING_TABLESPACE=staging                   # staging criada no volume
```

### Temporários do Postgres no volume
Sorts, hash joins e tabelas temporárias (inclui builds de índice) vão para o volume:
```bash
sudo -u postgres psql -c "ALTER SYSTEM SET temp_tablespaces = 'staging';"
sudo -u postgres psql -c "SELECT pg_reload_conf();"
sudo -u postgres psql -c "SHOW temp_tablespaces;"   # confere: staging
```

## Como o código usa isso

- **ETL** (`src/etl/ETL_dados_publicos_empresas.py`): ao criar um banco cujo nome termina
  em `_staging`, aplica `TABLESPACE $STAGING_TABLESPACE` se a variável estiver definida.
  Assim a staging nasce no volume; o banco principal continua no `pg_default` (raiz).
- **run_prod.py `--relocate-to-main`**: após o switch, executa
  `ALTER DATABASE receita_federal SET TABLESPACE pg_default` (encerrando conexões antes),
  trazendo o banco promovido para o disco principal e liberando o volume.

## Fluxo de atualização mensal

```bash
# 0. confira espaço nos dois discos
df -h / /mnt/pg_staging

# 1. dry-run: baixa + carrega staging (no volume) + valida, SEM trocar
uv run run_prod.py 06-2026 --dry-run

# 2. execução real: switch + relocação para o disco principal
uv run run_prod.py 06-2026 --relocate-to-main
```

Formato do mês: **`MM-AAAA`** (ex.: `06-2026`). Flags úteis:
- `--last` — versão mais recente da Receita.
- `--skip-download` — reusa arquivos já baixados (sem rede).
- `--skip-etl` — só valida e promove uma staging já existente.
- `--auto-switch` — não pede confirmação.

## Uso de disco em cada fase

| Fase | Raiz (~76 GB) | Volume (50 GB) |
|---|---|---|
| Normal | `receita_federal` ~35 + SO ~17 | vazio (temp sob demanda) |
| Durante o ETL | main ~35 + SO ~17 | staging ~35 + downloads ~6 + temp |
| Pós-switch (antes do relocate) | ~17 (old dropado) | `receita_federal` ~35 |
| Após `--relocate-to-main` | main ~35 + SO ~17 | livre |

## Cuidados

- **Conexões da API caem no switch** (`pg_terminate_backend`): a API fica alguns segundos
  em `receita_db: degraded` e reconecta sozinha. Para zero erro, pare a API no switch.
- **Relocação leva alguns minutos** (copia ~35 GB) e encerra conexões ao `receita_federal`.
- **Nunca deixe o volume encher durante o ETL** — staging + downloads + temp disputam os
  50 GB. Se ficar apertado, aumente o volume (Hetzner permite crescer: 50 → 100 GB).
- O `--relocate-to-main` é **best-effort**: se falhar, o switch já ocorreu e o banco está
  no volume; rode manualmente
  `ALTER DATABASE receita_federal SET TABLESPACE pg_default;` quando possível.

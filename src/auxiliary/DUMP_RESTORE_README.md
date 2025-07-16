# 🗄️ Dump e Restauração - Receita Federal CNPJ

Sistema completo para fazer backup e restaurar o banco de dados da Receita Federal com todos os dados públicos de CNPJ.

## 📋 Pré-requisitos

- PostgreSQL 12+ instalado
- Python 3.8+
- Pacotes Python: `asyncpg`, `python-dotenv`, `rich`
- Comando `pg_dump` e `pg_restore` disponíveis no PATH

```bash
# Instalar dependências
pip install asyncpg python-dotenv rich
```

## 🛠️ Configuração

1. **Configurar variáveis de ambiente** (arquivo `.env`):
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=receita_federal
DB_USER=postgres
DB_PASSWORD=sua_senha
```

2. **Verificar conexão com o banco**:
```bash
psql -h localhost -p 5432 -U postgres -d receita_federal
```

## 🚀 Comandos Disponíveis

### 1. Informações do Banco
```bash
python src/dump_and_restore.py info
```
Exibe estatísticas completas do banco (tamanho, registros por tabela, etc.)

### 2. Gerar Modelo (Apenas Estrutura)
```bash
python src/dump_and_restore.py model
```
Cria:
- `receita_federal_model_YYYYMMDD_HHMMSS.sql` - Estrutura do banco
- `receita_federal_model_doc_YYYYMMDD_HHMMSS.md` - Documentação

### 3. Dump Completo (Estrutura + Dados)
```bash
python src/dump_and_restore.py dump
```
Cria:
- `receita_federal_full_YYYYMMDD_HHMMSS.dump` - Backup completo

### 4. Restaurar Banco
```bash
python src/dump_and_restore.py restore arquivo_dump.dump
```
Restaura o banco em outro ambiente (solicita configurações interativamente)

### 5. Gerar Tudo
```bash
python src/dump_and_restore.py all
```
Gera modelo + dump completo + documentação

## 📊 Exemplo de Uso Completo

### Cenário: Migrar banco para outro servidor

1. **No servidor origem:**
```bash
# Gerar dump completo
python src/dump_and_restore.py dump
# Resultado: receita_federal_full_20250716_143022.dump
```

2. **No servidor destino:**
```bash
# Copiar arquivo dump para o servidor destino
scp receita_federal_full_20250716_143022.dump usuario@servidor-destino:/tmp/

# Restaurar
python src/dump_and_restore.py restore /tmp/receita_federal_full_20250716_143022.dump
```

### Cenário: Criar ambiente de desenvolvimento

1. **Gerar apenas estrutura:**
```bash
python src/dump_and_restore.py model
```

2. **Aplicar configurações otimizadas:**
```bash
psql -h localhost -p 5432 -U postgres -d receita_federal_dev -f src/database_setup.sql
```

## 🔧 Configuração Avançada

### Otimizações PostgreSQL

Para bancos grandes (65M+ registros), configure no `postgresql.conf`:

```conf
# Memória
shared_buffers = 4GB
work_mem = 1GB
maintenance_work_mem = 2GB

# WAL
max_wal_size = 4GB
min_wal_size = 1GB
wal_buffers = 16MB

# Checkpoint
checkpoint_completion_target = 0.9
checkpoint_timeout = 15min

# Conexões
max_connections = 200
```

### Aplicar Configurações
```bash
# Aplicar configurações de performance e índices
psql -h localhost -p 5432 -U postgres -d receita_federal -f src/database_setup.sql
```

## 📈 Monitoramento

### Verificar Progresso do Dump
```bash
# Em outro terminal, monitorar tamanho do arquivo
watch -n 5 'ls -lh receita_federal_full_*.dump'
```

### Verificar Progresso da Restauração
```bash
# Monitorar logs do PostgreSQL
tail -f /var/log/postgresql/postgresql-*.log
```

## 🔍 Validação

### Validar Integridade dos Dados
```sql
-- Verificar total de registros
SELECT 
    'empresa' as tabela, COUNT(*) as registros FROM empresa
UNION ALL
SELECT 
    'estabelecimento' as tabela, COUNT(*) as registros FROM estabelecimento
UNION ALL
SELECT 
    'socios' as tabela, COUNT(*) as registros FROM socios
UNION ALL
SELECT 
    'simples' as tabela, COUNT(*) as registros FROM simples;

-- Verificar integridade referencial
SELECT e.cnpj_basico 
FROM empresa e 
LEFT JOIN estabelecimento est ON e.cnpj_basico = est.cnpj_basico 
WHERE est.cnpj_basico IS NULL
LIMIT 10;
```

### Verificar Performance
```sql
-- Verificar uso de índices
EXPLAIN (ANALYZE, BUFFERS) 
SELECT * FROM estabelecimento WHERE cnpj_basico = '11222333';

-- Estatísticas de tabelas
SELECT 
    schemaname,
    tablename,
    n_tup_ins,
    n_tup_upd,
    n_tup_del,
    n_live_tup,
    n_dead_tup
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;
```

## 🛡️ Segurança

### Backup Seguro
```bash
# Criptografar dump
gpg --cipher-algo AES256 --compress-algo 1 --symmetric receita_federal_full_20250716_143022.dump

# Descriptografar
gpg --decrypt receita_federal_full_20250716_143022.dump.gpg > receita_federal_full_20250716_143022.dump
```

### Usuários Recomendados
```sql
-- Usuário somente leitura
CREATE USER receita_readonly WITH PASSWORD 'senha_segura';
GRANT CONNECT ON DATABASE receita_federal TO receita_readonly;
GRANT USAGE ON SCHEMA public TO receita_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO receita_readonly;

-- Usuário para aplicação
CREATE USER receita_app WITH PASSWORD 'senha_aplicacao';
GRANT CONNECT ON DATABASE receita_federal TO receita_app;
GRANT USAGE ON SCHEMA public TO receita_app;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO receita_app;
```

## 🚨 Troubleshooting

### Problemas Comuns

1. **Erro de memória durante dump:**
```bash
# Aumentar work_mem temporariamente
export PGOPTIONS="-c work_mem=2GB"
```

2. **Restauração lenta:**
```bash
# Desabilitar autovacuum temporariamente
ALTER TABLE empresa SET (autovacuum_enabled = false);
-- Reabilitar após restauração
ALTER TABLE empresa SET (autovacuum_enabled = true);
```

3. **Arquivo dump muito grande:**
```bash
# Usar compressão máxima
pg_dump --compress=9 --format=custom ...
```

4. **Falta de espaço em disco:**
```bash
# Verificar espaço
df -h
# Dump direto para outro servidor
pg_dump ... | ssh usuario@servidor-destino "cat > /path/dump.dump"
```

## 📋 Estrutura dos Arquivos

```
src/
├── dump_and_restore.py      # Script principal
├── database_setup.sql       # Configurações e otimizações
├── consultar_empresa.py     # Consultas otimizadas
├── create_indexes.py        # Criação de índices
└── resume_etl.py           # Retomar ETL

backups/
├── receita_federal_model_YYYYMMDD_HHMMSS.sql
├── receita_federal_full_YYYYMMDD_HHMMSS.dump
└── receita_federal_model_doc_YYYYMMDD_HHMMSS.md
```

## 🔗 Recursos Adicionais

- [Documentação PostgreSQL - pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html)
- [Documentação PostgreSQL - pg_restore](https://www.postgresql.org/docs/current/app-pgrestore.html)
- [Otimização PostgreSQL](https://wiki.postgresql.org/wiki/Performance_Optimization)

## 📞 Suporte

Para problemas específicos:
1. Verificar logs do PostgreSQL
2. Executar `python src/dump_and_restore.py info` para diagnóstico
3. Verificar espaço em disco e memória disponível
4. Consultar documentação oficial do PostgreSQL
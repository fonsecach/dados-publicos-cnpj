# 📄 SQL - Arquivos Indispensáveis

Scripts SQL essenciais para estrutura do banco, configurações e consultas principais.

## 📋 Arquivos

### 🏗️ `banco_de_dados.sql`
**Script de criação da estrutura do banco**

**Funcionalidades:**
- Criação de todas as tabelas
- Definição de tipos de dados
- Configuração de constraints
- Estrutura base para ETL

**Uso:**
```bash
# Criar estrutura do banco
psql -h localhost -p 5432 -U postgres -d receita_federal -f src/sql/banco_de_dados.sql
```

### 🔧 `database_setup.sql`
**Script de configuração e otimização**

**Funcionalidades:**
- Configurações de performance PostgreSQL
- Criação de índices otimizados
- Views úteis para consultas
- Funções auxiliares
- Configurações de segurança

**Uso:**
```bash
# Aplicar configurações
psql -h localhost -p 5432 -U postgres -d receita_federal -f src/sql/database_setup.sql
```

### 🔍 `consulta_empresa_completa.sql`
**Consultas SQL para dados completos de empresas**

**Funcionalidades:**
- Consulta por CNPJ básico ou completo
- Dados da empresa + estabelecimentos + sócios + simples
- Consultas otimizadas com JOINs
- Formatação de dados
- Consulta unificada com JSON

**Uso:**
```bash
# Executar consultas
psql -h localhost -p 5432 -U postgres -d receita_federal -f src/sql/consulta_empresa_completa.sql
```

## 🏗️ Estrutura do Banco

### Tabelas Principais:

#### 📊 `empresa`
```sql
CREATE TABLE empresa (
    cnpj_basico TEXT NOT NULL PRIMARY KEY,
    razao_social TEXT,
    natureza_juridica INTEGER,
    qualificacao_responsavel INTEGER,
    capital_social DECIMAL(18,2),
    porte_empresa TEXT,
    ente_federativo_responsavel TEXT
);
```

#### 🏢 `estabelecimento`
```sql
CREATE TABLE estabelecimento (
    cnpj_basico TEXT NOT NULL,
    cnpj_ordem TEXT NOT NULL,
    cnpj_dv TEXT NOT NULL,
    nome_fantasia TEXT,
    situacao_cadastral TEXT,
    data_situacao_cadastral DATE,
    motivo_situacao_cadastral INTEGER,
    nome_cidade_exterior TEXT,
    pais INTEGER,
    data_inicio_atividade DATE,
    cnae_fiscal_principal INTEGER,
    cnae_fiscal_secundaria TEXT,
    tipo_logradouro TEXT,
    logradouro TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cep TEXT,
    uf TEXT,
    municipio INTEGER,
    ddd_1 TEXT,
    telefone_1 TEXT,
    ddd_2 TEXT,
    telefone_2 TEXT,
    ddd_fax TEXT,
    fax TEXT,
    correio_eletronico TEXT,
    situacao_especial TEXT,
    data_situacao_especial DATE,
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
);
```

#### 👥 `socios`
```sql
CREATE TABLE socios (
    cnpj_basico TEXT NOT NULL,
    identificador_socio TEXT,
    nome_socio TEXT,
    cnpj_cpf_socio TEXT,
    qualificacao_socio INTEGER,
    data_entrada_sociedade DATE,
    pais INTEGER,
    representante_legal TEXT,
    nome_representante TEXT,
    qualificacao_representante INTEGER,
    faixa_etaria TEXT
);
```

#### 💼 `simples`
```sql
CREATE TABLE simples (
    cnpj_basico TEXT NOT NULL PRIMARY KEY,
    opcao_pelo_simples TEXT,
    data_opcao_simples DATE,
    data_exclusao_simples DATE,
    opcao_mei TEXT,
    data_opcao_mei DATE,
    data_exclusao_mei DATE
);
```

### Tabelas de Referência:

#### 🏭 `cnae`
```sql
CREATE TABLE cnae (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);
```

#### 🏛️ `natureza`
```sql
CREATE TABLE natureza (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);
```

#### 👨‍💼 `qualificacao`
```sql
CREATE TABLE qualificacao (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);
```

#### 🏙️ `municipio`
```sql
CREATE TABLE municipio (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);
```

#### 🌍 `pais`
```sql
CREATE TABLE pais (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);
```

#### 📋 `motivo`
```sql
CREATE TABLE motivo (
    codigo INTEGER PRIMARY KEY,
    descricao TEXT
);
```

## 🔧 Configurações Essenciais

### Performance PostgreSQL:
```sql
-- Memória
SET work_mem = '1GB';
SET maintenance_work_mem = '2GB';

-- WAL
SET wal_buffers = '16MB';
SET synchronous_commit = off;

-- Checkpoint
SET checkpoint_completion_target = 0.9;
```

### Índices Principais:
```sql
-- CNPJs
CREATE INDEX IF NOT EXISTS idx_empresa_cnpj ON empresa(cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_cnpj ON estabelecimento(cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_socios_cnpj ON socios(cnpj_basico);
CREATE INDEX IF NOT EXISTS idx_simples_cnpj ON simples(cnpj_basico);

-- Filtros comuns
CREATE INDEX IF NOT EXISTS idx_estabelecimento_situacao ON estabelecimento(situacao_cadastral);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_municipio ON estabelecimento(municipio);
CREATE INDEX IF NOT EXISTS idx_estabelecimento_uf ON estabelecimento(uf);
```

## 🔍 Consultas Essenciais

### 1. **Consulta Empresa Completa**
```sql
-- Buscar todos os dados de uma empresa
SELECT 
    e.*,
    est.*,
    s.*,
    sim.*
FROM empresa e
LEFT JOIN estabelecimento est ON e.cnpj_basico = est.cnpj_basico
LEFT JOIN socios s ON e.cnpj_basico = s.cnpj_basico
LEFT JOIN simples sim ON e.cnpj_basico = sim.cnpj_basico
WHERE e.cnpj_basico = '12345678';
```

### 2. **Consulta por CNPJ Completo**
```sql
-- Buscar estabelecimento específico
SELECT 
    e.razao_social,
    est.*,
    cnae.descricao as cnae_descricao,
    mun.descricao as municipio_descricao
FROM estabelecimento est
LEFT JOIN empresa e ON est.cnpj_basico = e.cnpj_basico
LEFT JOIN cnae ON est.cnae_fiscal_principal = cnae.codigo
LEFT JOIN municipio mun ON est.municipio = mun.codigo
WHERE est.cnpj_basico = '12345678'
AND est.cnpj_ordem = '0001'
AND est.cnpj_dv = '81';
```

### 3. **Estatísticas Gerais**
```sql
-- Resumo do banco
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
```

## 🎯 Views Úteis

### View Empresa Completa:
```sql
CREATE VIEW v_empresa_completa AS
SELECT 
    e.cnpj_basico,
    e.razao_social,
    e.natureza_juridica,
    nj.descricao as natureza_juridica_descricao,
    est.nome_fantasia,
    est.situacao_cadastral,
    est.cnae_fiscal_principal,
    cnae.descricao as cnae_principal_descricao,
    mun.descricao as municipio_descricao,
    est.uf
FROM empresa e
LEFT JOIN estabelecimento est ON e.cnpj_basico = est.cnpj_basico AND est.cnpj_ordem = '0001'
LEFT JOIN natureza nj ON e.natureza_juridica = nj.codigo
LEFT JOIN cnae ON est.cnae_fiscal_principal = cnae.codigo
LEFT JOIN municipio mun ON est.municipio = mun.codigo;
```

### View Estabelecimentos Ativos:
```sql
CREATE VIEW v_estabelecimentos_ativos AS
SELECT 
    est.*,
    e.razao_social,
    cnae.descricao as cnae_descricao,
    mun.descricao as municipio_descricao
FROM estabelecimento est
LEFT JOIN empresa e ON est.cnpj_basico = e.cnpj_basico
LEFT JOIN cnae ON est.cnae_fiscal_principal = cnae.codigo
LEFT JOIN municipio mun ON est.municipio = mun.codigo
WHERE est.situacao_cadastral = '02';
```

## 🛠️ Funções Úteis

### Formatação de CNPJ:
```sql
CREATE OR REPLACE FUNCTION format_cnpj(cnpj text) 
RETURNS text AS $$
BEGIN
    IF length(cnpj) = 14 THEN
        RETURN substring(cnpj from 1 for 2) || '.' ||
               substring(cnpj from 3 for 3) || '.' ||
               substring(cnpj from 6 for 3) || '/' ||
               substring(cnpj from 9 for 4) || '-' ||
               substring(cnpj from 13 for 2);
    ELSIF length(cnpj) = 8 THEN
        RETURN substring(cnpj from 1 for 2) || '.' ||
               substring(cnpj from 3 for 3) || '.' ||
               substring(cnpj from 6 for 3);
    ELSE
        RETURN cnpj;
    END IF;
END;
$$ LANGUAGE plpgsql;
```

### Busca de Empresa:
```sql
CREATE OR REPLACE FUNCTION buscar_empresa(cnpj_input text)
RETURNS TABLE(
    cnpj_basico text,
    razao_social text,
    situacao_cadastral text,
    cnae_principal text,
    municipio text,
    uf text
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        e.cnpj_basico,
        e.razao_social,
        est.situacao_cadastral,
        cnae.descricao as cnae_principal,
        mun.descricao as municipio,
        est.uf
    FROM empresa e
    LEFT JOIN estabelecimento est ON e.cnpj_basico = est.cnpj_basico AND est.cnpj_ordem = '0001'
    LEFT JOIN cnae ON est.cnae_fiscal_principal = cnae.codigo
    LEFT JOIN municipio mun ON est.municipio = mun.codigo
    WHERE e.cnpj_basico = CASE 
        WHEN length(cnpj_input) = 14 THEN substring(cnpj_input from 1 for 8)
        WHEN length(cnpj_input) = 8 THEN cnpj_input
        ELSE cnpj_input
    END;
END;
$$ LANGUAGE plpgsql;
```

## 🔐 Segurança

### Usuários Recomendados:
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

## 📊 Manutenção

### Atualização de Estatísticas:
```sql
-- Atualizar estatísticas de todas as tabelas
ANALYZE empresa;
ANALYZE estabelecimento;
ANALYZE socios;
ANALYZE simples;
```

### Limpeza:
```sql
-- Limpeza de espaço (cuidado: demora muito)
VACUUM ANALYZE;

-- Reconstruir índices se necessário
REINDEX DATABASE receita_federal;
```
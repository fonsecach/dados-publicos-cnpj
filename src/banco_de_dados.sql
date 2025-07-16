-- Criar a base de dados "Dados_RFB"
CREATE DATABASE "Dados_RFB"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;

COMMENT ON DATABASE "Dados_RFB"
    IS 'Base de dados para gravar os dados públicos de CNPJ da Receita Federal do Brasil';

-- Diretório físico do banco de dados:
--SHOW data_directory;

-- ========================================
-- CRIAÇÃO DAS TABELAS
-- ========================================

-- Conectar na base de dados
\c "Dados_RFB";

-- Tabela empresa
CREATE TABLE empresa (
    cnpj_basico TEXT NOT NULL,
    razao_social TEXT,
    natureza_juridica INTEGER,
    qualificacao_responsavel INTEGER,
    capital_social NUMERIC(15,2),
    porte_empresa INTEGER,
    ente_federativo_responsavel TEXT
);

-- Tabela estabelecimento  
CREATE TABLE estabelecimento (
    cnpj_basico TEXT NOT NULL,
    cnpj_ordem TEXT NOT NULL,
    cnpj_dv TEXT NOT NULL,
    identificador_matriz_filial INTEGER,
    nome_fantasia TEXT,
    situacao_cadastral INTEGER,
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
    data_situacao_especial DATE
);

-- Tabela socios
CREATE TABLE socios (
    cnpj_basico TEXT NOT NULL,
    identificador_socio INTEGER,
    nome_socio TEXT,
    cnpj_cpf_socio TEXT,
    qualificacao_socio INTEGER,
    data_entrada_sociedade DATE,
    pais INTEGER,
    representante_legal TEXT,
    nome_representante TEXT,
    qualificacao_representante_legal INTEGER,
    faixa_etaria INTEGER
);

-- Tabela simples
CREATE TABLE simples (
    cnpj_basico TEXT NOT NULL,
    opcao_pelo_simples TEXT,
    data_opcao_simples DATE,
    data_exclusao_simples DATE,
    opcao_mei TEXT,
    data_opcao_mei DATE,
    data_exclusao_mei DATE
);

-- Tabelas de apoio (códigos e descrições)
CREATE TABLE cnae (
    codigo INTEGER NOT NULL PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE motivo (
    codigo INTEGER NOT NULL PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE municipio (
    codigo INTEGER NOT NULL PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE natureza (
    codigo INTEGER NOT NULL PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE pais (
    codigo INTEGER NOT NULL PRIMARY KEY,
    descricao TEXT
);

CREATE TABLE qualificacao (
    codigo INTEGER NOT NULL PRIMARY KEY,
    descricao TEXT
);

-- Índices para melhor performance
CREATE INDEX IF NOT EXISTS empresa_cnpj ON empresa(cnpj_basico);
CREATE INDEX IF NOT EXISTS estabelecimento_cnpj ON estabelecimento(cnpj_basico);
CREATE INDEX IF NOT EXISTS socios_cnpj ON socios(cnpj_basico);
CREATE INDEX IF NOT EXISTS simples_cnpj ON simples(cnpj_basico);
# 📁 Estrutura do Projeto - Receita Federal CNPJ

## 📋 Organização das Pastas

```
src/
├── etl/                    # 🔄 Processo ETL (Extract, Transform, Load)
├── validation/             # ✅ Scripts de validação e verificação
├── indexes/                # 📊 Criação e gerenciamento de índices
├── sql/                    # 📄 Arquivos SQL indispensáveis
└── auxiliary/              # 🛠️ Scripts auxiliares
    ├── python/             # 🐍 Scripts Python auxiliares
    └── sql/                # 📄 Arquivos SQL auxiliares
```

## 📂 Descrição das Pastas

### 🔄 `/etl/` - Processo ETL
Scripts principais para extração, transformação e carregamento dos dados da Receita Federal.

### ✅ `/validation/` - Validação
Scripts para verificar integridade, consistência e qualidade dos dados.

### 📊 `/indexes/` - Índices
Scripts para criação e gerenciamento de índices para otimização de consultas.

### 📄 `/sql/` - SQL Indispensáveis
Arquivos SQL essenciais para estrutura do banco, configurações e consultas principais.

### 🛠️ `/auxiliary/` - Scripts Auxiliares
Scripts complementares e utilitários:
- **`python/`**: Scripts Python auxiliares (dumps, consultas, etc.)
- **`sql/`**: Scripts SQL auxiliares e utilitários

## 🚀 Ordem de Execução Recomendada

1. **ETL**: `etl/ETL_dados_publicos_empresas.py`
2. **Validação**: `validation/check_database_status.py`
3. **Índices**: `indexes/create_indexes.py`
4. **Configuração**: `sql/database_setup.sql`

## 📖 Documentação Completa

- **ETL**: Ver `etl/README.md`
- **Validação**: Ver `validation/README.md`
- **Índices**: Ver `indexes/README.md`
- **SQL**: Ver `sql/README.md`
- **Auxiliares**: Ver `auxiliary/README.md`
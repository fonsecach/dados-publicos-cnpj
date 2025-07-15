# Setup - Dados Públicos CNPJ com Docker

## Pré-requisitos

- Docker e Docker Compose instalados
- Python 3.10+ 
- uv (gerenciador de dependências Python)

## Instalação

### 1. Instalar o uv (se ainda não instalado)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Clonar e configurar o projeto

```bash
cd /caminho/para/o/projeto
uv sync
```

### 3. Iniciar o PostgreSQL com Docker

```bash
docker-compose up -d
```

Isso irá:
- Criar um container PostgreSQL usando a imagem Bitnami
- Configurar o banco de dados `receita_cnpj`
- Executar o script de inicialização do banco

### 4. Verificar se o banco está rodando

```bash
docker-compose ps
```

### 5. Executar o ETL

```bash
uv run python code/ETL_coletar_dados_e_gravar_BD.py
```

## Configurações

O arquivo `.env` é criado automaticamente com as seguintes configurações:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=receita_cnpj
DB_USER=postgres
DB_PASSWORD=senha123

# File paths (Linux compatible)
OUTPUT_FILES_PATH=./data/downloads
EXTRACTED_FILES_PATH=./data/extracted
```

## Melhorias Implementadas

1. **Docker Compose**: PostgreSQL Bitnami containerizado
2. **httpx**: Substituiu wget com retry automático
3. **Paths Linux**: Compatibilidade com sistemas Unix
4. **URL Atualizada**: Aponta para dados de 2025-06
5. **uv**: Gerenciamento moderno de dependências
6. **Retry Logic**: Download com tentativas automáticas

## Comandos Úteis

```bash
# Ver logs do banco
docker-compose logs postgres

# Parar o banco
docker-compose down

# Resetar completamente (apaga dados)
docker-compose down -v

# Conectar ao banco diretamente
docker exec -it receita_postgres psql -U postgres -d receita_cnpj
```
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
git clone https://github.com/[usuario]/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ.git
cd Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ
uv sync
```

### 3. Configurar variáveis de ambiente

```bash
cp .env.example .env
```

### 4. Iniciar o PostgreSQL com Docker

```bash
docker-compose up -d
```

Isso irá:
- Criar um container PostgreSQL usando a imagem Bitnami
- Configurar o banco de dados `receita_cnpj`
- Executar o script de inicialização do banco

### 5. Verificar se o banco está rodando

```bash
docker-compose ps
```

### 6. Executar o ETL

```bash
uv run python src/ETL_dados_publicos_empresas.py
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

### Estrutura Refatorada (2025)
1. **Reorganização de código**: Código movido para diretório `src/` para melhor organização
2. **Novo script ETL**: `src/ETL_dados_publicos_empresas.py` com melhor estrutura
3. **Configuração simplificada**: Arquivo `.env.example` para configuração inicial

### Melhorias de 2024
1. **Docker Compose**: PostgreSQL Bitnami containerizado
2. **httpx**: Substituiu wget com retry automático
3. **Paths Linux**: Compatibilidade com sistemas Unix
4. **URL Atualizada**: Aponta para dados atualizados
5. **uv**: Gerenciamento moderno de dependências Python
6. **Retry Logic**: Download com tentativas automáticas
7. **Tipos de dados**: Colunas numéricas como 'Int32' para melhor performance
8. **psycopg2-binary**: Dependência simplificada para instalação

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
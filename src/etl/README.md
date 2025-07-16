# 🔄 ETL - Extract, Transform, Load

Scripts para o processo completo de ETL dos dados públicos da Receita Federal.

## 📋 Arquivos

### 🎯 `ETL_dados_publicos_empresas.py`
**Script principal do processo ETL**

**Funcionalidades:**
- Download automático dos arquivos da Receita Federal
- Extração de arquivos ZIP
- Transformação e limpeza dos dados
- Carregamento no banco PostgreSQL
- Processamento assíncrono para performance

**Uso:**
```bash
# Executar ETL completo
python src/etl/ETL_dados_publicos_empresas.py

# Com ambiente virtual
uv run src/etl/ETL_dados_publicos_empresas.py
```

**Configuração:**
Arquivo `.env` na raiz do projeto:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=receita_federal
DB_USER=postgres
DB_PASSWORD=sua_senha
```

### 🔄 `resume_etl.py`
**Script para retomar ETL interrompido**

**Funcionalidades:**
- Verifica estado atual do banco
- Retoma processo de onde parou
- Foca na criação de índices (parte que mais falha)
- Validação de integridade

**Uso:**
```bash
# Retomar ETL
python src/etl/resume_etl.py

# Com ambiente virtual
uv run src/etl/resume_etl.py
```

## 🔍 Processo ETL Detalhado

### 1. **Extract (Extração)**
- Download dos arquivos ZIP da Receita Federal
- Verificação de integridade dos arquivos
- Extração dos arquivos CSV

### 2. **Transform (Transformação)**
- Limpeza de dados
- Conversão de tipos
- Tratamento de encoding
- Processamento em chunks para otimização

### 3. **Load (Carregamento)**
- Criação das tabelas
- Inserção assíncrona dos dados
- Criação de índices (opcional)
- Validação final

## 📊 Dados Processados

### Tabelas Principais:
- **`empresa`**: Dados básicos das empresas (~63M registros)
- **`estabelecimento`**: Estabelecimentos/filiais (~66M registros)
- **`socios`**: Sócios e representantes (~26M registros)
- **`simples`**: Regime tributário Simples Nacional (~44M registros)

### Tabelas de Referência:
- **`cnae`**: Códigos de atividade econômica
- **`natureza`**: Naturezas jurídicas
- **`municipio`**: Códigos dos municípios
- **`pais`**: Códigos dos países
- **`qualificacao`**: Qualificações de sócios
- **`motivo`**: Motivos de situação cadastral

## ⚡ Performance

### Otimizações Implementadas:
- **Processamento assíncrono**: Múltiplas operações simultâneas
- **Chunking**: Processamento em lotes
- **Índices eficientes**: Criação posterior aos dados
- **Conexão pooling**: Reutilização de conexões

### Tempo Estimado:
- **Download**: 5-10 minutos
- **Extração**: 5-10 minutos
- **Processamento**: 2-4 horas
- **Índices**: 30-60 minutos
- **Total**: 3-5 horas (dependendo do hardware)

## 🚨 Troubleshooting

### Problemas Comuns:

1. **Timeout na criação de índices:**
   ```bash
   # Use o script de retomada
   python src/etl/resume_etl.py
   ```

2. **Memória insuficiente:**
   ```bash
   # Aumentar configurações PostgreSQL
   # work_mem = 1GB
   # maintenance_work_mem = 2GB
   ```

3. **Erro de conexão:**
   ```bash
   # Verificar .env
   # Testar conexão: psql -h localhost -p 5432 -U postgres -d receita_federal
   ```

4. **Arquivo corrompido:**
   ```bash
   # Deletar arquivos de download e rodar novamente
   rm -rf downloads/
   python src/etl/ETL_dados_publicos_empresas.py
   ```

## 📈 Monitoramento

### Logs:
- Arquivo: `etl_log.log`
- Nível: INFO, ERROR
- Rotação automática por tamanho

### Métricas:
- Total de registros processados
- Tempo de execução por fase
- Uso de memória
- Erros e warnings

## 🔧 Configuração Avançada

### PostgreSQL:
```sql
-- Configurações recomendadas no postgresql.conf
shared_buffers = 4GB
work_mem = 1GB
maintenance_work_mem = 2GB
max_wal_size = 4GB
checkpoint_completion_target = 0.9
```

### Sistema:
```bash
# Aumentar limites de arquivo
ulimit -n 65536

# Verificar espaço em disco (mínimo 50GB)
df -h

# Verificar memória disponível (mínimo 8GB)
free -h
```
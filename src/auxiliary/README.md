# 🛠️ Auxiliares - Scripts Complementares

Scripts e utilitários auxiliares para suporte, análise e manutenção do sistema.

## 📋 Estrutura

```
auxiliary/
├── python/          # Scripts Python auxiliares
└── sql/             # Scripts SQL auxiliares
```

## 🐍 Python (`/python/`)

### 📊 `consultar_empresa.py`
**Interface interativa para consultas de empresas**

**Funcionalidades:**
- Consulta completa por CNPJ
- Formatação rica de dados
- Exibição de empresa, estabelecimentos, sócios e simples
- Processamento de CNAEs secundários
- Validação de entrada

**Uso:**
```bash
# Consultar empresa por CNPJ
python src/auxiliary/python/consultar_empresa.py 75608620000107

# CNPJ básico
python src/auxiliary/python/consultar_empresa.py 75608620

# Com formatação
python src/auxiliary/python/consultar_empresa.py 75.608.620/0001-07
```

### 🗄️ `dump_and_restore.py`
**Sistema completo de backup e restauração**

**Funcionalidades:**
- Geração de dumps completos
- Criação de modelos (apenas estrutura)
- Restauração em outros ambientes
- Validação de integridade
- Relatórios detalhados

**Uso:**
```bash
# Ver informações do banco
python src/auxiliary/python/dump_and_restore.py info

# Gerar modelo
python src/auxiliary/python/dump_and_restore.py model

# Dump completo
python src/auxiliary/python/dump_and_restore.py dump

# Restaurar
python src/auxiliary/python/dump_and_restore.py restore arquivo.dump
```

### 🔧 `sql_dump_generator.py`
**Gerador de dump usando apenas SQL**

**Funcionalidades:**
- Alternativa quando pg_dump não está disponível
- Geração de estrutura SQL
- Amostra de dados para testes
- Script de instalação automático

**Uso:**
```bash
# Gerar dump SQL
python src/auxiliary/python/sql_dump_generator.py
```

## 📄 SQL (`/sql/`)

### Scripts auxiliares e utilitários SQL serão organizados aqui conforme necessário.

## 🎯 Casos de Uso

### 1. **Análise de Dados**
```bash
# Consultar empresa específica
python src/auxiliary/python/consultar_empresa.py 11222333000181

# Resultado: Dados completos formatados
```

### 2. **Backup e Migração**
```bash
# Criar backup completo
python src/auxiliary/python/dump_and_restore.py dump

# Migrar para outro servidor
python src/auxiliary/python/dump_and_restore.py restore backup.dump
```

### 3. **Desenvolvimento**
```bash
# Criar ambiente de desenvolvimento
python src/auxiliary/python/sql_dump_generator.py

# Instalar com dados de amostra
./install_database.sh
```

### 4. **Monitoramento**
```bash
# Verificar status atual
python src/auxiliary/python/dump_and_restore.py info

# Resultado: Estatísticas completas do banco
```

## 🔍 Recursos Avançados

### Consulta Personalizada:
```python
# Exemplo de uso do consultar_empresa.py
async def main():
    cnpj = "75608620000107"
    
    # Buscar dados completos
    empresa = await consultar_empresa_basico(conn, cnpj)
    estabelecimentos = await consultar_estabelecimentos(conn, cnpj)
    socios = await consultar_socios(conn, cnpj)
    simples = await consultar_simples(conn, cnpj)
    
    # Exibir formatado
    exibir_empresa_basico(empresa)
    exibir_estabelecimentos(estabelecimentos)
    exibir_socios(socios)
    exibir_simples(simples)
```

### Dump Personalizado:
```python
# Exemplo de uso do dump_and_restore.py
async def criar_backup():
    # Obter informações do banco
    info = await get_database_info()
    
    # Gerar dump com compressão
    dump_file = await generate_full_dump()
    
    # Validar integridade
    if await validate_dump(dump_file):
        print("Backup criado com sucesso!")
```

## 📊 Integração com Scripts Principais

### Fluxo Completo:
```bash
# 1. Executar ETL
python src/etl/ETL_dados_publicos_empresas.py

# 2. Validar dados
python src/validation/check_database_status.py

# 3. Criar índices
python src/indexes/create_indexes.py

# 4. Aplicar configurações
psql -f src/sql/database_setup.sql

# 5. Fazer backup
python src/auxiliary/python/dump_and_restore.py dump

# 6. Testar consultas
python src/auxiliary/python/consultar_empresa.py 11222333000181
```

## 🔧 Configuração

### Variáveis de Ambiente:
```env
# .env na raiz do projeto
DB_HOST=localhost
DB_PORT=5432
DB_NAME=receita_federal
DB_USER=postgres
DB_PASSWORD=senha
```

### Dependências:
```bash
# Instalar dependências
pip install asyncpg python-dotenv rich

# Ou com UV
uv add asyncpg python-dotenv rich
```

## 🎨 Personalização

### Adicionar Novos Scripts:
1. **Python**: Adicionar em `auxiliary/python/`
2. **SQL**: Adicionar em `auxiliary/sql/`
3. **Documentar**: Atualizar este README

### Exemplo de Novo Script:
```python
# auxiliary/python/novo_script.py
#!/usr/bin/env python3
"""
Novo script auxiliar
"""

import asyncio
import asyncpg
from dotenv import load_dotenv

async def main():
    # Sua lógica aqui
    pass

if __name__ == "__main__":
    asyncio.run(main())
```

## 🚀 Roadmap

### Próximos Scripts:
- [ ] **Análise de qualidade**: Verificar inconsistências
- [ ] **Relatórios**: Gerar relatórios automatizados
- [ ] **Exportação**: Exportar dados para CSV/Excel
- [ ] **API**: Interface REST para consultas
- [ ] **Dashboard**: Interface web para visualização

### Melhorias:
- [ ] **Cache**: Sistema de cache para consultas frequentes
- [ ] **Logs**: Sistema de logging unificado
- [ ] **Testes**: Testes automatizados
- [ ] **Documentação**: Documentação automatizada
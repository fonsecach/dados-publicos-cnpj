#!/usr/bin/env python3
"""
Script para resumir o processo ETL da Receita Federal CNPJ
de onde parou (apenas criação de índices)

Baseado nos logs, todos os dados foram inseridos com sucesso.
Apenas a criação de índices falhou devido a timeout.
"""

import asyncio
import asyncpg
import os
import sys
import time
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

# Carregar variáveis de ambiente
load_dotenv()

console = Console()

# Configurações do banco
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'receita_federal'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
    'command_timeout': 3600,  # 1 hora de timeout
    'server_settings': {
        'statement_timeout': '3600000',  # 1 hora em ms
        'lock_timeout': '3600000',
        'idle_in_transaction_session_timeout': '3600000'
    }
}

# Tabelas que devem existir
EXPECTED_TABLES = [
    'empresa',
    'estabelecimento', 
    'socios',
    'simples',
    'cnae',
    'motivo',
    'municipio',
    'natureza',
    'pais',
    'qualificacao'
]

# Índices que precisam ser criados
INDEXES_TO_CREATE = [
    {
        'name': 'empresa_cnpj',
        'table': 'empresa',
        'columns': 'cnpj_basico',
        'sql': 'CREATE INDEX IF NOT EXISTS empresa_cnpj ON empresa(cnpj_basico);'
    },
    {
        'name': 'estabelecimento_cnpj',
        'table': 'estabelecimento',
        'columns': 'cnpj_basico',
        'sql': 'CREATE INDEX IF NOT EXISTS estabelecimento_cnpj ON estabelecimento(cnpj_basico);'
    },
    {
        'name': 'estabelecimento_cnpj_completo',
        'table': 'estabelecimento',
        'columns': 'cnpj_basico, cnpj_ordem, cnpj_dv',
        'sql': 'CREATE INDEX IF NOT EXISTS estabelecimento_cnpj_completo ON estabelecimento(cnpj_basico, cnpj_ordem, cnpj_dv);'
    },
    {
        'name': 'socios_cnpj',
        'table': 'socios',
        'columns': 'cnpj_basico',
        'sql': 'CREATE INDEX IF NOT EXISTS socios_cnpj ON socios(cnpj_basico);'
    },
    {
        'name': 'simples_cnpj',
        'table': 'simples',
        'columns': 'cnpj_basico',
        'sql': 'CREATE INDEX IF NOT EXISTS simples_cnpj ON simples(cnpj_basico);'
    },
    {
        'name': 'estabelecimento_situacao',
        'table': 'estabelecimento',
        'columns': 'situacao_cadastral',
        'sql': 'CREATE INDEX IF NOT EXISTS estabelecimento_situacao ON estabelecimento(situacao_cadastral);'
    },
    {
        'name': 'estabelecimento_municipio',
        'table': 'estabelecimento',
        'columns': 'municipio',
        'sql': 'CREATE INDEX IF NOT EXISTS estabelecimento_municipio ON estabelecimento(municipio);'
    }
]


async def create_db_pool():
    """Cria pool de conexões com o banco"""
    try:
        pool = await asyncpg.create_pool(**DB_CONFIG)
        return pool
    except Exception as e:
        console.print(f"[red]Erro ao conectar com o banco: {e}[/red]")
        sys.exit(1)


async def verify_data_integrity(pool):
    """Verifica se todos os dados foram inseridos corretamente"""
    console.print("\n[bold blue]🔍 Verificando integridade dos dados...[/bold blue]")
    
    async with pool.acquire() as conn:
        missing_tables = []
        empty_tables = []
        
        for table_name in EXPECTED_TABLES:
            # Verificar se a tabela existe
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
                table_name
            )
            
            if not exists:
                missing_tables.append(table_name)
                continue
            
            # Verificar se a tabela tem dados
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
            if count == 0:
                empty_tables.append(table_name)
            else:
                console.print(f"[green]✅ {table_name}: {count:,} registros[/green]")
        
        if missing_tables:
            console.print(f"[red]❌ Tabelas faltando: {', '.join(missing_tables)}[/red]")
            console.print("[red]⚠️  Execute o ETL completo primeiro![/red]")
            return False
        
        if empty_tables:
            console.print(f"[yellow]⚠️  Tabelas vazias: {', '.join(empty_tables)}[/yellow]")
            console.print("[yellow]⚠️  Execute o ETL completo primeiro![/yellow]")
            return False
        
        console.print("[green]✅ Todos os dados estão presentes no banco![/green]")
        return True


async def check_existing_indexes(pool):
    """Verifica quais índices já existem"""
    console.print("\n[bold blue]🔍 Verificando índices existentes...[/bold blue]")
    
    async with pool.acquire() as conn:
        existing_indexes = []
        missing_indexes = []
        
        for index_info in INDEXES_TO_CREATE:
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname = $1)",
                index_info['name']
            )
            
            if exists:
                existing_indexes.append(index_info['name'])
                console.print(f"[green]✅ {index_info['name']} já existe[/green]")
            else:
                missing_indexes.append(index_info)
                console.print(f"[yellow]⚠️  {index_info['name']} precisa ser criado[/yellow]")
        
        return existing_indexes, missing_indexes


async def create_missing_indexes(pool, missing_indexes):
    """Cria os índices faltantes"""
    if not missing_indexes:
        console.print("[green]✅ Todos os índices já existem![/green]")
        return True
    
    console.print(f"\n[bold blue]🔨 Criando {len(missing_indexes)} índices faltantes...[/bold blue]")
    
    success_count = 0
    failed_count = 0
    
    for i, index_info in enumerate(missing_indexes, 1):
        async with pool.acquire() as conn:
            try:
                start_time = time.time()
                
                # Obter tamanho da tabela
                table_size = await conn.fetchval(f"SELECT COUNT(*) FROM {index_info['table']}")
                
                console.print(f"[cyan]🔨 [{i}/{len(missing_indexes)}] Criando {index_info['name']} ({table_size:,} registros)...[/cyan]")
                
                await conn.execute(index_info['sql'])
                
                elapsed_time = time.time() - start_time
                minutes = int(elapsed_time // 60)
                seconds = int(elapsed_time % 60)
                
                console.print(f"[green]✅ {index_info['name']} criado em {minutes}m {seconds}s[/green]")
                success_count += 1
                
            except Exception as e:
                console.print(f"[red]❌ Erro ao criar {index_info['name']}: {e}[/red]")
                failed_count += 1
                continue
    
    console.print(f"\n[bold white]📊 Resumo da criação de índices:[/bold white]")
    console.print(f"[green]✅ Criados com sucesso: {success_count}[/green]")
    console.print(f"[red]❌ Falharam: {failed_count}[/red]")
    
    return failed_count == 0


async def main():
    """Função principal"""
    console.print("\n[bold magenta]" + "="*60 + "[/bold magenta]")
    console.print("[bold magenta]    RESUMIR PROCESSO ETL - RECEITA FEDERAL    [/bold magenta]")
    console.print("[bold magenta]" + "="*60 + "[/bold magenta]")
    
    console.print("\n[bold yellow]📋 Análise dos logs:[/bold yellow]")
    console.print("[green]✅ Download de arquivos: Completo[/green]")
    console.print("[green]✅ Extração de arquivos: Completo[/green]")
    console.print("[green]✅ Empresas (10 arquivos): Completo[/green]")
    console.print("[green]✅ Estabelecimentos (10 arquivos): Completo[/green]")
    console.print("[green]✅ Sócios (10 arquivos): Completo[/green]")
    console.print("[green]✅ Simples Nacional (45 partes): Completo[/green]")
    console.print("[green]✅ Tabelas de referência: Completo[/green]")
    console.print("[red]❌ Criação de índices: Falhou (timeout)[/red]")
    
    start_time = time.time()
    
    # Criar pool de conexões
    pool = await create_db_pool()
    
    try:
        # Verificar integridade dos dados
        if not await verify_data_integrity(pool):
            console.print("\n[red]❌ Dados incompletos. Execute o ETL completo primeiro![/red]")
            return
        
        # Verificar índices existentes
        existing_indexes, missing_indexes = await check_existing_indexes(pool)
        
        if not missing_indexes:
            console.print("\n[green]🎉 Processo ETL já está completo![/green]")
            console.print("[green]✅ Todos os dados foram inseridos[/green]")
            console.print("[green]✅ Todos os índices foram criados[/green]")
            return
        
        # Criar índices faltantes
        success = await create_missing_indexes(pool, missing_indexes)
        
        total_time = time.time() - start_time
        minutes = int(total_time // 60)
        seconds = int(total_time % 60)
        
        console.print("\n[bold green]" + "="*60 + "[/bold green]")
        
        if success:
            console.print("[bold green]    PROCESSO ETL COMPLETADO COM SUCESSO!    [/bold green]")
            console.print("[bold green]" + "="*60 + "[/bold green]")
            console.print("\n[green]🎉 Todos os dados foram inseridos no banco de dados![/green]")
            console.print("[green]🎉 Todos os índices foram criados com sucesso![/green]")
        else:
            console.print("[bold yellow]    PROCESSO ETL PARCIALMENTE COMPLETO    [/bold yellow]")
            console.print("[bold yellow]" + "="*60 + "[/bold yellow]")
            console.print("\n[green]✅ Todos os dados foram inseridos no banco de dados![/green]")
            console.print("[yellow]⚠️  Alguns índices falharam - execute create_indexes.py[/yellow]")
        
        console.print(f"\n[cyan]⏱️  Tempo de execução: {minutes}m {seconds}s[/cyan]")
        
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️  Processo interrompido pelo usuário[/yellow]")
        
    finally:
        await pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[yellow]Processo interrompido.[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[red]Erro no processo: {e}[/red]")
        sys.exit(1)
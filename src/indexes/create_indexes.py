#!/usr/bin/env python3
"""
Script para criar índices nas tabelas do banco de dados
Receita Federal CNPJ - Dados Públicos

Este script deve ser executado após o processo ETL principal
para criar os índices necessários que melhoram a performance das consultas.
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

# Lista de índices a serem criados
INDEXES = [
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


async def check_table_exists(pool, table_name):
    """Verifica se a tabela existe"""
    async with pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
            table_name
        )
        return result


async def get_table_size(pool, table_name):
    """Obtém o tamanho da tabela"""
    async with pool.acquire() as conn:
        try:
            result = await conn.fetchval(
                "SELECT COUNT(*) FROM " + table_name
            )
            return result
        except:
            return 0


async def check_index_exists(pool, index_name):
    """Verifica se o índice já existe"""
    async with pool.acquire() as conn:
        result = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname = $1)",
            index_name
        )
        return result


async def create_index(pool, index_info):
    """Cria um índice específico"""
    async with pool.acquire() as conn:
        try:
            start_time = time.time()
            
            # Verificar se a tabela existe
            if not await check_table_exists(pool, index_info['table']):
                console.print(f"[yellow]⚠️  Tabela {index_info['table']} não encontrada, pulando índice {index_info['name']}[/yellow]")
                return False
            
            # Verificar se o índice já existe
            if await check_index_exists(pool, index_info['name']):
                console.print(f"[blue]ℹ️  Índice {index_info['name']} já existe[/blue]")
                return True
            
            # Obter tamanho da tabela
            table_size = await get_table_size(pool, index_info['table'])
            
            console.print(f"[cyan]🔨 Criando índice {index_info['name']} na tabela {index_info['table']} ({table_size:,} registros)...[/cyan]")
            
            await conn.execute(index_info['sql'])
            
            elapsed_time = time.time() - start_time
            minutes = int(elapsed_time // 60)
            seconds = int(elapsed_time % 60)
            
            console.print(f"[green]✅ Índice {index_info['name']} criado com sucesso em {minutes}m {seconds}s[/green]")
            return True
            
        except Exception as e:
            console.print(f"[red]❌ Erro ao criar índice {index_info['name']}: {e}[/red]")
            return False


async def main():
    """Função principal"""
    console.print("\n[bold magenta]" + "="*60 + "[/bold magenta]")
    console.print("[bold magenta]    CRIAÇÃO DE ÍNDICES - RECEITA FEDERAL CNPJ    [/bold magenta]")
    console.print("[bold magenta]" + "="*60 + "[/bold magenta]\n")
    
    start_time = time.time()
    
    # Criar pool de conexões
    pool = await create_db_pool()
    
    try:
        created_count = 0
        failed_count = 0
        skipped_count = 0
        
        console.print(f"[bold blue]📋 Total de índices a serem criados: {len(INDEXES)}[/bold blue]\n")
        
        for i, index_info in enumerate(INDEXES, 1):
            console.print(f"[bold white]Progresso: {i}/{len(INDEXES)}[/bold white]")
            
            result = await create_index(pool, index_info)
            
            if result is True:
                created_count += 1
            elif result is False:
                failed_count += 1
            else:
                skipped_count += 1
                
            console.print()  # Linha em branco
        
        total_time = time.time() - start_time
        minutes = int(total_time // 60)
        seconds = int(total_time % 60)
        
        console.print("\n[bold green]" + "="*60 + "[/bold green]")
        console.print("[bold green]    CRIAÇÃO DE ÍNDICES CONCLUÍDA    [/bold green]")
        console.print("[bold green]" + "="*60 + "[/bold green]")
        
        console.print(f"\n[bold white]📊 Resumo:[/bold white]")
        console.print(f"[green]✅ Índices criados: {created_count}[/green]")
        console.print(f"[red]❌ Índices com erro: {failed_count}[/red]")
        console.print(f"[blue]ℹ️  Índices já existentes: {skipped_count}[/blue]")
        console.print(f"[cyan]⏱️  Tempo total: {minutes}m {seconds}s[/cyan]")
        
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
        console.print(f"\n[red]Erro no processo de criação de índices: {e}[/red]")
        sys.exit(1)
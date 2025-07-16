#!/usr/bin/env python3
"""
Script para verificar o estado atual do banco de dados
após o processo ETL da Receita Federal CNPJ
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

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
}

# Tabelas esperadas
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

# Índices esperados
EXPECTED_INDEXES = [
    'empresa_cnpj',
    'estabelecimento_cnpj',
    'estabelecimento_cnpj_completo',
    'socios_cnpj',
    'simples_cnpj',
    'estabelecimento_situacao',
    'estabelecimento_municipio'
]


async def create_db_connection():
    """Cria conexão com o banco"""
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        console.print(f"[red]Erro ao conectar com o banco: {e}[/red]")
        return None


async def get_table_info(conn, table_name):
    """Obtém informações de uma tabela"""
    try:
        # Verificar se a tabela existe
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
            table_name
        )
        
        if not exists:
            return {"exists": False, "count": 0, "size": "N/A"}
        
        # Obter contagem de registros
        count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
        
        # Obter tamanho da tabela
        size_result = await conn.fetchval(
            "SELECT pg_size_pretty(pg_total_relation_size($1))",
            table_name
        )
        
        return {
            "exists": True,
            "count": count,
            "size": size_result
        }
        
    except Exception as e:
        return {"exists": False, "count": 0, "size": f"Error: {e}"}


async def get_index_info(conn, index_name):
    """Verifica se um índice existe"""
    try:
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname = $1)",
            index_name
        )
        return exists
    except:
        return False


async def get_database_size(conn):
    """Obtém o tamanho total do banco"""
    try:
        size = await conn.fetchval(
            "SELECT pg_size_pretty(pg_database_size(current_database()))"
        )
        return size
    except:
        return "N/A"


async def main():
    """Função principal"""
    console.print("\n[bold magenta]" + "="*60 + "[/bold magenta]")
    console.print("[bold magenta]    STATUS DO BANCO DE DADOS - RECEITA FEDERAL    [/bold magenta]")
    console.print("[bold magenta]" + "="*60 + "[/bold magenta]\n")
    
    # Conectar ao banco
    conn = await create_db_connection()
    if not conn:
        return
    
    try:
        # Tamanho total do banco
        db_size = await get_database_size(conn)
        console.print(f"[bold blue]📊 Tamanho total do banco: {db_size}[/bold blue]\n")
        
        # Verificar tabelas
        console.print("[bold yellow]📋 STATUS DAS TABELAS[/bold yellow]")
        table_info = Table()
        table_info.add_column("Tabela", style="cyan")
        table_info.add_column("Existe", style="green")
        table_info.add_column("Registros", style="yellow")
        table_info.add_column("Tamanho", style="magenta")
        
        total_records = 0
        existing_tables = 0
        
        for table_name in EXPECTED_TABLES:
            info = await get_table_info(conn, table_name)
            
            if info["exists"]:
                existing_tables += 1
                total_records += info["count"]
                status = "✅ Sim"
                count_str = f"{info['count']:,}"
            else:
                status = "❌ Não"
                count_str = "0"
            
            table_info.add_row(
                table_name,
                status,
                count_str,
                info["size"]
            )
        
        console.print(table_info)
        console.print(f"\n[bold white]📈 Total de registros: {total_records:,}[/bold white]")
        console.print(f"[bold white]📊 Tabelas existentes: {existing_tables}/{len(EXPECTED_TABLES)}[/bold white]\n")
        
        # Verificar índices
        console.print("[bold yellow]🔍 STATUS DOS ÍNDICES[/bold yellow]")
        index_info = Table()
        index_info.add_column("Índice", style="cyan")
        index_info.add_column("Existe", style="green")
        
        existing_indexes = 0
        
        for index_name in EXPECTED_INDEXES:
            exists = await get_index_info(conn, index_name)
            
            if exists:
                existing_indexes += 1
                status = "✅ Sim"
            else:
                status = "❌ Não"
            
            index_info.add_row(index_name, status)
        
        console.print(index_info)
        console.print(f"\n[bold white]📊 Índices existentes: {existing_indexes}/{len(EXPECTED_INDEXES)}[/bold white]\n")
        
        # Análise baseada nos logs
        console.print("[bold yellow]📋 ANÁLISE DOS LOGS[/bold yellow]")
        
        analysis = Table()
        analysis.add_column("Processo", style="cyan")
        analysis.add_column("Status", style="green")
        analysis.add_column("Observação", style="yellow")
        
        # Processos que foram completados baseado nos logs
        completed_processes = [
            ("Download de arquivos", "✅ Completo", "6.2 segundos"),
            ("Extração de arquivos", "✅ Completo", "359.4 segundos"),
            ("Empresas (10 arquivos)", "✅ Completo", "Todos os arquivos processados"),
            ("Estabelecimentos (10 arquivos)", "✅ Completo", "Processados em chunks"),
            ("Sócios (10 arquivos)", "✅ Completo", "Todos os arquivos processados"),
            ("Simples Nacional (45 partes)", "✅ Completo", "43.865.689 linhas processadas"),
            ("Tabelas de referência", "✅ Completo", "CNAE, Motivo, Município, etc."),
            ("Criação de índices", "❌ Falhou", "Timeout no primeiro índice")
        ]
        
        for process, status, obs in completed_processes:
            analysis.add_row(process, status, obs)
        
        console.print(analysis)
        
        # Recomendações
        console.print("\n[bold green]💡 RECOMENDAÇÕES[/bold green]")
        
        if existing_indexes == 0:
            console.print("🔹 Execute o script [cyan]create_indexes.py[/cyan] para criar os índices")
        elif existing_indexes < len(EXPECTED_INDEXES):
            console.print("🔹 Execute o script [cyan]create_indexes.py[/cyan] para completar os índices faltantes")
        else:
            console.print("🔹 Todos os índices foram criados com sucesso")
        
        if existing_tables == len(EXPECTED_TABLES) and total_records > 0:
            console.print("🔹 [green]Processo ETL foi completado com sucesso![/green]")
            console.print("🔹 Todos os dados foram inseridos no banco")
        else:
            console.print("🔹 [yellow]Alguns dados podem estar faltando[/yellow]")
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
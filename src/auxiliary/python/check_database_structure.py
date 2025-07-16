#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import asyncpg
import os
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

# Load environment variables
load_dotenv()

console = Console()

async def check_database_structure():
    """
    Connect to PostgreSQL database and check the structure of the estabelecimento table,
    focusing on telephone/phone related columns.
    """
    
    # Get database connection parameters from environment
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    database = os.getenv('DB_NAME')
    
    console.print(f"[bold cyan]Connecting to database:[/bold cyan] {database}@{host}:{port}")
    
    try:
        # Create database connection
        conn = await asyncpg.connect(
            user=user,
            password=password,
            database=database,
            host=host,
            port=port
        )
        
        console.print("[green]✅ Successfully connected to database![/green]")
        
        # Check if estabelecimento table exists
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'estabelecimento'
            )
        """)
        
        if not table_exists:
            console.print("[red]❌ Table 'estabelecimento' does not exist![/red]")
            return
        
        console.print("[green]✅ Table 'estabelecimento' exists![/green]")
        
        # Get all columns from the estabelecimento table
        columns = await conn.fetch("""
            SELECT 
                column_name, 
                data_type, 
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns 
            WHERE table_name = 'estabelecimento' 
            ORDER BY ordinal_position
        """)
        
        console.print(f"\n[bold yellow]📋 All columns in 'estabelecimento' table ({len(columns)} columns):[/bold yellow]")
        
        # Create a table to display all columns
        table = Table(title="Estabelecimento Table Structure")
        table.add_column("Column Name", style="cyan")
        table.add_column("Data Type", style="magenta")
        table.add_column("Nullable", style="green")
        table.add_column("Default", style="yellow")
        table.add_column("Max Length", style="blue")
        
        for col in columns:
            table.add_row(
                col['column_name'],
                col['data_type'],
                col['is_nullable'],
                str(col['column_default']) if col['column_default'] else 'None',
                str(col['character_maximum_length']) if col['character_maximum_length'] else 'N/A'
            )
        
        console.print(table)
        
        # Focus on telephone/phone related columns
        phone_related_columns = []
        for col in columns:
            col_name = col['column_name'].lower()
            if any(keyword in col_name for keyword in ['tel', 'phone', 'fone', 'ddd', 'fax']):
                phone_related_columns.append(col)
        
        if phone_related_columns:
            console.print(f"\n[bold green]📞 Phone/Telephone related columns found ({len(phone_related_columns)}):[/bold green]")
            
            phone_table = Table(title="Phone/Telephone Related Columns")
            phone_table.add_column("Column Name", style="cyan")
            phone_table.add_column("Data Type", style="magenta")
            phone_table.add_column("Nullable", style="green")
            
            for col in phone_related_columns:
                phone_table.add_row(
                    col['column_name'],
                    col['data_type'],
                    col['is_nullable']
                )
            
            console.print(phone_table)
            
            # Show sample data for phone-related columns
            phone_column_names = [col['column_name'] for col in phone_related_columns]
            sample_query = f"""
                SELECT {', '.join(phone_column_names)}
                FROM estabelecimento 
                WHERE {' OR '.join([f"{col} IS NOT NULL AND {col} != ''" for col in phone_column_names])}
                LIMIT 10
            """
            
            console.print(f"\n[bold blue]📊 Sample data for phone-related columns:[/bold blue]")
            
            try:
                sample_data = await conn.fetch(sample_query)
                
                if sample_data:
                    sample_table = Table(title="Sample Phone Data")
                    for col_name in phone_column_names:
                        sample_table.add_column(col_name, style="cyan")
                    
                    for row in sample_data:
                        sample_table.add_row(*[str(row[col]) if row[col] is not None else 'NULL' for col in phone_column_names])
                    
                    console.print(sample_table)
                else:
                    console.print("[yellow]⚠️ No sample data found for phone-related columns[/yellow]")
                    
            except Exception as e:
                console.print(f"[red]❌ Error getting sample data: {e}[/red]")
        
        else:
            console.print("[yellow]⚠️ No phone/telephone related columns found[/yellow]")
        
        # Check for any columns that might have similar names
        console.print(f"\n[bold yellow]🔍 Looking for columns with similar names (telefone, phone, etc.):[/bold yellow]")
        
        similar_columns = []
        search_terms = ['telefone', 'phone', 'tel', 'fone', 'ddd', 'fax']
        
        for col in columns:
            col_name = col['column_name'].lower()
            for term in search_terms:
                if term in col_name:
                    similar_columns.append((col['column_name'], term))
                    break
        
        if similar_columns:
            similar_table = Table(title="Columns with Phone-Related Keywords")
            similar_table.add_column("Column Name", style="cyan")
            similar_table.add_column("Matched Keyword", style="magenta")
            
            for col_name, keyword in similar_columns:
                similar_table.add_row(col_name, keyword)
            
            console.print(similar_table)
        else:
            console.print("[yellow]⚠️ No columns found with phone-related keywords[/yellow]")
        
        # Get table statistics
        console.print(f"\n[bold blue]📊 Table statistics:[/bold blue]")
        row_count = await conn.fetchval("SELECT COUNT(*) FROM estabelecimento")
        console.print(f"Total rows in estabelecimento table: {row_count:,}")
        
        # Count non-null values for phone-related columns
        if phone_related_columns:
            console.print(f"\n[bold green]📈 Non-null counts for phone-related columns:[/bold green]")
            
            count_table = Table(title="Phone Columns Non-Null Counts")
            count_table.add_column("Column Name", style="cyan")
            count_table.add_column("Non-Null Count", style="magenta")
            count_table.add_column("Percentage", style="green")
            
            for col in phone_related_columns:
                col_name = col['column_name']
                count = await conn.fetchval(f"SELECT COUNT(*) FROM estabelecimento WHERE {col_name} IS NOT NULL AND {col_name} != ''")
                percentage = (count / row_count * 100) if row_count > 0 else 0
                count_table.add_row(col_name, f"{count:,}", f"{percentage:.2f}%")
            
            console.print(count_table)
        
        await conn.close()
        console.print("\n[green]✅ Database connection closed successfully[/green]")
        
    except Exception as e:
        console.print(f"[red]❌ Database connection error: {e}[/red]")
        console.print(f"[red]Make sure PostgreSQL is running and the database '{database}' exists[/red]")

if __name__ == "__main__":
    asyncio.run(check_database_structure())
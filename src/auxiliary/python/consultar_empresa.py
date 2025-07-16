#!/usr/bin/env python3
"""
Script para consultar dados completos de uma empresa por CNPJ
Receita Federal - Dados Públicos CNPJ
"""

import asyncio
import asyncpg
import json
import os
import sys
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.json import JSON

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


def format_cnpj(cnpj):
    """Formata CNPJ para exibição"""
    if len(cnpj) == 14:
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"
    elif len(cnpj) == 8:
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}"
    return cnpj


def validate_cnpj(cnpj):
    """Valida formato do CNPJ"""
    # Remove caracteres não numéricos
    cnpj_clean = ''.join(filter(str.isdigit, cnpj))
    
    if len(cnpj_clean) not in [8, 14]:
        return False, "CNPJ deve ter 8 (básico) ou 14 (completo) dígitos"
    
    return True, cnpj_clean


async def create_db_connection():
    """Cria conexão com o banco"""
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        console.print(f"[red]Erro ao conectar com o banco: {e}[/red]")
        return None


async def consultar_empresa_basico(conn, cnpj):
    """Consulta dados básicos da empresa"""
    cnpj_basico = cnpj[:8] if len(cnpj) == 14 else cnpj
    
    query = """
    SELECT 
        e.cnpj_basico,
        e.razao_social,
        e.natureza_juridica,
        nj.descricao as natureza_juridica_descricao,
        e.qualificacao_responsavel,
        qr.descricao as qualificacao_responsavel_descricao,
        e.capital_social,
        e.porte_empresa,
        CASE 
            WHEN e.porte_empresa = '01' THEN 'Microempresa'
            WHEN e.porte_empresa = '03' THEN 'Empresa de Pequeno Porte'
            WHEN e.porte_empresa = '05' THEN 'Demais'
            ELSE 'Não informado'
        END as porte_empresa_descricao,
        e.ente_federativo_responsavel
    FROM empresa e
    LEFT JOIN natureza nj ON e.natureza_juridica = nj.codigo
    LEFT JOIN qualificacao qr ON e.qualificacao_responsavel = qr.codigo
    WHERE e.cnpj_basico = $1
    """
    
    return await conn.fetchrow(query, cnpj_basico)


async def consultar_estabelecimentos(conn, cnpj):
    """Consulta estabelecimentos da empresa"""
    cnpj_basico = cnpj[:8] if len(cnpj) == 14 else cnpj
    
    query = """
    SELECT 
        est.cnpj_basico,
        est.cnpj_ordem,
        est.cnpj_dv,
        est.cnpj_basico || est.cnpj_ordem || est.cnpj_dv as cnpj_completo,
        CASE 
            WHEN est.cnpj_ordem = '0001' THEN 'MATRIZ'
            ELSE 'FILIAL'
        END as tipo_estabelecimento,
        est.nome_fantasia,
        est.situacao_cadastral,
        CASE 
            WHEN est.situacao_cadastral = '01' THEN 'NULA'
            WHEN est.situacao_cadastral = '02' THEN 'ATIVA'
            WHEN est.situacao_cadastral = '03' THEN 'SUSPENSA'
            WHEN est.situacao_cadastral = '04' THEN 'INAPTA'
            WHEN est.situacao_cadastral = '08' THEN 'BAIXADA'
            ELSE 'SITUAÇÃO DESCONHECIDA'
        END as situacao_cadastral_descricao,
        est.data_situacao_cadastral,
        est.motivo_situacao_cadastral,
        m.descricao as motivo_situacao_descricao,
        est.data_inicio_atividade,
        est.cnae_fiscal_principal,
        cnae_prin.descricao as cnae_principal_descricao,
        est.cnae_fiscal_secundaria,
        est.tipo_logradouro,
        est.logradouro,
        est.numero,
        est.complemento,
        est.bairro,
        est.cep,
        est.uf,
        est.municipio,
        mun.descricao as municipio_descricao,
        est.ddd_1,
        est.telefone_1,
        est.ddd_2,
        est.telefone_2,
        est.correio_eletronico
    FROM estabelecimento est
    LEFT JOIN motivo m ON est.motivo_situacao_cadastral = m.codigo
    LEFT JOIN cnae cnae_prin ON est.cnae_fiscal_principal = cnae_prin.codigo
    LEFT JOIN municipio mun ON est.municipio = mun.codigo
    WHERE est.cnpj_basico = $1
    ORDER BY est.cnpj_ordem
    """
    
    return await conn.fetch(query, cnpj_basico)


async def consultar_socios(conn, cnpj):
    """Consulta sócios da empresa"""
    cnpj_basico = cnpj[:8] if len(cnpj) == 14 else cnpj
    
    query = """
    SELECT 
        s.cnpj_basico,
        s.identificador_socio,
        CASE 
            WHEN s.identificador_socio = '1' THEN 'PESSOA JURÍDICA'
            WHEN s.identificador_socio = '2' THEN 'PESSOA FÍSICA'
            WHEN s.identificador_socio = '3' THEN 'ESTRANGEIRO'
            ELSE 'TIPO DESCONHECIDO'
        END as tipo_socio,
        s.nome_socio,
        s.cnpj_cpf_socio,
        s.qualificacao_socio,
        q.descricao as qualificacao_socio_descricao,
        s.data_entrada_sociedade,
        s.pais,
        p.descricao as pais_descricao,
        s.faixa_etaria,
        CASE 
            WHEN s.faixa_etaria = '1' THEN '0 a 12 anos'
            WHEN s.faixa_etaria = '2' THEN '13 a 20 anos'
            WHEN s.faixa_etaria = '3' THEN '21 a 30 anos'
            WHEN s.faixa_etaria = '4' THEN '31 a 40 anos'
            WHEN s.faixa_etaria = '5' THEN '41 a 50 anos'
            WHEN s.faixa_etaria = '6' THEN '51 a 80 anos'
            WHEN s.faixa_etaria = '8' THEN 'Maior de 80 anos'
            WHEN s.faixa_etaria = '0' THEN 'Não se aplica'
            ELSE 'Não informado'
        END as faixa_etaria_descricao
    FROM socios s
    LEFT JOIN qualificacao q ON s.qualificacao_socio = q.codigo
    LEFT JOIN pais p ON s.pais = p.codigo
    WHERE s.cnpj_basico = $1
    ORDER BY s.nome_socio
    """
    
    return await conn.fetch(query, cnpj_basico)


async def consultar_simples(conn, cnpj):
    """Consulta informações do Simples Nacional"""
    cnpj_basico = cnpj[:8] if len(cnpj) == 14 else cnpj
    
    query = """
    SELECT 
        sim.cnpj_basico,
        sim.opcao_pelo_simples,
        CASE 
            WHEN sim.opcao_pelo_simples = 'S' THEN 'SIM'
            WHEN sim.opcao_pelo_simples = 'N' THEN 'NÃO'
            ELSE 'NÃO INFORMADO'
        END as opcao_simples_descricao,
        sim.data_opcao_simples,
        sim.data_exclusao_simples,
        sim.opcao_mei,
        CASE 
            WHEN sim.opcao_mei = 'S' THEN 'SIM'
            WHEN sim.opcao_mei = 'N' THEN 'NÃO'
            ELSE 'NÃO INFORMADO'
        END as opcao_mei_descricao,
        sim.data_opcao_mei,
        sim.data_exclusao_mei
    FROM simples sim
    WHERE sim.cnpj_basico = $1
    """
    
    return await conn.fetchrow(query, cnpj_basico)


def exibir_empresa_basico(empresa):
    """Exibe dados básicos da empresa"""
    if not empresa:
        console.print("[red]❌ Empresa não encontrada[/red]")
        return
    
    console.print(f"\n[bold blue]📊 DADOS DA EMPRESA[/bold blue]")
    
    table = Table(show_header=False, box=None)
    table.add_column("Campo", style="cyan")
    table.add_column("Valor", style="white")
    
    table.add_row("CNPJ Básico", format_cnpj(empresa['cnpj_basico']))
    table.add_row("Razão Social", empresa['razao_social'] or 'Não informado')
    table.add_row("Natureza Jurídica", f"{empresa['natureza_juridica']} - {empresa['natureza_juridica_descricao'] or 'Não informado'}")
    table.add_row("Porte", empresa['porte_empresa_descricao'])
    table.add_row("Capital Social", f"R$ {float(empresa['capital_social'] or 0):,.2f}")
    table.add_row("Responsável", empresa['qualificacao_responsavel_descricao'] or 'Não informado')
    
    console.print(table)


async def processar_cnaes_secundarios(conn, cnae_secundaria_str):
    """Processa string de CNAEs secundários e retorna descrições"""
    if not cnae_secundaria_str:
        return []
    
    # CNAEs secundários são separados por vírgula
    cnaes_secundarios = [cnae.strip() for cnae in cnae_secundaria_str.split(',') if cnae.strip()]
    
    if not cnaes_secundarios:
        return []
    
    # Converter para inteiros
    try:
        cnaes_int = [int(cnae) for cnae in cnaes_secundarios if cnae.isdigit()]
    except ValueError:
        return []
    
    if not cnaes_int:
        return []
    
    # Buscar descrições dos CNAEs
    query = """
    SELECT codigo, descricao 
    FROM cnae 
    WHERE codigo = ANY($1::integer[])
    ORDER BY codigo
    """
    
    result = await conn.fetch(query, cnaes_int)
    return [(str(row['codigo']), row['descricao']) for row in result]


def exibir_estabelecimentos(estabelecimentos):
    """Exibe estabelecimentos da empresa"""
    if not estabelecimentos:
        console.print("[yellow]⚠️  Nenhum estabelecimento encontrado[/yellow]")
        return
    
    console.print(f"\n[bold blue]🏢 ESTABELECIMENTOS ({len(estabelecimentos)})[/bold blue]")
    
    for est in estabelecimentos:
        # Construir endereço
        endereco_parts = []
        if est['tipo_logradouro']: endereco_parts.append(est['tipo_logradouro'])
        if est['logradouro']: endereco_parts.append(est['logradouro'])
        if est['numero']: endereco_parts.append(f"nº {est['numero']}")
        if est['complemento']: endereco_parts.append(est['complemento'])
        if est['bairro']: endereco_parts.append(f"- {est['bairro']}")
        if est['municipio_descricao']: endereco_parts.append(f"- {est['municipio_descricao']}")
        if est['uf']: endereco_parts.append(f"/{est['uf']}")
        if est['cep']: endereco_parts.append(f"CEP: {est['cep']}")
        
        endereco = ' '.join(endereco_parts) if endereco_parts else 'Não informado'
        
        # Construir telefone
        telefone = ""
        if est['ddd_1'] and est['telefone_1']:
            telefone = f"({est['ddd_1']}) {est['telefone_1']}"
        
        # Formatação do CNAE principal
        cnae_principal = f"{est['cnae_fiscal_principal']} - {est['cnae_principal_descricao']}" if est['cnae_fiscal_principal'] and est['cnae_principal_descricao'] else 'Não informado'
        
        # Formatação dos CNAEs secundários
        cnaes_secundarios_text = "Não informado"
        if est.get('cnaes_secundarios'):
            cnaes_list = []
            for codigo, descricao in est['cnaes_secundarios']:
                cnaes_list.append(f"{codigo} - {descricao}")
            if cnaes_list:
                cnaes_secundarios_text = '\n                    '.join(cnaes_list)
        
        panel_content = f"""
[bold]CNPJ:[/bold] {format_cnpj(est['cnpj_completo'])}
[bold]Tipo:[/bold] {est['tipo_estabelecimento']}
[bold]Nome Fantasia:[/bold] {est['nome_fantasia'] or 'Não informado'}
[bold]Situação:[/bold] {est['situacao_cadastral_descricao']} ({str(est['data_situacao_cadastral']) if est['data_situacao_cadastral'] else 'Não informado'})
[bold]CNAE Principal:[/bold] {cnae_principal}
[bold]CNAEs Secundários:[/bold] {cnaes_secundarios_text}
[bold]Início Atividade:[/bold] {str(est['data_inicio_atividade']) if est['data_inicio_atividade'] else 'Não informado'}
[bold]Endereço:[/bold] {endereco}
[bold]Telefone:[/bold] {telefone or 'Não informado'}
[bold]E-mail:[/bold] {est['correio_eletronico'] or 'Não informado'}
"""
        
        console.print(Panel(panel_content, title=f"{est['tipo_estabelecimento']} - {est['cnpj_completo']}"))


def exibir_socios(socios):
    """Exibe sócios da empresa"""
    if not socios:
        console.print("[yellow]⚠️  Nenhum sócio encontrado[/yellow]")
        return
    
    console.print(f"\n[bold blue]👥 SÓCIOS ({len(socios)})[/bold blue]")
    
    table = Table()
    table.add_column("Nome", style="cyan")
    table.add_column("Tipo", style="yellow")
    table.add_column("Qualificação", style="green")
    table.add_column("Entrada", style="magenta")
    table.add_column("Faixa Etária", style="white")
    
    for socio in socios:
        table.add_row(
            socio['nome_socio'] or 'Não informado',
            socio['tipo_socio'],
            socio['qualificacao_socio_descricao'] or 'Não informado',
            str(socio['data_entrada_sociedade']) if socio['data_entrada_sociedade'] else 'Não informado',
            socio['faixa_etaria_descricao'] or 'Não informado'
        )
    
    console.print(table)


def exibir_simples(simples):
    """Exibe informações do Simples Nacional"""
    if not simples:
        console.print("[yellow]⚠️  Sem informações do Simples Nacional[/yellow]")
        return
    
    console.print(f"\n[bold blue]📋 SIMPLES NACIONAL[/bold blue]")
    
    table = Table(show_header=False, box=None)
    table.add_column("Campo", style="cyan")
    table.add_column("Valor", style="white")
    
    table.add_row("Optante Simples", simples['opcao_simples_descricao'])
    table.add_row("Data Opção Simples", str(simples['data_opcao_simples']) if simples['data_opcao_simples'] else 'Não informado')
    table.add_row("Data Exclusão Simples", str(simples['data_exclusao_simples']) if simples['data_exclusao_simples'] else 'Não informado')
    table.add_row("Optante MEI", simples['opcao_mei_descricao'])
    table.add_row("Data Opção MEI", str(simples['data_opcao_mei']) if simples['data_opcao_mei'] else 'Não informado')
    table.add_row("Data Exclusão MEI", str(simples['data_exclusao_mei']) if simples['data_exclusao_mei'] else 'Não informado')
    
    console.print(table)


async def main():
    """Função principal"""
    if len(sys.argv) < 2:
        console.print("[red]Uso: python consultar_empresa.py <CNPJ>[/red]")
        console.print("[yellow]Exemplo: python consultar_empresa.py 11222333000181[/yellow]")
        console.print("[yellow]Exemplo: python consultar_empresa.py 11222333[/yellow]")
        sys.exit(1)
    
    cnpj_input = sys.argv[1]
    
    # Validar CNPJ
    is_valid, cnpj_clean = validate_cnpj(cnpj_input)
    if not is_valid:
        console.print(f"[red]❌ {cnpj_clean}[/red]")
        sys.exit(1)
    
    console.print(f"\n[bold magenta]🔍 CONSULTANDO EMPRESA: {format_cnpj(cnpj_clean)}[/bold magenta]")
    
    # Conectar ao banco
    conn = await create_db_connection()
    if not conn:
        sys.exit(1)
    
    try:
        # Buscar dados
        empresa = await consultar_empresa_basico(conn, cnpj_clean)
        estabelecimentos = await consultar_estabelecimentos(conn, cnpj_clean)
        socios = await consultar_socios(conn, cnpj_clean)
        simples = await consultar_simples(conn, cnpj_clean)
        
        # Processar CNAEs secundários para cada estabelecimento
        estabelecimentos_processados = []
        for est in estabelecimentos:
            est_dict = dict(est)
            est_dict['cnaes_secundarios'] = await processar_cnaes_secundarios(conn, est['cnae_fiscal_secundaria'])
            estabelecimentos_processados.append(est_dict)
        
        estabelecimentos = estabelecimentos_processados
        
        # Exibir resultados
        exibir_empresa_basico(empresa)
        exibir_estabelecimentos(estabelecimentos)
        exibir_socios(socios)
        exibir_simples(simples)
        
        if not empresa:
            console.print("\n[red]❌ Empresa não encontrada na base de dados[/red]")
            sys.exit(1)
        
        console.print("\n[green]✅ Consulta realizada com sucesso![/green]")
        
    except Exception as e:
        console.print(f"\n[red]❌ Erro na consulta: {e}[/red]")
        sys.exit(1)
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
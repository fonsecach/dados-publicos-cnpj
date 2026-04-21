# -*- coding: utf-8 -*-
import argparse
import asyncio
import concurrent.futures
import datetime
import gc
import json
import logging
import os
import pathlib
import re
import sys
import time
import zipfile

import asyncpg
import bs4 as bs
import httpx
import pandas as pd
from dotenv import load_dotenv
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from tenacity import retry, stop_after_attempt, wait_exponential

# Configuração de logging e console
console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        RichHandler(console=console, rich_tracebacks=True),
        logging.FileHandler("etl_log.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def check_diff(url, file_name):
    """
    Verifica se o arquivo no servidor existe no disco e se ele tem o mesmo
    tamanho no servidor.
    """
    if not os.path.isfile(file_name):
        return True  # ainda nao foi baixado

    import ssl

    try:
        # Configurar SSL mais permissivo
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        with httpx.Client(
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            },
            timeout=30.0,
            verify=ssl_context,
            follow_redirects=True,
        ) as client:
            response = client.head(url)
            new_size = int(response.headers.get("content-length", 0))
            old_size = os.path.getsize(file_name)
            if new_size != old_size:
                os.remove(file_name)
                return True  # tamanho diferentes
    except Exception as e:
        logger.warning(
            f"Erro ao verificar arquivo {url}: {e}. Assumindo que precisa baixar."
        )
        return True

    return False  # arquivos sao iguais


def makedirs(path):
    """
    cria path caso seja necessario
    """
    if not os.path.exists(path):
        os.makedirs(path)


CHECKPOINT_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "checkpoint.json")


def save_checkpoint(stage, file_index=None, batch_index=None):
    """
    Salva checkpoint do progresso atual
    """
    checkpoint = {
        "stage": stage,
        "file_index": file_index,
        "batch_index": batch_index,
        "timestamp": datetime.datetime.now().isoformat(),
    }

    try:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint, f, indent=2)
        logger.info(f"Checkpoint salvo: {stage} - arquivo {file_index}")
    except Exception as e:
        logger.error(f"Erro ao salvar checkpoint: {e}")


def load_checkpoint():
    """
    Carrega checkpoint salvo
    """
    try:
        if os.path.exists(CHECKPOINT_FILE):
            with open(CHECKPOINT_FILE, "r") as f:
                checkpoint = json.load(f)
            logger.info(
                f"Checkpoint carregado: {checkpoint['stage']} - arquivo {checkpoint.get('file_index', 0)}"
            )
            return checkpoint
    except Exception as e:
        logger.error(f"Erro ao carregar checkpoint: {e}")

    return None


def clear_checkpoint():
    """
    Remove checkpoint após conclusão
    """
    try:
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
            logger.info("Checkpoint removido após conclusão")
    except Exception as e:
        logger.error(f"Erro ao remover checkpoint: {e}")


async def to_sql_async(dataframe, pool, table_name, batch_size=2600):
    """
    Insere dados de forma assíncrona usando batch inserts otimizados
    """
    from datetime import date, datetime

    total = len(dataframe)
    columns = list(dataframe.columns)

    # Definir colunas de data para cada tabela
    date_columns = {
        "estabelecimento": [
            "data_situacao_cadastral",
            "data_inicio_atividade",
            "data_situacao_especial",
        ],
        "socios": ["data_entrada_sociedade"],
        "simples": [
            "data_opcao_simples",
            "data_exclusao_simples",
            "data_opcao_mei",
            "data_exclusao_mei",
        ],
    }

    def convert_date_string(date_str):
        """Converte string de data YYYYMMDD para objeto date"""
        if (
            pd.isna(date_str)
            or date_str == ""
            or date_str == "00000000"
            or date_str == "0"
        ):
            return None
        try:
            if isinstance(date_str, str) and len(date_str) == 8 and date_str.isdigit():
                return datetime.strptime(date_str, "%Y%m%d").date()
            return None if isinstance(date_str, str) else date_str
        except (ValueError, TypeError):
            return None

    # Converter DataFrame para lista de tuplas e substituir NaN por None
    import numpy as np

    records = []
    for row in dataframe.values:
        clean_row = []
        for i, val in enumerate(row):
            if pd.isna(val):
                clean_row.append(None)
            elif (
                table_name in date_columns
                and i < len(columns)
                and columns[i] in date_columns[table_name]
            ):
                # Converter colunas de data
                clean_row.append(convert_date_string(val))
            else:
                clean_row.append(val)
        records.append(tuple(clean_row))

    # Criar statement de insert
    placeholders = ",".join(["$" + str(i + 1) for i in range(len(columns))])
    insert_sql = (
        f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
    )

    async def insert_batch(batch_records, batch_num):
        async with pool.acquire() as conn:
            await conn.executemany(insert_sql, batch_records, timeout=500)

        # Progress tracking
        completed = batch_num * batch_size
        percent = min((completed * 100) / total, 100)
        progress = f"{table_name} {percent:.2f}% {completed:0{len(str(total))}}/{total}"
        sys.stdout.write(f"\r{progress}")
        sys.stdout.flush()

    # Processar em batches
    tasks = []
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        task = insert_batch(batch, i // batch_size)
        tasks.append(task)

    # Executar todos os batches em paralelo (limitando concorrência)
    semaphore = asyncio.Semaphore(10)  # Máximo 10 conexões simultâneas

    async def bounded_task(task):
        async with semaphore:
            await task

    await asyncio.gather(*[bounded_task(task) for task in tasks])
    sys.stdout.write("\n")


def getEnv(env, default=None):
    return os.getenv(env, default)


# Carregar variáveis de ambiente automaticamente
current_path = pathlib.Path().resolve()

# Procurar .env primeiro no diretório raiz do projeto (pai do src)
parent_path = current_path.parent
dotenv_path = os.path.join(parent_path, ".env")

# Se não encontrar no diretório pai, verificar no diretório atual
if not os.path.isfile(dotenv_path):
    dotenv_path = os.path.join(current_path, ".env")

    # Se não encontrar em lugar nenhum, usar o do diretório pai (raiz do projeto)
    if not os.path.isfile(dotenv_path):
        dotenv_path = os.path.join(parent_path, ".env")
        print(
            "Arquivo .env não encontrado. Verifique se existe um arquivo .env no diretório raiz do projeto."
        )
        print(f"Procurando em: {dotenv_path}")

print(f"Carregando configurações de: {dotenv_path}")
load_dotenv(dotenv_path=dotenv_path)


def parse_arguments():
    """Parseia argumentos de linha de comando"""
    parser = argparse.ArgumentParser(
        description="ETL de Dados Públicos do CNPJ da Receita Federal",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Modo interativo (padrão)
  python ETL_dados_publicos_empresas.py

  # Baixar a versão mais recente disponível
  python ETL_dados_publicos_empresas.py --last

  # Baixar uma versão específica (formato MM-AAAA)
  python ETL_dados_publicos_empresas.py 01-2025
  python ETL_dados_publicos_empresas.py 12-2024
        """,
    )

    parser.add_argument(
        "date", nargs="?", help="Data no formato MM-AAAA (exemplo: 01-2025)"
    )

    parser.add_argument(
        "--last",
        action="store_true",
        help="Baixar a versão mais recente disponível na Receita Federal",
    )

    return parser.parse_args()


SHARE_TOKEN = "YggdBLfdninEJX9"
WEBDAV_BASE_URL = f"https://arquivos.receitafederal.gov.br/public.php/webdav"


def webdav_list(path="/"):
    """Lista entradas de um diretório via API WebDAV do Nextcloud."""
    import base64
    import ssl
    import xml.etree.ElementTree as ET

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    credentials = base64.b64encode(f"{SHARE_TOKEN}:".encode()).decode()
    url = WEBDAV_BASE_URL + path

    with httpx.Client(
        headers={
            "Authorization": f"Basic {credentials}",
            "Depth": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        },
        timeout=30.0,
        verify=ssl_context,
        follow_redirects=True,
    ) as client:
        response = client.request(
            "PROPFIND",
            url,
            content='<?xml version="1.0"?><d:propfind xmlns:d="DAV:"><d:prop><d:displayname/></d:prop></d:propfind>',
        )
        response.raise_for_status()

    root = ET.fromstring(response.content)
    ns = {"d": "DAV:"}
    entries = []
    for resp in root.findall("d:response", ns):
        href = resp.find("d:href", ns)
        if href is None:
            continue
        name = href.text.rstrip("/").split("/")[-1]
        # Ignorar a própria entrada do diretório pai
        parent = path.strip("/")
        if name and name != parent and name != SHARE_TOKEN:
            entries.append(name)
    return entries


def get_latest_available_date():
    """
    Detecta a versão mais recente disponível na Receita Federal via WebDAV.
    """
    console.print(
        "[yellow]🔍 Detectando a versão mais recente disponível na Receita Federal...[/yellow]"
    )

    try:
        entries = webdav_list("/")
        matches = [e for e in entries if re.match(r"^\d{4}-\d{2}$", e)]

        if not matches:
            raise ValueError("Nenhuma versão encontrada na página da Receita Federal")

        matches.sort(reverse=True)
        latest = matches[0]

        year, month = latest.split("-")
        year = int(year)
        month = int(month)

        console.print(
            f"[green]✅ Versão mais recente encontrada: {month:02d}-{year}[/green]"
        )
        return year, month

    except Exception as e:
        logger.error(f"Erro ao detectar versão mais recente: {e}")
        console.print(f"[red]❌ Erro ao detectar versão mais recente: {e}[/red]")
        console.print("[yellow]⚠️  Usando mês anterior como fallback[/yellow]")

        now = datetime.datetime.now()
        if now.month == 1:
            return now.year - 1, 12
        return now.year, now.month - 1


def parse_date_string(date_str):
    """
    Parseia string de data no formato MM-AAAA
    Retorna tupla (ano, mes)
    """
    try:
        # Validar formato
        if not re.match(r"^\d{2}-\d{4}$", date_str):
            raise ValueError(f"Formato inválido. Use MM-AAAA (exemplo: 01-2025)")

        parts = date_str.split("-")
        month = int(parts[0])
        year = int(parts[1])

        # Validar valores
        current_year = datetime.datetime.now().year

        if year < 2019 or year > current_year + 1:
            raise ValueError(f"Ano deve estar entre 2019 e {current_year + 1}")

        if month < 1 or month > 12:
            raise ValueError("Mês deve estar entre 01 e 12")

        return year, month

    except ValueError as e:
        console.print(f"[red]❌ Erro no formato da data: {e}[/red]")
        sys.exit(1)


# Solicitar ano e mês do usuário
def get_year_month(args=None):
    """
    Determina ano e mês baseado nos argumentos de linha de comando
    ou solicita interativamente ao usuário
    """
    # Se argumentos não foram fornecidos, modo interativo padrão
    if args is None:
        console.print("[blue]Modo: Interativo[/blue]")
        current_year = datetime.datetime.now().year
        current_month = datetime.datetime.now().month

        print("\n" + "=" * 50)
        print("CONFIGURAÇÃO DE ANO E MÊS PARA DOWNLOAD DOS DADOS")
        print("=" * 50)

        while True:
            try:
                year = input(f"Digite o ano (exemplo: {current_year}): ").strip()
                if not year:
                    year = current_year
                else:
                    year = int(year)

                if year < 2019 or year > current_year:
                    print(f"❌ Ano deve estar entre 2019 e {current_year}")
                    continue
                break
            except ValueError:
                print("❌ Por favor, digite um ano válido")

        while True:
            try:
                month = input(
                    f"Digite o mês (1-12, exemplo: {current_month}): "
                ).strip()
                if not month:
                    month = current_month
                else:
                    month = int(month)

                if month < 1 or month > 12:
                    print("❌ Mês deve estar entre 1 e 12")
                    continue
                break
            except ValueError:
                print("❌ Por favor, digite um mês válido")

        return year, month

    # Se --last foi especificado, buscar a versão mais recente
    if args.last:
        console.print("[blue]Modo: Baixar versão mais recente[/blue]")
        return get_latest_available_date()

    # Se uma data foi fornecida como argumento
    if args.date:
        console.print(f"[blue]Modo: Data especificada ({args.date})[/blue]")
        return parse_date_string(args.date)

    # Modo interativo (padrão)
    return get_year_month(args=None)


# Parsear argumentos de linha de comando
args = parse_arguments()

# Obter ano e mês do usuário (via argumentos ou interativo)
ano, mes = get_year_month(args)
mes_formatado = f"{mes:02d}"  # Formatar mês com 2 dígitos

print(f"\n✅ Configurado para baixar dados de: {ano}-{mes_formatado}")
print("=" * 50)

# URL base do compartilhamento Nextcloud da Receita Federal
SHARE_BASE_URL = f"https://arquivos.receitafederal.gov.br/index.php/s/{SHARE_TOKEN}"

# Read details from ".env" file:
output_files = None
extracted_files = None
try:
    output_files = getEnv("OUTPUT_FILES_PATH")
    makedirs(output_files)

    extracted_files = getEnv("EXTRACTED_FILES_PATH")
    makedirs(extracted_files)

    print(
        "Diretórios definidos: \n"
        + "output_files: "
        + str(output_files)
        + "\n"
        + "extracted_files: "
        + str(extracted_files)
    )
except:
    pass
    logger.error(
        'Erro na definição dos diretórios, verifique o arquivo ".env" ou o local informado do seu arquivo de configuração.'
    )


# Fazer request com httpx com tratamento de erros robusto
def get_html_with_retry(url, max_retries=3):
    """Faz request com retry e tratamento de erros SSL/TLS"""
    import ssl

    for attempt in range(max_retries):
        try:
            # Configurar cliente com timeout e configurações SSL mais flexíveis
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            with httpx.Client(
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                },
                timeout=30.0,
                verify=ssl_context,
                follow_redirects=True,
            ) as client:
                response = client.get(url)
                response.raise_for_status()
                return response.content

        except (httpx.ConnectError, httpx.TimeoutException, ssl.SSLError) as e:
            logger.warning(f"Tentativa {attempt + 1}/{max_retries} falhou: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(2**attempt)  # Backoff exponencial
        except Exception as e:
            logger.error(f"Erro inesperado ao acessar {url}: {e}")
            raise


try:
    all_entries = webdav_list(f"/{ano}-{mes_formatado}")
    Files = [e for e in all_entries if e.lower().endswith(".zip")]
except Exception as e:
    logger.error(f"Erro fatal ao listar arquivos via WebDAV: {e}")
    print(f"\n❌ Erro ao listar arquivos da Receita Federal via WebDAV.")
    print(f"Diretório tentado: /{ano}-{mes_formatado}")
    print(f"Erro: {e}")
    print("\n🔍 Verificações:")
    print("1. Confirme se o ano/mês estão corretos")
    print("2. Verifique sua conexão com a internet")
    print("3. Tente novamente em alguns minutos")
    sys.exit(1)

print("Arquivos que serão baixados:")
for l in Files:
    print(l)

# Separar arquivos por tipo
Items = [name for name in os.listdir(extracted_files) if name.endswith("")]

arquivos_empresa = []
arquivos_estabelecimento = []
arquivos_socios = []
arquivos_simples = []
arquivos_cnae = []
arquivos_moti = []
arquivos_munic = []
arquivos_natju = []
arquivos_pais = []
arquivos_quals = []

for i in range(0, len(Items)):
    name_upper = Items[i].upper()
    if "EMPRECSV" in name_upper or "EMPRESAS" in name_upper:
        arquivos_empresa.append(Items[i])
    elif "ESTABELE" in name_upper or "ESTABELECIMENTOS" in name_upper:
        arquivos_estabelecimento.append(Items[i])
    elif "SOCIOCSV" in name_upper or "SOCIOS" in name_upper:
        arquivos_socios.append(Items[i])
    elif "SIMPLES" in name_upper:
        arquivos_simples.append(Items[i])
    elif "CNAECSV" in name_upper or "CNAES" in name_upper:
        arquivos_cnae.append(Items[i])
    elif "MOTICSV" in name_upper or "MOTIVOS" in name_upper:
        arquivos_moti.append(Items[i])
    elif "MUNICCSV" in name_upper or "MUNICIPIOS" in name_upper:
        arquivos_munic.append(Items[i])
    elif "NATJUCSV" in name_upper or "NATUREZAS" in name_upper:
        arquivos_natju.append(Items[i])
    elif "PAISCSV" in name_upper or "PAISES" in name_upper:
        arquivos_pais.append(Items[i])
    elif "QUALSCSV" in name_upper or "QUALIFICACOES" in name_upper:
        arquivos_quals.append(Items[i])
    else:
        pass


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
async def download_file_async(url, file_path, semaphore):
    """Download file asynchronously with retry logic using httpx"""
    import ssl

    async with semaphore:  # Limita downloads simultâneos
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        # Configurar SSL mais permissivo para downloads
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        try:
            async with httpx.AsyncClient(
                headers=headers,
                timeout=120.0,  # Timeout maior para downloads grandes
                verify=ssl_context,
                follow_redirects=True,
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
            ) as client:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()

                    total_size = int(response.headers.get("content-length", 0))
                    downloaded = 0

                    with open(file_path, "wb") as f:
                        async for chunk in response.aiter_bytes(
                            chunk_size=65536
                        ):  # Chunks maiores para melhor performance
                            f.write(chunk)
                            downloaded += len(chunk)

                            if total_size > 0:
                                percent = (downloaded / total_size) * 100
                                file_name = os.path.basename(file_path)
                                sys.stdout.write(
                                    f"\r{file_name}: {percent:.1f}% [{downloaded:,}/{total_size:,}] bytes"
                                )
                                sys.stdout.flush()

                    print(f"\n{os.path.basename(file_path)} baixado com sucesso!")

        except (httpx.ConnectError, httpx.TimeoutException, ssl.SSLError) as e:
            logger.error(
                f"Erro de conexão ao baixar {os.path.basename(file_path)}: {e}"
            )
            raise
        except Exception as e:
            logger.error(
                f"Erro inesperado ao baixar {os.path.basename(file_path)}: {e}"
            )
            raise


async def download_all_files():
    """Download todos os arquivos em paralelo de forma assíncrona"""
    print(f"Iniciando download de {len(Files)} arquivos em paralelo...")

    # Semáforo para limitar downloads simultâneos (evita sobrecarregar o servidor)
    download_semaphore = asyncio.Semaphore(3)  # Máximo 3 downloads simultâneos

    async def download_single_file(file_name):
        url = f"{SHARE_BASE_URL}/download?path=/{ano}-{mes_formatado}&files={file_name}"
        file_path = os.path.join(output_files, file_name)

        if check_diff(url, file_path):
            try:
                await download_file_async(url, file_path, download_semaphore)
            except Exception as e:
                logger.error(f"Erro ao baixar {file_name}: {e}")
                return False
        else:
            print(f"{file_name} já existe e está atualizado.")
        return True

    # Executar downloads em paralelo
    tasks = [download_single_file(file_name) for file_name in Files]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful = sum(1 for r in results if r is True)
    print(f"\nDownloads concluídos: {successful}/{len(Files)} arquivos")


async def extract_all_files():
    """Extrai todos os arquivos ZIP em paralelo usando threading"""
    print(f"Iniciando extração de {len(Files)} arquivos...")

    def extract_single_file(file_name):
        try:
            full_path = os.path.join(output_files, file_name)
            if not os.path.exists(full_path):
                return f"Arquivo {file_name} não encontrado"

            # Verificar integridade do arquivo ZIP
            try:
                with zipfile.ZipFile(full_path, "r") as zip_ref:
                    # Testar integridade do arquivo
                    bad_file = zip_ref.testzip()
                    if bad_file:
                        return f"✗ Arquivo corrompido {file_name}: {bad_file} inválido"

                    # Extrair arquivos
                    zip_ref.extractall(extracted_files)
                return f"✓ {file_name} extraído"
            except zipfile.BadZipFile:
                return f"✗ Arquivo corrompido {file_name}: não é um ZIP válido"
        except Exception as e:
            return f"✗ Erro ao extrair {file_name}: {e}"

    # Usar ThreadPoolExecutor para extração paralela (I/O bound)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        # Submeter todas as tarefas
        future_to_file = {
            executor.submit(extract_single_file, file_name): file_name
            for file_name in Files
        }

        # Processar resultados conforme completam
        for future in concurrent.futures.as_completed(future_to_file):
            result = future.result()
            print(result)

    print("Extração concluída!")


async def create_database_if_not_exists():
    """
    Cria o banco de dados se não existir
    """
    user = getEnv("DB_USER")
    passw = getEnv("DB_PASSWORD")
    host = getEnv("DB_HOST")
    port = getEnv("DB_PORT")
    database = getEnv("DB_NAME")
    ssl_mode = getEnv("DB_SSL_MODE", "disable")

    # Converter string SSL mode para valor booleano/None esperado pelo asyncpg
    ssl_config = None
    if ssl_mode.lower() in ["disable", "false"]:
        ssl_config = False
    elif ssl_mode.lower() in ["require", "true"]:
        ssl_config = True
    else:
        ssl_config = "prefer"  # padrão do asyncpg

    # Conectar ao banco padrão postgres para criar o banco se necessário
    try:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                conn = await asyncpg.connect(
                    user=user,
                    password=passw,
                    database="postgres",
                    host=host,
                    port=port,
                    ssl=ssl_config,
                    timeout=30,
                )
                break
            except (
                ConnectionResetError,
                asyncpg.exceptions.ConnectionDoesNotExistError,
            ) as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(
                    f"Tentativa {attempt + 1}/{max_retries} falhou: {e}. Tentando novamente em 2s..."
                )
                await asyncio.sleep(2)

        # Verificar se o banco existe
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", database
        )

        if not exists:
            console.print(f"[yellow]Criando banco de dados: {database}[/yellow]")
            await conn.execute(f'CREATE DATABASE "{database}"')
            console.print(
                f"[green]✅ Banco de dados '{database}' criado com sucesso![/green]"
            )
        else:
            console.print(f"[blue]Banco de dados '{database}' já existe[/blue]")

        await conn.close()

    except Exception as e:
        logger.error(f"Erro ao criar banco de dados: {e}")
        raise


async def create_db_pool():
    """
    Cria pool de conexões assíncronas com o PostgreSQL
    """
    user = getEnv("DB_USER")
    passw = getEnv("DB_PASSWORD")
    host = getEnv("DB_HOST")
    port = getEnv("DB_PORT")
    database = getEnv("DB_NAME")
    ssl_mode = getEnv("DB_SSL_MODE", "disable")

    # Converter string SSL mode para valor booleano/None esperado pelo asyncpg
    ssl_config = None
    if ssl_mode.lower() in ["disable", "false"]:
        ssl_config = False
    elif ssl_mode.lower() in ["require", "true"]:
        ssl_config = True
    else:
        ssl_config = "prefer"  # padrão do asyncpg

    return await asyncpg.create_pool(
        user=user,
        password=passw,
        database=database,
        host=host,
        port=port,
        ssl=ssl_config,
        min_size=5,
        max_size=10,
        command_timeout=300,
        server_settings={"client_encoding": "utf8", "timezone": "UTC"},
    )


async def setup_tables(pool):
    """
    Configura as tabelas necessárias
    """
    async with pool.acquire() as conn:
        # Drop tables se existirem
        await conn.execute('DROP TABLE IF EXISTS "empresa";')
        await conn.execute('DROP TABLE IF EXISTS "estabelecimento";')
        await conn.execute('DROP TABLE IF EXISTS "simples";')
        await conn.execute('DROP TABLE IF EXISTS "socios";')
        await conn.execute('DROP TABLE IF EXISTS "cnae";')
        await conn.execute('DROP TABLE IF EXISTS "motivo";')
        await conn.execute('DROP TABLE IF EXISTS "municipio";')
        await conn.execute('DROP TABLE IF EXISTS "natureza";')
        await conn.execute('DROP TABLE IF EXISTS "pais";')
        await conn.execute('DROP TABLE IF EXISTS "qualificacao";')

        # Criar tabelas
        await conn.execute("""
            CREATE TABLE empresa (
                cnpj_basico TEXT NOT NULL,
                razao_social TEXT,
                natureza_juridica INTEGER,
                qualificacao_responsavel INTEGER,
                capital_social NUMERIC(15,2),
                porte_empresa INTEGER,
                ente_federativo_responsavel TEXT
            );
        """)

        await conn.execute("""
            CREATE TABLE estabelecimento (
                cnpj_basico TEXT NOT NULL,
                cnpj_ordem TEXT NOT NULL,
                cnpj_dv TEXT NOT NULL,
                identificador_matriz_filial INTEGER,
                nome_fantasia TEXT,
                situacao_cadastral INTEGER,
                data_situacao_cadastral DATE,
                motivo_situacao_cadastral INTEGER,
                nome_cidade_exterior TEXT,
                pais INTEGER,
                data_inicio_atividade DATE,
                cnae_fiscal_principal INTEGER,
                cnae_fiscal_secundaria TEXT,
                tipo_logradouro TEXT,
                logradouro TEXT,
                numero TEXT,
                complemento TEXT,
                bairro TEXT,
                cep TEXT,
                uf TEXT,
                municipio INTEGER,
                ddd_1 TEXT,
                telefone_1 TEXT,
                ddd_2 TEXT,
                telefone_2 TEXT,
                ddd_fax TEXT,
                fax TEXT,
                correio_eletronico TEXT,
                situacao_especial TEXT,
                data_situacao_especial DATE
            );
        """)

        await conn.execute("""
            CREATE TABLE socios (
                cnpj_basico TEXT NOT NULL,
                identificador_socio INTEGER,
                nome_socio TEXT,
                cnpj_cpf_socio TEXT,
                qualificacao_socio INTEGER,
                data_entrada_sociedade DATE,
                pais INTEGER,
                representante_legal TEXT,
                nome_representante TEXT,
                qualificacao_representante_legal INTEGER,
                faixa_etaria INTEGER
            );
        """)

        await conn.execute("""
            CREATE TABLE simples (
                cnpj_basico TEXT NOT NULL,
                opcao_pelo_simples TEXT,
                data_opcao_simples DATE,
                data_exclusao_simples DATE,
                opcao_mei TEXT,
                data_opcao_mei DATE,
                data_exclusao_mei DATE
            );
        """)

        await conn.execute("""
            CREATE TABLE cnae (
                codigo INTEGER NOT NULL PRIMARY KEY,
                descricao TEXT
            );
        """)

        await conn.execute("""
            CREATE TABLE motivo (
                codigo INTEGER NOT NULL PRIMARY KEY,
                descricao TEXT
            );
        """)

        await conn.execute("""
            CREATE TABLE municipio (
                codigo INTEGER NOT NULL PRIMARY KEY,
                descricao TEXT
            );
        """)

        await conn.execute("""
            CREATE TABLE natureza (
                codigo INTEGER NOT NULL PRIMARY KEY,
                descricao TEXT
            );
        """)

        await conn.execute("""
            CREATE TABLE pais (
                codigo INTEGER NOT NULL PRIMARY KEY,
                descricao TEXT
            );
        """)

        await conn.execute("""
            CREATE TABLE qualificacao (
                codigo INTEGER NOT NULL PRIMARY KEY,
                descricao TEXT
            );
        """)

        print("Tabelas criadas com sucesso!")


async def process_empresa_files(pool):
    """
    Processa arquivos de empresa de forma assíncrona
    """
    empresa_insert_start = time.time()
    print("""
#######################
## Arquivos de EMPRESA:
#######################
""")

    for e in range(0, len(arquivos_empresa)):
        print("Trabalhando no arquivo: " + arquivos_empresa[e] + " [...]")
        try:
            del empresa
        except:
            pass

        empresa_dtypes = {
            0: object,
            1: object,
            2: "Int32",
            3: "Int32",
            4: object,
            5: "Int32",
            6: object,
        }
        extracted_file_path = os.path.join(extracted_files, arquivos_empresa[e])

        try:
            empresa = pd.read_csv(
                filepath_or_buffer=extracted_file_path,
                sep=";",
                skiprows=0,
                header=None,
                dtype=empresa_dtypes,
                encoding="latin-1",
            )
        except pd.errors.EmptyDataError:
            print(f"Arquivo {arquivos_empresa[e]} está vazio. Pulando...")
            continue
        except Exception as e:
            logger.error(f"Erro ao ler arquivo {arquivos_empresa[e]}: {str(e)}")
            continue

        empresa = empresa.reset_index()
        del empresa["index"]

        # Renomear colunas
        empresa.columns = [
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "ente_federativo_responsavel",
        ]

        # Replace "," por "."
        empresa["capital_social"] = empresa["capital_social"].apply(
            lambda x: x.replace(",", ".")
        )
        empresa["capital_social"] = empresa["capital_social"].astype(float)

        # Gravar dados no banco usando função assíncrona
        await to_sql_async(empresa, pool, "empresa")
        logger.info(
            f"Arquivo {arquivos_empresa[e]} inserido com sucesso no banco de dados!"
        )

        # Liberar memória explicitamente
        del empresa
        gc.collect()

    print("Arquivos de empresa finalizados!")
    empresa_insert_end = time.time()
    empresa_Tempo_insert = round((empresa_insert_end - empresa_insert_start))
    print(
        "Tempo de execução do processo de empresa (em segundos): "
        + str(empresa_Tempo_insert)
    )


async def process_estabelecimento_files(pool):
    """
    Processa arquivos de estabelecimento de forma assíncrona
    """
    estabelecimento_insert_start = time.time()
    logger.info("Iniciando processamento dos arquivos de ESTABELECIMENTO")
    console.print("\n[bold green]###############################[/bold green]")
    console.print("[bold green]## Arquivos de ESTABELECIMENTO:[/bold green]")
    console.print("[bold green]###############################[/bold green]\n")

    logger.info(f"Tem {len(arquivos_estabelecimento)} arquivos de estabelecimento!")

    # Verificar checkpoint
    checkpoint = load_checkpoint()
    start_index = 0

    if checkpoint and checkpoint["stage"] == "estabelecimento":
        start_index = checkpoint.get("file_index", 0)
        console.print(
            f"[yellow]Retomando do arquivo {start_index + 1}/{len(arquivos_estabelecimento)}[/yellow]"
        )

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        files_task = progress.add_task(
            "Processando arquivos ESTABELECIMENTO", total=len(arquivos_estabelecimento)
        )
        progress.update(files_task, completed=start_index)

        for e in range(start_index, len(arquivos_estabelecimento)):
            logger.info(f"Trabalhando no arquivo: {arquivos_estabelecimento[e]}")
            try:
                del estabelecimento
            except:
                pass

            estabelecimento_dtypes = {
                0: object,
                1: object,
                2: object,
                3: "Int32",
                4: object,
                5: "Int32",
                6: object,
                7: "Int32",
                8: object,
                9: "Int32",
                10: object,
                11: "Int32",
                12: object,
                13: object,
                14: object,
                15: object,
                16: object,
                17: object,
                18: object,
                19: object,
                20: "Int32",
                21: object,
                22: object,
                23: object,
                24: object,
                25: object,
                26: object,
                27: object,
                28: object,
                29: object,
            }
            extracted_file_path = os.path.join(
                extracted_files, arquivos_estabelecimento[e]
            )

            parts_task = progress.add_task(
                f"Processando {arquivos_estabelecimento[e]}", total=None
            )
            NROWS = 2000000
            part = 0
            while True:
                try:
                    estabelecimento = pd.read_csv(
                        filepath_or_buffer=extracted_file_path,
                        sep=";",
                        nrows=NROWS,
                        skiprows=NROWS * part,
                        header=None,
                        dtype=estabelecimento_dtypes,
                        encoding="latin-1",
                    )
                except pd.errors.EmptyDataError:
                    logger.info(
                        f"Fim do arquivo {arquivos_estabelecimento[e]} na parte {part}"
                    )
                    break
                except Exception as ex:
                    logger.error(
                        f"Erro ao ler arquivo {arquivos_estabelecimento[e]} na parte {part}: {str(ex)}"
                    )
                    break

                if estabelecimento.empty:
                    break

                estabelecimento = estabelecimento.reset_index()
                del estabelecimento["index"]

                estabelecimento.columns = [
                    "cnpj_basico",
                    "cnpj_ordem",
                    "cnpj_dv",
                    "identificador_matriz_filial",
                    "nome_fantasia",
                    "situacao_cadastral",
                    "data_situacao_cadastral",
                    "motivo_situacao_cadastral",
                    "nome_cidade_exterior",
                    "pais",
                    "data_inicio_atividade",
                    "cnae_fiscal_principal",
                    "cnae_fiscal_secundaria",
                    "tipo_logradouro",
                    "logradouro",
                    "numero",
                    "complemento",
                    "bairro",
                    "cep",
                    "uf",
                    "municipio",
                    "ddd_1",
                    "telefone_1",
                    "ddd_2",
                    "telefone_2",
                    "ddd_fax",
                    "fax",
                    "correio_eletronico",
                    "situacao_especial",
                    "data_situacao_especial",
                ]

                await to_sql_async(estabelecimento, pool, "estabelecimento")
                logger.info(
                    f"Parte {part + 1} do arquivo {arquivos_estabelecimento[e]} inserida com sucesso!"
                )
                progress.update(parts_task, advance=1)

                part += 1
                del estabelecimento
                gc.collect()

            logger.info(
                f"Arquivo {arquivos_estabelecimento[e]} inserido com sucesso no banco de dados!"
            )
            progress.update(files_task, advance=1)
            progress.remove_task(parts_task)

            # Salvar checkpoint após cada arquivo processado
            save_checkpoint("estabelecimento", e + 1)

    logger.info("Arquivos de estabelecimento finalizados!")
    estabelecimento_insert_end = time.time()
    estabelecimento_Tempo_insert = round(
        (estabelecimento_insert_end - estabelecimento_insert_start)
    )
    logger.info(
        f"Tempo de execução do processo de estabelecimento (em segundos): {estabelecimento_Tempo_insert}"
    )


async def process_socios_files(pool):
    """
    Processa arquivos de sócios de forma assíncrona
    """
    socios_insert_start = time.time()
    print("""
######################
## Arquivos de SOCIOS:
######################
""")

    for e in range(0, len(arquivos_socios)):
        print("Trabalhando no arquivo: " + arquivos_socios[e] + " [...]")
        try:
            del socios
        except:
            pass

        socios_dtypes = {
            0: object,
            1: "Int32",
            2: object,
            3: object,
            4: "Int32",
            5: object,
            6: "Int32",
            7: object,
            8: object,
            9: "Int32",
            10: "Int32",
        }
        extracted_file_path = os.path.join(extracted_files, arquivos_socios[e])
        try:
            socios = pd.read_csv(
                filepath_or_buffer=extracted_file_path,
                sep=";",
                skiprows=0,
                header=None,
                dtype=socios_dtypes,
                encoding="latin-1",
            )
        except pd.errors.EmptyDataError:
            print(f"Arquivo {arquivos_socios[e]} está vazio. Pulando...")
            continue
        except Exception as e:
            logger.error(f"Erro ao ler arquivo {arquivos_socios[e]}: {str(e)}")
            continue

        # Tratamento do arquivo antes de inserir na base:
        socios = socios.reset_index()
        del socios["index"]

        # Renomear colunas
        socios.columns = [
            "cnpj_basico",
            "identificador_socio",
            "nome_socio",
            "cnpj_cpf_socio",
            "qualificacao_socio",
            "data_entrada_sociedade",
            "pais",
            "representante_legal",
            "nome_representante",
            "qualificacao_representante_legal",
            "faixa_etaria",
        ]

        # Gravar dados no banco usando função assíncrona
        await to_sql_async(socios, pool, "socios")
        logger.info(
            f"Arquivo {arquivos_socios[e]} inserido com sucesso no banco de dados!"
        )

        del socios
        gc.collect()

    print("Arquivos de socios finalizados!")
    socios_insert_end = time.time()
    socios_Tempo_insert = round((socios_insert_end - socios_insert_start))
    print(
        "Tempo de execução do processo de sócios (em segundos): "
        + str(socios_Tempo_insert)
    )


async def process_simples_files(pool):
    """
    Processa arquivos do Simples Nacional de forma assíncrona
    """
    simples_insert_start = time.time()
    logger.info("Iniciando processamento dos arquivos do SIMPLES NACIONAL")
    console.print("\n[bold blue]################################[/bold blue]")
    console.print("[bold blue]## Arquivos do SIMPLES NACIONAL:[/bold blue]")
    console.print("[bold blue]################################[/bold blue]\n")

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        files_task = progress.add_task(
            "Processando arquivos SIMPLES", total=len(arquivos_simples)
        )

        for e in range(0, len(arquivos_simples)):
            logger.info(f"Trabalhando no arquivo: {arquivos_simples[e]}")

            simples_dtypes = {
                0: object,
                1: object,
                2: object,
                3: object,
                4: object,
                5: object,
                6: object,
            }
            extracted_file_path = os.path.join(extracted_files, arquivos_simples[e])

            simples_lenght = sum(1 for line in open(extracted_file_path, "r"))
            logger.info(
                f"Linhas no arquivo do Simples {arquivos_simples[e]}: {simples_lenght}"
            )

            tamanho_das_partes = 1000000  # Registros por carga
            qtd_loops = round(simples_lenght / tamanho_das_partes, 0)
            nrows = tamanho_das_partes
            logger.info(f"Esse arquivo será carregado em {int(qtd_loops + 1)} partes")

            parts_task = progress.add_task(
                f"Processando {arquivos_simples[e]}", total=int(qtd_loops + 1)
            )

            skiprows = 0
            for i in range(int(qtd_loops + 1)):
                logger.debug(f"Iniciando a parte {i + 1} de {int(qtd_loops + 1)}")

                try:
                    simples = pd.read_csv(
                        filepath_or_buffer=extracted_file_path,
                        sep=";",
                        nrows=nrows,
                        skiprows=skiprows,
                        header=None,
                        dtype=simples_dtypes,
                        encoding="latin-1",
                    )
                except pd.errors.EmptyDataError:
                    logger.info(
                        f"Fim do arquivo alcançado na parte {i + 1}. Encerrando processamento."
                    )
                    break

                if simples.empty:
                    break

                simples = simples.reset_index()
                del simples["index"]

                simples.columns = [
                    "cnpj_basico",
                    "opcao_pelo_simples",
                    "data_opcao_simples",
                    "data_exclusao_simples",
                    "opcao_mei",
                    "data_opcao_mei",
                    "data_exclusao_mei",
                ]

                # Gravar dados no banco usando função assíncrona
                await to_sql_async(simples, pool, "simples")

                skiprows = skiprows + nrows
                logger.info(
                    f"Parte {i + 1} do arquivo {arquivos_simples[e]} inserida com sucesso no banco de dados!"
                )
                progress.update(parts_task, advance=1)
                del simples
                gc.collect()

            logger.info(
                f"Arquivo {arquivos_simples[e]} inserido com sucesso no banco de dados!"
            )
            progress.update(files_task, advance=1)
            progress.remove_task(parts_task)

    logger.info("Arquivos do Simples Nacional finalizados!")
    simples_insert_end = time.time()
    simples_Tempo_insert = round((simples_insert_end - simples_insert_start))
    logger.info(
        f"Tempo de execução do processo do Simples Nacional (em segundos): {simples_Tempo_insert}"
    )


async def process_outros_arquivos(pool):
    """
    Processa os demais arquivos (CNAE, Motivo, Municipio, etc.)
    """
    print("Processando arquivos auxiliares (CNAE, Motivo, Municipio, etc.)")

    # Processar CNAE
    if arquivos_cnae:
        for e in range(0, len(arquivos_cnae)):
            extracted_file_path = os.path.join(extracted_files, arquivos_cnae[e])
            try:
                cnae = pd.read_csv(
                    filepath_or_buffer=extracted_file_path,
                    sep=";",
                    skiprows=0,
                    header=None,
                    dtype={0: "Int32", 1: "object"},
                    encoding="latin-1",
                )
            except pd.errors.EmptyDataError:
                print(f"Arquivo CNAE {arquivos_cnae[e]} está vazio. Pulando...")
                continue
            except Exception as e:
                logger.error(f"Erro ao ler arquivo CNAE {arquivos_cnae[e]}: {str(e)}")
                continue
            cnae = cnae.reset_index()
            del cnae["index"]
            cnae.columns = ["codigo", "descricao"]
            await to_sql_async(cnae, pool, "cnae")
            logger.info(f"Arquivo CNAE {arquivos_cnae[e]} inserido!")
            del cnae
            gc.collect()

    # Processar demais arquivos de forma similar...
    for arquivo_tipo, nome_tabela in [
        (arquivos_moti, "motivo"),
        (arquivos_munic, "municipio"),
        (arquivos_natju, "natureza"),
        (arquivos_pais, "pais"),
        (arquivos_quals, "qualificacao"),
    ]:
        if arquivo_tipo:
            for e in range(0, len(arquivo_tipo)):
                extracted_file_path = os.path.join(extracted_files, arquivo_tipo[e])
                try:
                    df = pd.read_csv(
                        filepath_or_buffer=extracted_file_path,
                        sep=";",
                        skiprows=0,
                        header=None,
                        dtype={0: "Int32", 1: "object"},
                        encoding="latin-1",
                    )
                except pd.errors.EmptyDataError:
                    print(
                        f"Arquivo {nome_tabela} {arquivo_tipo[e]} está vazio. Pulando..."
                    )
                    continue
                except Exception as e:
                    logger.error(
                        f"Erro ao ler arquivo {nome_tabela} {arquivo_tipo[e]}: {str(e)}"
                    )
                    continue
                df = df.reset_index()
                del df["index"]
                df.columns = ["codigo", "descricao"]
                await to_sql_async(df, pool, nome_tabela)
                logger.info(f"Arquivo {nome_tabela} {arquivo_tipo[e]} inserido!")
                del df
                gc.collect()


async def create_indexes(pool):
    """
    Cria índices nas tabelas de forma assíncrona com timeout maior
    """
    console.print(
        "\n[bold yellow]🔨 [FASE 4] Criando índices para otimização de consultas...[/bold yellow]"
    )

    # Lista completa de índices baseada no script create_indexes.py
    indexes = [
        {
            "name": "empresa_cnpj",
            "table": "empresa",
            "columns": "cnpj_basico",
            "sql": "CREATE INDEX IF NOT EXISTS empresa_cnpj ON empresa(cnpj_basico);",
        },
        {
            "name": "estabelecimento_cnpj",
            "table": "estabelecimento",
            "columns": "cnpj_basico",
            "sql": "CREATE INDEX IF NOT EXISTS estabelecimento_cnpj ON estabelecimento(cnpj_basico);",
        },
        {
            "name": "estabelecimento_cnpj_completo",
            "table": "estabelecimento",
            "columns": "cnpj_basico, cnpj_ordem, cnpj_dv",
            "sql": "CREATE INDEX IF NOT EXISTS estabelecimento_cnpj_completo ON estabelecimento(cnpj_basico, cnpj_ordem, cnpj_dv);",
        },
        {
            "name": "socios_cnpj",
            "table": "socios",
            "columns": "cnpj_basico",
            "sql": "CREATE INDEX IF NOT EXISTS socios_cnpj ON socios(cnpj_basico);",
        },
        {
            "name": "simples_cnpj",
            "table": "simples",
            "columns": "cnpj_basico",
            "sql": "CREATE INDEX IF NOT EXISTS simples_cnpj ON simples(cnpj_basico);",
        },
        {
            "name": "estabelecimento_situacao",
            "table": "estabelecimento",
            "columns": "situacao_cadastral",
            "sql": "CREATE INDEX IF NOT EXISTS estabelecimento_situacao ON estabelecimento(situacao_cadastral);",
        },
        {
            "name": "estabelecimento_municipio",
            "table": "estabelecimento",
            "columns": "municipio",
            "sql": "CREATE INDEX IF NOT EXISTS estabelecimento_municipio ON estabelecimento(municipio);",
        },
    ]

    created_count = 0
    failed_count = 0
    skipped_count = 0

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        index_task = progress.add_task("Criando índices", total=len(indexes))

        async with pool.acquire() as conn:
            # Configurar timeout maior para criação de índices
            await conn.execute("SET statement_timeout = '3600000';")  # 1 hora
            await conn.execute("SET lock_timeout = '3600000';")

            for index_info in indexes:
                try:
                    # Verificar se a tabela existe
                    table_exists = await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
                        index_info["table"],
                    )

                    if not table_exists:
                        logger.warning(
                            f"Tabela {index_info['table']} não encontrada, pulando índice {index_info['name']}"
                        )
                        skipped_count += 1
                        progress.update(index_task, advance=1)
                        continue

                    # Verificar se o índice já existe
                    index_exists = await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname = $1)",
                        index_info["name"],
                    )

                    if index_exists:
                        logger.info(f"Índice {index_info['name']} já existe")
                        skipped_count += 1
                        progress.update(index_task, advance=1)
                        continue

                    # Obter tamanho da tabela
                    table_size = await conn.fetchval(
                        f"SELECT COUNT(*) FROM {index_info['table']}"
                    )

                    logger.info(
                        f"Criando índice {index_info['name']} na tabela {index_info['table']} ({table_size:,} registros)"
                    )

                    start_time = time.time()
                    await conn.execute(index_info["sql"])
                    elapsed_time = time.time() - start_time

                    logger.info(
                        f"Índice {index_info['name']} criado com sucesso em {elapsed_time:.1f}s"
                    )
                    created_count += 1

                except Exception as e:
                    logger.error(f"Erro ao criar índice {index_info['name']}: {e}")
                    failed_count += 1

                progress.update(index_task, advance=1)

    console.print(f"\n[green]✅ Criação de índices concluída![/green]")
    console.print(f"[green]  • Índices criados: {created_count}[/green]")
    console.print(f"[blue]  • Índices já existentes: {skipped_count}[/blue]")
    if failed_count > 0:
        console.print(f"[red]  • Índices com erro: {failed_count}[/red]")


async def main():
    """
    Função principal que executa todo o processo de ETL de forma assíncrona
    """
    console.print("\n[bold magenta]" + "=" * 50 + "[/bold magenta]")
    console.print(
        "[bold magenta]    PROCESSO ETL ASSÍNCRONO INICIADO    [/bold magenta]"
    )
    console.print("[bold magenta]" + "=" * 50 + "[/bold magenta]\n")

    start_time = time.time()
    logger.info("Processo ETL iniciado")

    try:
        # Fase 1: Download paralelo
        console.print(
            "\n[bold yellow]📥 [FASE 1] Download dos arquivos...[/bold yellow]"
        )
        download_start = time.time()
        await download_all_files()
        download_time = time.time() - download_start
        logger.info(f"Download concluído em {download_time:.1f}s")
        console.print(f"[green]✅ Download concluído em {download_time:.1f}s[/green]")

        # Fase 2: Extração paralela
        console.print(
            "\n[bold yellow]📂 [FASE 2] Extração dos arquivos...[/bold yellow]"
        )
        extract_start = time.time()
        await extract_all_files()
        extract_time = time.time() - extract_start
        logger.info(f"Extração concluída em {extract_time:.1f}s")
        console.print(f"[green]✅ Extração concluída em {extract_time:.1f}s[/green]")

        # Fase 3: Processamento de dados
        console.print(
            "\n[bold yellow]🗄️  [FASE 3] Processamento e inserção no banco...[/bold yellow]"
        )
        logger.info("Iniciando processamento e inserção no banco")

        # Criar banco de dados se não existir
        await create_database_if_not_exists()

        # Criar pool de conexões
        pool = await create_db_pool()

        try:
            # Configurar tabelas
            await setup_tables(pool)

            # Processar todos os tipos de arquivo
            checkpoint = load_checkpoint()

            # Verificar qual etapa retomar
            if not checkpoint or checkpoint["stage"] == "empresa":
                await process_empresa_files(pool)
                save_checkpoint("empresa_completed")

            if not checkpoint or checkpoint["stage"] in [
                "empresa",
                "empresa_completed",
                "estabelecimento",
            ]:
                await process_estabelecimento_files(pool)
                save_checkpoint("estabelecimento_completed")

            if not checkpoint or checkpoint["stage"] in [
                "empresa",
                "empresa_completed",
                "estabelecimento",
                "estabelecimento_completed",
                "socios",
            ]:
                await process_socios_files(pool)
                save_checkpoint("socios_completed")

            if not checkpoint or checkpoint["stage"] in [
                "empresa",
                "empresa_completed",
                "estabelecimento",
                "estabelecimento_completed",
                "socios",
                "socios_completed",
                "simples",
            ]:
                await process_simples_files(pool)
                save_checkpoint("simples_completed")

            await process_outros_arquivos(pool)

            # Criar índices automaticamente
            save_checkpoint("creating_indexes")
            await create_indexes(pool)

            # Limpar checkpoint após conclusão bem-sucedida
            clear_checkpoint()

        finally:
            # Fechar pool de conexões
            await pool.close()

        total_time = time.time() - start_time
        minutes = int(total_time // 60)
        seconds = int(total_time % 60)

        console.print(f"\n[bold green]" + "=" * 60 + "[/bold green]")
        console.print(
            f"[bold green]    ✅ PROCESSO CONCLUÍDO EM {minutes}m {seconds}s ({total_time:.1f}s)    [/bold green]"
        )
        console.print(f"[bold green]" + "=" * 60 + "[/bold green]")

        # Resumo dos tempos
        table = Table(title="📊 Resumo de Performance")
        table.add_column("Fase", style="cyan")
        table.add_column("Tempo", style="magenta")
        table.add_row("Download", f"{download_time:.1f}s")
        table.add_row("Extração", f"{extract_time:.1f}s")
        table.add_row(
            "Processamento", f"{total_time - download_time - extract_time:.1f}s"
        )
        table.add_row("Total", f"{total_time:.1f}s", style="bold")
        console.print(table)

        logger.info(f"Processo ETL concluído em {total_time:.1f}s")
        console.print(
            "\n[bold blue]🎉 Processo 100% finalizado! Você já pode usar seus dados no BD![/bold blue]"
        )

    except Exception as e:
        logger.error(f"Erro no processo ETL: {e}", exc_info=True)
        console.print(f"\n[bold red]✗ ERRO NO PROCESSO ETL: {e}[/bold red]")
        raise


if __name__ == "__main__":
    asyncio.run(main())

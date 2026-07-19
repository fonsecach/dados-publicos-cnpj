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
import polars as pl
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

    import base64
    import ssl

    try:
        # Configurar SSL mais permissivo
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        credentials = base64.b64encode(f"{SHARE_TOKEN}:".encode()).decode()
        with httpx.Client(
            headers={
                "Authorization": f"Basic {credentials}",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
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


def transcode_to_utf8(path):
    """
    Copia um CSV latin-1 (encoding dos arquivos da Receita Federal) para um
    arquivo irmão "<path>.utf8.csv" em UTF-8. O parser CSV do Polars só lê
    utf8/utf8-lossy nativamente — não existe suporte a latin-1. Como latin-1
    é 1 byte = 1 caractere, o corte em blocos abaixo nunca quebra um
    caractere multibyte. O chamador deve remover o arquivo gerado após o uso.
    """
    utf8_path = f"{path}.utf8.csv"
    with open(path, "rb") as fin, open(utf8_path, "wb") as fout:
        while True:
            chunk = fin.read(4 * 1024 * 1024)
            if not chunk:
                break
            fout.write(chunk.decode("latin-1").encode("utf-8"))
    return utf8_path


def remove_file_safe(path):
    """
    Remove um arquivo já carregado no banco, sem derrubar o ETL se falhar
    (ex: permissão, arquivo já removido). Loga um aviso e segue em frente.
    """
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except OSError as e:
        logger.warning(f"Não foi possível remover {path}: {e}")


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


async def to_sql_async(dataframe, pool, table_name, batch_size=50000):
    """
    Insere dados de forma assíncrona usando o protocolo COPY do Postgres
    (asyncpg.copy_records_to_table) — muito mais rápido que INSERT em lote
    para as tabelas de fato (estabelecimento, empresa, socios, simples).
    """
    total = dataframe.height
    columns = dataframe.columns

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

    df = dataframe
    date_cols_present = [c for c in date_columns.get(table_name, []) if c in df.columns]
    if date_cols_present:
        # "", "0" e "00000000" não casam com %Y%m%d e viram NULL com strict=False
        df = df.with_columns(
            [
                pl.col(c).str.strptime(pl.Date, format="%Y%m%d", strict=False)
                for c in date_cols_present
            ]
        )

    async def copy_batch(offset, length):
        # Fatiar e materializar em tuplas Python só na hora de copiar ESTE batch
        # (não a tabela inteira de uma vez) — df.slice() é uma view zero-copy
        # sobre os buffers Arrow do Polars, e .rows() só aloca objetos Python
        # para as `length` linhas deste batch. Antes disso era feito um único
        # `df.rows()` sobre a tabela inteira, mantendo a tabela inteira como
        # tuplas Python + o DataFrame original vivos ao mesmo tempo — causa
        # confirmada (dmesg/oom-killer) de OOM em produção com tabelas grandes.
        batch_records = df.slice(offset, length).rows()
        async with pool.acquire() as conn:
            await conn.copy_records_to_table(
                table_name, records=batch_records, columns=columns
            )

        # Progress tracking
        completed = min(offset + length, total)
        percent = (completed * 100) / total if total else 100
        progress = f"{table_name} {percent:.2f}% {completed:0{len(str(total))}}/{total}"
        sys.stdout.write(f"\r{progress}")
        sys.stdout.flush()

    # Processar em batches
    tasks = [
        copy_batch(i, min(batch_size, total - i))
        for i in range(0, total, batch_size)
    ]

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

    parser.add_argument(
        "--db-target",
        default="receita_federal_staging",
        dest="db_target",
        help="Banco de dados destino para a carga (padrão: receita_federal_staging)",
    )

    parser.add_argument(
        "--skip-download",
        action="store_true",
        dest="skip_download",
        help=(
            "Pula download (Fase 1) e extração (Fase 2) — usa os arquivos já "
            "baixados/extraídos em disco e vai direto para o processamento/"
            "inserção no banco (Fase 3). Não faz requisição à Receita Federal."
        ),
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


if args.skip_download:
    # --skip-download: não faz nenhuma requisição à Receita Federal —
    # os arquivos já baixados/extraídos em disco são usados como estão
    Files = []
    print(
        "[--skip-download] Pulando listagem remota e download — "
        "usando arquivos já extraídos em disco."
    )
else:
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

# Listas de arquivos por tipo — populadas em categorize_extracted_files(),
# chamada dentro de main() DEPOIS da extração (ver nota abaixo sobre o bug corrigido)
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


def categorize_extracted_files():
    """
    Categoriza os arquivos de extracted_files por tipo de tabela.
    Precisa ser chamada DEPOIS da extração (Fase 2) — chamar antes lê o
    diretório com o conteúdo da execução anterior (ou vazio), fazendo o
    processamento agir sobre listas vazias mesmo com a extração bem-sucedida.
    """
    global arquivos_empresa, arquivos_estabelecimento, arquivos_socios
    global arquivos_simples, arquivos_cnae, arquivos_moti, arquivos_munic
    global arquivos_natju, arquivos_pais, arquivos_quals

    items = [name for name in os.listdir(extracted_files) if name.endswith("")]

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

    for item in items:
        name_upper = item.upper()
        if "EMPRECSV" in name_upper or "EMPRESAS" in name_upper:
            arquivos_empresa.append(item)
        elif "ESTABELE" in name_upper or "ESTABELECIMENTOS" in name_upper:
            arquivos_estabelecimento.append(item)
        elif "SOCIOCSV" in name_upper or "SOCIOS" in name_upper:
            arquivos_socios.append(item)
        elif "SIMPLES" in name_upper:
            arquivos_simples.append(item)
        elif "CNAECSV" in name_upper or "CNAES" in name_upper:
            arquivos_cnae.append(item)
        elif "MOTICSV" in name_upper or "MOTIVOS" in name_upper:
            arquivos_moti.append(item)
        elif "MUNICCSV" in name_upper or "MUNICIPIOS" in name_upper:
            arquivos_munic.append(item)
        elif "NATJUCSV" in name_upper or "NATUREZAS" in name_upper:
            arquivos_natju.append(item)
        elif "PAISCSV" in name_upper or "PAISES" in name_upper:
            arquivos_pais.append(item)
        elif "QUALSCSV" in name_upper or "QUALIFICACOES" in name_upper:
            arquivos_quals.append(item)


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
async def download_file_async(url, file_path, semaphore):
    """Download file asynchronously with retry logic using httpx"""
    import ssl

    async with semaphore:  # Limita downloads simultâneos
        import base64
        credentials = base64.b64encode(f"{SHARE_TOKEN}:".encode()).decode()
        headers = {
            "Authorization": f"Basic {credentials}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
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
        url = f"{WEBDAV_BASE_URL}/{ano}-{mes_formatado}/{file_name}"
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


async def create_database_if_not_exists(db_name: str = None):
    """
    Cria o banco de dados se não existir
    """
    user = getEnv("DB_USER")
    passw = getEnv("DB_PASSWORD")
    host = getEnv("DB_HOST")
    port = getEnv("DB_PORT")
    database = db_name or getEnv("DB_NAME")
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


async def create_db_pool(db_name: str = None):
    """
    Cria pool de conexões assíncronas com o PostgreSQL
    """
    user = getEnv("DB_USER")
    passw = getEnv("DB_PASSWORD")
    host = getEnv("DB_HOST")
    port = getEnv("DB_PORT")
    database = db_name or getEnv("DB_NAME")
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


TABLE_DDL = {
    "empresa": """
        CREATE TABLE empresa (
            cnpj_basico TEXT NOT NULL,
            razao_social TEXT,
            natureza_juridica INTEGER,
            qualificacao_responsavel INTEGER,
            capital_social NUMERIC(15,2),
            porte_empresa INTEGER,
            ente_federativo_responsavel TEXT
        );
    """,
    "estabelecimento": """
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
    """,
    "socios": """
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
    """,
    "simples": """
        CREATE TABLE simples (
            cnpj_basico TEXT NOT NULL,
            opcao_pelo_simples TEXT,
            data_opcao_simples DATE,
            data_exclusao_simples DATE,
            opcao_mei TEXT,
            data_opcao_mei DATE,
            data_exclusao_mei DATE
        );
    """,
    "cnae": """
        CREATE TABLE cnae (
            codigo INTEGER NOT NULL PRIMARY KEY,
            descricao TEXT
        );
    """,
    "motivo": """
        CREATE TABLE motivo (
            codigo INTEGER NOT NULL PRIMARY KEY,
            descricao TEXT
        );
    """,
    "municipio": """
        CREATE TABLE municipio (
            codigo INTEGER NOT NULL PRIMARY KEY,
            descricao TEXT
        );
    """,
    "natureza": """
        CREATE TABLE natureza (
            codigo INTEGER NOT NULL PRIMARY KEY,
            descricao TEXT
        );
    """,
    "pais": """
        CREATE TABLE pais (
            codigo INTEGER NOT NULL PRIMARY KEY,
            descricao TEXT
        );
    """,
    "qualificacao": """
        CREATE TABLE qualificacao (
            codigo INTEGER NOT NULL PRIMARY KEY,
            descricao TEXT
        );
    """,
}


def tables_to_preserve(checkpoint):
    """
    Retorna o conjunto de tabelas que já têm dado carregado (total ou
    parcialmente, no caso de estabelecimento em retomada por arquivo) segundo
    o checkpoint, e que portanto NÃO devem ser dropadas/recriadas em
    setup_tables(). Espelha os mesmos estágios usados para pular reprocessamento
    em main() — só preserva o que já foi efetivamente concluído.
    """
    if not checkpoint:
        return set()

    stage = checkpoint.get("stage")
    preserve = set()

    past_empresa = (
        "empresa_completed",
        "estabelecimento",
        "estabelecimento_completed",
        "socios_completed",
        "simples_completed",
        "outros_completed",
        "creating_indexes",
    )
    past_estabelecimento = (
        "estabelecimento_completed",
        "socios_completed",
        "simples_completed",
        "outros_completed",
        "creating_indexes",
    )
    past_socios = ("socios_completed", "simples_completed", "outros_completed", "creating_indexes")
    past_simples = ("simples_completed", "outros_completed", "creating_indexes")
    past_outros = ("outros_completed", "creating_indexes")

    if stage in past_empresa:
        preserve.add("empresa")
    # "estabelecimento" como stage significa retomada por arquivo — a tabela
    # já tem dado parcial (arquivos 0..file_index-1) e não deve ser dropada
    if stage == "estabelecimento" or stage in past_estabelecimento:
        preserve.add("estabelecimento")
    if stage in past_socios:
        preserve.add("socios")
    if stage in past_simples:
        preserve.add("simples")
    if stage in past_outros:
        preserve.update({"cnae", "motivo", "municipio", "natureza", "pais", "qualificacao"})

    return preserve


async def setup_tables(pool, preserve_tables=None):
    """
    Configura as tabelas necessárias. Tabelas em `preserve_tables` (já
    carregadas segundo o checkpoint) não são dropadas nem recriadas — mantém
    os dados de uma execução anterior interrompida.
    """
    preserve_tables = preserve_tables or set()

    async with pool.acquire() as conn:
        for table_name, ddl in TABLE_DDL.items():
            if table_name in preserve_tables:
                logger.info(
                    f"Tabela '{table_name}' preservada (checkpoint indica dado já carregado)"
                )
                continue
            await conn.execute(f'DROP TABLE IF EXISTS "{table_name}";')
            await conn.execute(ddl)

        print("Tabelas configuradas com sucesso!")


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

    empresa_columns = [
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo_responsavel",
    ]

    for e in range(0, len(arquivos_empresa)):
        print("Trabalhando no arquivo: " + arquivos_empresa[e] + " [...]")

        extracted_file_path = os.path.join(extracted_files, arquivos_empresa[e])
        utf8_path = None

        try:
            utf8_path = transcode_to_utf8(extracted_file_path)
            empresa = pl.read_csv(
                utf8_path,
                separator=";",
                has_header=False,
                new_columns=empresa_columns,
                schema_overrides=[pl.Utf8] * len(empresa_columns),
                encoding="utf8",
            )
        except pl.exceptions.NoDataError:
            print(f"Arquivo {arquivos_empresa[e]} está vazio. Pulando...")
            continue
        except Exception as ex:
            logger.error(f"Erro ao ler arquivo {arquivos_empresa[e]}: {str(ex)}")
            continue
        finally:
            if utf8_path and os.path.exists(utf8_path):
                os.remove(utf8_path)

        # Colunas numéricas — inválido/vazio vira NULL (strict=False), não exceção
        empresa = empresa.with_columns(
            [
                pl.col("natureza_juridica").cast(pl.Int32, strict=False),
                pl.col("qualificacao_responsavel").cast(pl.Int32, strict=False),
                pl.col("porte_empresa").cast(pl.Int32, strict=False),
                pl.col("capital_social")
                .str.replace(",", ".", literal=True)
                .cast(pl.Float64, strict=False),
            ]
        )

        # Gravar dados no banco usando função assíncrona
        await to_sql_async(empresa, pool, "empresa")
        logger.info(
            f"Arquivo {arquivos_empresa[e]} inserido com sucesso no banco de dados!"
        )

        # Já carregado no banco — libera espaço em disco
        remove_file_safe(extracted_file_path)

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

        estabelecimento_columns = [
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
        estabelecimento_int32_cols = [
            "identificador_matriz_filial",
            "situacao_cadastral",
            "motivo_situacao_cadastral",
            "pais",
            "cnae_fiscal_principal",
            "municipio",
        ]

        for e in range(start_index, len(arquivos_estabelecimento)):
            logger.info(f"Trabalhando no arquivo: {arquivos_estabelecimento[e]}")

            extracted_file_path = os.path.join(
                extracted_files, arquivos_estabelecimento[e]
            )

            parts_task = progress.add_task(
                f"Processando {arquivos_estabelecimento[e]}", total=None
            )
            NROWS = 2000000
            part = 0
            utf8_path = None
            try:
                utf8_path = transcode_to_utf8(extracted_file_path)
                lf = pl.scan_csv(
                    utf8_path,
                    separator=";",
                    has_header=False,
                    new_columns=estabelecimento_columns,
                    schema_overrides=[pl.Utf8] * len(estabelecimento_columns),
                    encoding="utf8",
                )
                for estabelecimento in lf.collect_batches(chunk_size=NROWS):
                    if estabelecimento.height == 0:
                        continue

                    # Colunas numéricas — inválido/vazio vira NULL (strict=False)
                    estabelecimento = estabelecimento.with_columns(
                        [
                            pl.col(c).cast(pl.Int32, strict=False)
                            for c in estabelecimento_int32_cols
                        ]
                    )

                    await to_sql_async(estabelecimento, pool, "estabelecimento")
                    logger.info(
                        f"Parte {part + 1} do arquivo {arquivos_estabelecimento[e]} inserida com sucesso!"
                    )
                    progress.update(parts_task, advance=1)

                    part += 1
                    del estabelecimento
                    gc.collect()
            except pl.exceptions.NoDataError:
                logger.info(
                    f"Fim do arquivo {arquivos_estabelecimento[e]} na parte {part}"
                )
            except Exception as ex:
                logger.error(
                    f"Erro ao ler arquivo {arquivos_estabelecimento[e]} na parte {part}: {str(ex)}"
                )
            finally:
                if utf8_path and os.path.exists(utf8_path):
                    os.remove(utf8_path)

            logger.info(
                f"Arquivo {arquivos_estabelecimento[e]} inserido com sucesso no banco de dados!"
            )
            progress.update(files_task, advance=1)
            progress.remove_task(parts_task)

            # Salvar checkpoint após cada arquivo processado
            save_checkpoint("estabelecimento", e + 1)

            # Já carregado no banco — libera espaço em disco
            remove_file_safe(extracted_file_path)

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

    socios_columns = [
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

    for e in range(0, len(arquivos_socios)):
        print("Trabalhando no arquivo: " + arquivos_socios[e] + " [...]")

        extracted_file_path = os.path.join(extracted_files, arquivos_socios[e])
        utf8_path = None

        try:
            utf8_path = transcode_to_utf8(extracted_file_path)
            socios = pl.read_csv(
                utf8_path,
                separator=";",
                has_header=False,
                new_columns=socios_columns,
                schema_overrides=[pl.Utf8] * len(socios_columns),
                encoding="utf8",
            )
        except pl.exceptions.NoDataError:
            print(f"Arquivo {arquivos_socios[e]} está vazio. Pulando...")
            continue
        except Exception as ex:
            logger.error(f"Erro ao ler arquivo {arquivos_socios[e]}: {str(ex)}")
            continue
        finally:
            if utf8_path and os.path.exists(utf8_path):
                os.remove(utf8_path)

        # Colunas numéricas — inválido/vazio vira NULL (strict=False), não exceção
        socios = socios.with_columns(
            [
                pl.col("identificador_socio").cast(pl.Int32, strict=False),
                pl.col("qualificacao_socio").cast(pl.Int32, strict=False),
                pl.col("pais").cast(pl.Int32, strict=False),
                pl.col("qualificacao_representante_legal").cast(pl.Int32, strict=False),
                pl.col("faixa_etaria").cast(pl.Int32, strict=False),
            ]
        )

        # Gravar dados no banco usando função assíncrona
        await to_sql_async(socios, pool, "socios")
        logger.info(
            f"Arquivo {arquivos_socios[e]} inserido com sucesso no banco de dados!"
        )

        # Já carregado no banco — libera espaço em disco
        remove_file_safe(extracted_file_path)

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

        simples_columns = [
            "cnpj_basico",
            "opcao_pelo_simples",
            "data_opcao_simples",
            "data_exclusao_simples",
            "opcao_mei",
            "data_opcao_mei",
            "data_exclusao_mei",
        ]

        for e in range(0, len(arquivos_simples)):
            logger.info(f"Trabalhando no arquivo: {arquivos_simples[e]}")

            extracted_file_path = os.path.join(extracted_files, arquivos_simples[e])

            tamanho_das_partes = 1000000  # Registros por carga

            parts_task = progress.add_task(
                f"Processando {arquivos_simples[e]}", total=None
            )

            part = 0
            utf8_path = None
            try:
                utf8_path = transcode_to_utf8(extracted_file_path)
                lf = pl.scan_csv(
                    utf8_path,
                    separator=";",
                    has_header=False,
                    new_columns=simples_columns,
                    schema_overrides=[pl.Utf8] * len(simples_columns),
                    encoding="utf8",
                )
                for simples in lf.collect_batches(chunk_size=tamanho_das_partes):
                    if simples.height == 0:
                        continue

                    # Gravar dados no banco usando função assíncrona
                    await to_sql_async(simples, pool, "simples")

                    part += 1
                    logger.info(
                        f"Parte {part} do arquivo {arquivos_simples[e]} inserida com sucesso no banco de dados!"
                    )
                    progress.update(parts_task, advance=1)
                    del simples
                    gc.collect()
            except pl.exceptions.NoDataError:
                logger.info(
                    f"Fim do arquivo {arquivos_simples[e]} alcançado na parte {part}"
                )
            except Exception as ex:
                logger.error(
                    f"Erro ao ler arquivo {arquivos_simples[e]} na parte {part}: {str(ex)}"
                )
            finally:
                if utf8_path and os.path.exists(utf8_path):
                    os.remove(utf8_path)

            logger.info(
                f"Arquivo {arquivos_simples[e]} inserido com sucesso no banco de dados!"
            )
            progress.update(files_task, advance=1)
            progress.remove_task(parts_task)

            # Já carregado no banco — libera espaço em disco
            remove_file_safe(extracted_file_path)

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

    def _read_codigo_descricao(extracted_file_path):
        utf8_path = transcode_to_utf8(extracted_file_path)
        try:
            df = pl.read_csv(
                utf8_path,
                separator=";",
                has_header=False,
                new_columns=["codigo", "descricao"],
                schema_overrides=[pl.Utf8, pl.Utf8],
                encoding="utf8",
            )
        finally:
            os.remove(utf8_path)
        return df.with_columns(pl.col("codigo").cast(pl.Int32, strict=False))

    # Processar CNAE
    if arquivos_cnae:
        for e in range(0, len(arquivos_cnae)):
            extracted_file_path = os.path.join(extracted_files, arquivos_cnae[e])
            try:
                cnae = _read_codigo_descricao(extracted_file_path)
            except pl.exceptions.NoDataError:
                print(f"Arquivo CNAE {arquivos_cnae[e]} está vazio. Pulando...")
                continue
            except Exception as e:
                logger.error(f"Erro ao ler arquivo CNAE {arquivos_cnae[e]}: {str(e)}")
                continue
            await to_sql_async(cnae, pool, "cnae")
            logger.info(f"Arquivo CNAE {arquivos_cnae[e]} inserido!")
            remove_file_safe(extracted_file_path)
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
                    df = _read_codigo_descricao(extracted_file_path)
                except pl.exceptions.NoDataError:
                    print(
                        f"Arquivo {nome_tabela} {arquivo_tipo[e]} está vazio. Pulando..."
                    )
                    continue
                except Exception as e:
                    logger.error(
                        f"Erro ao ler arquivo {nome_tabela} {arquivo_tipo[e]}: {str(e)}"
                    )
                    continue
                await to_sql_async(df, pool, nome_tabela)
                logger.info(f"Arquivo {nome_tabela} {arquivo_tipo[e]} inserido!")
                remove_file_safe(extracted_file_path)
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
        {
            "name": "empresa_razao_social_trgm",
            "table": "empresa",
            "columns": "razao_social (GIN trgm)",
            "sql": "CREATE INDEX IF NOT EXISTS empresa_razao_social_trgm ON empresa USING GIN (razao_social gin_trgm_ops);",
        },
        {
            "name": "estabelecimento_nome_fantasia_trgm",
            "table": "estabelecimento",
            "columns": "nome_fantasia (GIN trgm)",
            "sql": "CREATE INDEX IF NOT EXISTS estabelecimento_nome_fantasia_trgm ON estabelecimento USING GIN (nome_fantasia gin_trgm_ops);",
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
            await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

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
                    # timeout explícito: o pool tem command_timeout=300s (create_db_pool),
                    # que sobrepõe o "SET statement_timeout" acima — sem isso, índices GIN
                    # trgm em tabelas grandes (ex: empresa_razao_social_trgm, 69M+ linhas)
                    # estouram o timeout do lado do cliente antes de terminar no servidor
                    await conn.execute(index_info["sql"], timeout=3600)
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
    _project_root = str(pathlib.Path(__file__).resolve().parent.parent.parent)
    if _project_root not in sys.path:
        sys.path.insert(0, _project_root)
    from src.blue_green.state import StateManager

    console.print("\n[bold magenta]" + "=" * 50 + "[/bold magenta]")
    console.print(
        "[bold magenta]    PROCESSO ETL ASSÍNCRONO INICIADO    [/bold magenta]"
    )
    console.print("[bold magenta]" + "=" * 50 + "[/bold magenta]\n")

    db_target = args.db_target
    state = StateManager()

    console.print(f"[blue]Destino do banco: {db_target}[/blue]")

    start_time = time.time()
    logger.info("Processo ETL iniciado")

    try:
        # Garante que as pastas de trabalho existem. Não limpa o conteúdo:
        # check_diff() já reaproveita downloads cujo tamanho bate com o do
        # servidor, então preservar o diretório evita rebaixar arquivos
        # grandes já validados em execuções anteriores.
        for _folder in [output_files, extracted_files]:
            if _folder:
                makedirs(_folder)

        if args.skip_download:
            console.print(
                "\n[bold yellow]⏭  [FASE 1+2] Puladas (--skip-download) — "
                "usando arquivos já baixados/extraídos em disco[/bold yellow]"
            )
            download_time = 0.0
            extract_time = 0.0
            state.update_staging_downloaded(source_month=f"{mes:02d}-{ano}")
        else:
            # Fase 1: Download paralelo
            console.print(
                "\n[bold yellow]📥 [FASE 1] Download dos arquivos...[/bold yellow]"
            )
            download_start = time.time()
            await download_all_files()
            download_time = time.time() - download_start
            logger.info(f"Download concluído em {download_time:.1f}s")
            console.print(f"[green]✅ Download concluído em {download_time:.1f}s[/green]")

            state.update_staging_downloaded(source_month=f"{mes:02d}-{ano}")

            # Fase 2: Extração paralela
            console.print(
                "\n[bold yellow]📂 [FASE 2] Extração dos arquivos...[/bold yellow]"
            )
            extract_start = time.time()
            await extract_all_files()
            extract_time = time.time() - extract_start
            logger.info(f"Extração concluída em {extract_time:.1f}s")
            console.print(f"[green]✅ Extração concluída em {extract_time:.1f}s[/green]")

        # Categoriza os arquivos extraídos por tipo de tabela — precisa
        # rodar aqui, depois da extração, para não operar sobre listas vazias
        categorize_extracted_files()
        logger.info(
            "Arquivos categorizados: "
            f"empresa={len(arquivos_empresa)}, "
            f"estabelecimento={len(arquivos_estabelecimento)}, "
            f"socios={len(arquivos_socios)}, simples={len(arquivos_simples)}, "
            f"cnae={len(arquivos_cnae)}, motivo={len(arquivos_moti)}, "
            f"municipio={len(arquivos_munic)}, natureza={len(arquivos_natju)}, "
            f"pais={len(arquivos_pais)}, qualificacao={len(arquivos_quals)}"
        )

        # Fase 3: Processamento de dados
        console.print(
            "\n[bold yellow]🗄️  [FASE 3] Processamento e inserção no banco...[/bold yellow]"
        )
        logger.info("Iniciando processamento e inserção no banco")

        # Criar banco de dados se não existir
        await create_database_if_not_exists(db_name=db_target)

        # Criar pool de conexões
        pool = await create_db_pool(db_name=db_target)

        try:
            # Checkpoint precisa ser lido ANTES de configurar as tabelas: se uma
            # execução anterior já carregou algumas tabelas com sucesso, elas
            # não devem ser dropadas/recriadas aqui (setup_tables preserva o
            # que já está pronto)
            checkpoint = load_checkpoint()
            preserve = tables_to_preserve(checkpoint)
            if preserve:
                console.print(
                    f"[yellow]Retomando checkpoint — preservando tabelas já carregadas: "
                    f"{', '.join(sorted(preserve))}[/yellow]"
                )

            # Configurar tabelas
            await setup_tables(pool, preserve_tables=preserve)

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

            if not checkpoint or checkpoint["stage"] in [
                "empresa",
                "empresa_completed",
                "estabelecimento",
                "estabelecimento_completed",
                "socios",
                "socios_completed",
                "simples",
                "simples_completed",
            ]:
                await process_outros_arquivos(pool)
                save_checkpoint("outros_completed")

            # Todos os dados já foram carregados no banco — os ZIPs baixados não
            # servem mais nesta execução (não ajudam numa reexecução futura, já
            # que o dataset da Receita Federal muda todo mês)
            console.print(
                "\n[bold yellow]🧹 Limpando arquivos ZIP já processados...[/bold yellow]"
            )
            for _name in os.listdir(output_files):
                if _name.lower().endswith(".zip"):
                    remove_file_safe(os.path.join(output_files, _name))

            # Criar índices automaticamente
            save_checkpoint("creating_indexes")
            await create_indexes(pool)

            state.update_staging_processed()

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

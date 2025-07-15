import asyncio
import datetime
import gc
import pathlib
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import bs4 as bs
import os
import pandas as pd
import asyncpg
import re
import sys
import time
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
import zipfile


def check_diff(url, file_name):
    '''
    Verifica se o arquivo no servidor existe no disco e se ele tem o mesmo
    tamanho no servidor.
    '''
    if not os.path.isfile(file_name):
        return True # ainda nao foi baixado

    with httpx.Client(headers={'User-Agent': 'Mozilla/5.0'}) as client:
        response = client.head(url)
        new_size = int(response.headers.get('content-length', 0))
        old_size = os.path.getsize(file_name)
        if new_size != old_size:
            os.remove(file_name)
            return True # tamanho diferentes

    return False # arquivos sao iguais


#%%
def makedirs(path):
    '''
    cria path caso seja necessario
    '''
    if not os.path.exists(path):
        os.makedirs(path)

#%%
async def to_sql_async(dataframe, pool, table_name, batch_size=8192):
    '''
    Insere dados de forma assíncrona usando batch inserts otimizados
    '''
    total = len(dataframe)
    columns = list(dataframe.columns)
    
    # Converter DataFrame para lista de tuplas para inserção mais eficiente
    records = [tuple(row) for row in dataframe.values]
    
    # Criar statement de insert
    placeholders = ','.join(['$' + str(i+1) for i in range(len(columns))])
    insert_sql = f'INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})'
    
    async def insert_batch(batch_records, batch_num):
        async with pool.acquire() as conn:
            await conn.executemany(insert_sql, batch_records)
            
        # Progress tracking
        completed = batch_num * batch_size
        percent = min((completed * 100) / total, 100)
        progress = f'{table_name} {percent:.2f}% {completed:0{len(str(total))}}/{total}'
        sys.stdout.write(f'\r{progress}')
        sys.stdout.flush()
    
    # Processar em batches
    tasks = []
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        task = insert_batch(batch, i // batch_size)
        tasks.append(task)
    
    # Executar todos os batches em paralelo (limitando concorrência)
    semaphore = asyncio.Semaphore(10)  # Máximo 10 conexões simultâneas
    
    async def bounded_task(task):
        async with semaphore:
            await task
    
    await asyncio.gather(*[bounded_task(task) for task in tasks])
    sys.stdout.write('\n')

#%%
# Ler arquivo de configuração de ambiente # https://dev.to/jakewitcher/using-env-files-for-environment-variables-in-python-applications-55a1
def getEnv(env):
    return os.getenv(env)


# Carregar variáveis de ambiente automaticamente
current_path = pathlib.Path().resolve()

# Procurar .env primeiro no diretório raiz do projeto (pai do code)
parent_path = current_path.parent
dotenv_path = os.path.join(parent_path, '.env')

# Se não encontrar no diretório pai, verificar no diretório atual
if not os.path.isfile(dotenv_path):
    dotenv_path = os.path.join(current_path, '.env')
    
    # Se não encontrar em lugar nenhum, usar o do diretório pai (raiz do projeto)
    if not os.path.isfile(dotenv_path):
        dotenv_path = os.path.join(parent_path, '.env')
        print('Arquivo .env não encontrado. Verifique se existe um arquivo .env no diretório raiz do projeto.')
        print(f'Procurando em: {dotenv_path}')
        
print(f'Carregando configurações de: {dotenv_path}')
load_dotenv(dotenv_path=dotenv_path)

# URL de referencia da receita para baixar os arquivos .zip  
base_url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-06/"

#%%
# Read details from ".env" file:
output_files = None
extracted_files = None
try:
    output_files = getEnv('OUTPUT_FILES_PATH')
    makedirs(output_files)

    extracted_files = getEnv('EXTRACTED_FILES_PATH')
    makedirs(extracted_files)

    print('Diretórios definidos: \n' +
          'output_files: ' + str(output_files)  + '\n' +
          'extracted_files: ' + str(extracted_files))
except:
    pass
    print('Erro na definição dos diretórios, verifique o arquivo ".env" ou o local informado do seu arquivo de configuração.')

#%%
# Fazer request com httpx
with httpx.Client(headers={'User-Agent': 'Mozilla/5.0'}) as client:
    response = client.get(base_url)
    response.raise_for_status()
    raw_html = response.content

# Formatar página e converter em string
page_items = bs.BeautifulSoup(raw_html, 'lxml')
html_str = str(page_items)

# Obter arquivos
Files = []
text = '.zip'
for m in re.finditer(text, html_str):
    i_start = m.start()-40
    i_end = m.end()
    i_loc = html_str[i_start:i_end].find('href=')+6
    Files.append(html_str[i_start+i_loc:i_end])

# Correcao do nome dos arquivos devido a mudanca na estrutura do HTML da pagina - 31/07/22 - Aphonso Rafael
Files_clean = []
for i in range(len(Files)):
    if not Files[i].find('.zip">') > -1:
        Files_clean.append(Files[i])

try:
    del Files
except:
    pass

Files = Files_clean

print('Arquivos que serão baixados:')
i_f = 0
for f in Files:
    i_f += 1
    print(str(i_f) + ' - ' + f)

#%%
########################################################################################################################
## DOWNLOAD ############################################################################################################
########################################################################################################################
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
def download_file(url, file_path):
    """Download file with retry logic using httpx"""
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    with httpx.Client(headers=headers, timeout=30.0) as client:
        with client.stream('GET', url) as response:
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_bytes(chunk_size=8192):
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        sys.stdout.write(f"\rDownloading: {percent:.2f}% [{downloaded}/{total_size}] bytes")
                        sys.stdout.flush()
            
            print()  # Nova linha após download

#%%
# Download arquivos ################################################################################################################################
i_l = 0
for l in Files:
    # Download dos arquivos
    i_l += 1
    print('Baixando arquivo:')
    print(str(i_l) + ' - ' + l)
    url = base_url + l
    file_name = os.path.join(output_files, l)
    if check_diff(url, file_name):
        try:
            download_file(url, file_name)
            print(f"Arquivo {l} baixado com sucesso!")
        except Exception as e:
            print(f"Erro ao baixar o arquivo {l}: {e}")
            continue

#%%
# Download layout:
# FIXME está pedindo login gov.br
# Layout = 'https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/arquivos/NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf'
# print('Baixando layout:')
# wget.download(Layout, out=output_files, bar=bar_progress)

####################################################################################################################################################

#%%
# Extracting files:
i_l = 0
for l in Files:
    try:
        i_l += 1
        print('Descompactando arquivo:')
        print(str(i_l) + ' - ' + l)
        full_path = os.path.join(output_files, l)
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            zip_ref.extractall(extracted_files)
    except:
        pass

#%%
########################################################################################################################
## LER E INSERIR DADOS #################################################################################################
########################################################################################################################
insert_start = time.time()

# Files:
Items = [name for name in os.listdir(extracted_files) if name.endswith('')]

# Separar arquivos:
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
for i in range(len(Items)):
    if Items[i].find('EMPRE') > -1:
        arquivos_empresa.append(Items[i])
    elif Items[i].find('ESTABELE') > -1:
        arquivos_estabelecimento.append(Items[i])
    elif Items[i].find('SOCIO') > -1:
        arquivos_socios.append(Items[i])
    elif Items[i].find('SIMPLES') > -1:
        arquivos_simples.append(Items[i])
    elif Items[i].find('CNAE') > -1:
        arquivos_cnae.append(Items[i])
    elif Items[i].find('MOTI') > -1:
        arquivos_moti.append(Items[i])
    elif Items[i].find('MUNIC') > -1:
        arquivos_munic.append(Items[i])
    elif Items[i].find('NATJU') > -1:
        arquivos_natju.append(Items[i])
    elif Items[i].find('PAIS') > -1:
        arquivos_pais.append(Items[i])
    elif Items[i].find('QUALS') > -1:
        arquivos_quals.append(Items[i])
    else:
        pass

#%%
# Conectar no banco de dados:
# Dados da conexão com o BD
user=getEnv('DB_USER')
passw=getEnv('DB_PASSWORD')
host=getEnv('DB_HOST')
port=getEnv('DB_PORT')
database=getEnv('DB_NAME')

async def create_db_pool():
    '''
    Cria pool de conexões assíncronas com o PostgreSQL
    '''
    return await asyncpg.create_pool(
        user=user,
        password=passw,
        database=database,
        host=host,
        port=port,
        min_size=5,
        max_size=20,
        command_timeout=60
    )

async def setup_tables(pool):
    '''
    Configura as tabelas necessárias
    '''
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

async def process_empresa_files(pool):
    '''
    Processa arquivos de empresa de forma assíncrona
    '''
    empresa_insert_start = time.time()
    print("""
#######################
## Arquivos de EMPRESA:
#######################
""")

    for e in range(0, len(arquivos_empresa)):
        print('Trabalhando no arquivo: '+arquivos_empresa[e]+' [...]')
        try:
            del empresa
        except:
            pass

        empresa_dtypes = {0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: object}
        extracted_file_path = os.path.join(extracted_files, arquivos_empresa[e])

        empresa = pd.read_csv(filepath_or_buffer=extracted_file_path,
                              sep=';',
                              skiprows=0,
                              header=None,
                              dtype=empresa_dtypes,
                              encoding='latin-1',
        )

    # Tratamento do arquivo antes de inserir na base:
        empresa = empresa.reset_index()
        del empresa['index']

        # Renomear colunas
        empresa.columns = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']

        # Replace "," por "."
        empresa['capital_social'] = empresa['capital_social'].apply(lambda x: x.replace(',','.'))
        empresa['capital_social'] = empresa['capital_social'].astype(float)

        # Gravar dados no banco usando função assíncrona
        await to_sql_async(empresa, pool, 'empresa')
        print('Arquivo ' + arquivos_empresa[e] + ' inserido com sucesso no banco de dados!')

    try:
        del empresa
    except:
        pass
    print('Arquivos de empresa finalizados!')
    empresa_insert_end = time.time()
    empresa_Tempo_insert = round((empresa_insert_end - empresa_insert_start))
    print('Tempo de execução do processo de empresa (em segundos): ' + str(empresa_Tempo_insert))

async def process_estabelecimento_files(pool):
    '''
    Processa arquivos de estabelecimento de forma assíncrona
    '''
    estabelecimento_insert_start = time.time()
    print("""
###############################
## Arquivos de ESTABELECIMENTO:
###############################
""")

    print('Tem %i arquivos de estabelecimento!' % len(arquivos_estabelecimento))
    for e in range(0, len(arquivos_estabelecimento)):
        print('Trabalhando no arquivo: '+arquivos_estabelecimento[e]+' [...]')
    try:
        del estabelecimento
        gc.collect()
    except:
        pass

    # estabelecimento = pd.DataFrame(columns=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28])
    estabelecimento_dtypes = {0: object, 1: object, 2: object, 3: 'Int32', 4: object, 5: 'Int32', 6: 'Int32',
                              7: 'Int32', 8: object, 9: object, 10: 'Int32', 11: 'Int32', 12: object, 13: object,
                              14: object, 15: object, 16: object, 17: object, 18: object, 19: object,
                              20: 'Int32', 21: object, 22: object, 23: object, 24: object, 25: object,
                              26: object, 27: object, 28: object, 29: 'Int32'}
    extracted_file_path = os.path.join(extracted_files, arquivos_estabelecimento[e])

    NROWS = 2000000
    part = 0
    while True:
        estabelecimento = pd.read_csv(filepath_or_buffer=extracted_file_path,
                              sep=';',
                              nrows=NROWS,
                              skiprows=NROWS * part,
                              header=None,
                              dtype=estabelecimento_dtypes,
                              encoding='latin-1',
        )

        # Tratamento do arquivo antes de inserir na base:
        estabelecimento = estabelecimento.reset_index()
        del estabelecimento['index']
        gc.collect()

        # Renomear colunas
        estabelecimento.columns = ['cnpj_basico',
                                   'cnpj_ordem',
                                   'cnpj_dv',
                                   'identificador_matriz_filial',
                                   'nome_fantasia',
                                   'situacao_cadastral',
                                   'data_situacao_cadastral',
                                   'motivo_situacao_cadastral',
                                   'nome_cidade_exterior',
                                   'pais',
                                   'data_inicio_atividade',
                                   'cnae_fiscal_principal',
                                   'cnae_fiscal_secundaria',
                                   'tipo_logradouro',
                                   'logradouro',
                                   'numero',
                                   'complemento',
                                   'bairro',
                                   'cep',
                                   'uf',
                                   'municipio',
                                   'ddd_1',
                                   'telefone_1',
                                   'ddd_2',
                                   'telefone_2',
                                   'ddd_fax',
                                   'fax',
                                   'correio_eletronico',
                                   'situacao_especial',
                                   'data_situacao_especial']

        # Gravar dados no banco:
        # estabelecimento
        to_sql(estabelecimento, name='estabelecimento', con=engine, if_exists='append', index=False)
        print('Arquivo ' + arquivos_estabelecimento[e] + ' / ' + str(part) + ' inserido com sucesso no banco de dados!')
        if len(estabelecimento) == NROWS:
            part += 1
        else:
            break

try:
    del estabelecimento
except:
    pass
print('Arquivos de estabelecimento finalizados!')
estabelecimento_insert_end = time.time()
estabelecimento_Tempo_insert = round((estabelecimento_insert_end - estabelecimento_insert_start))
print('Tempo de execução do processo de estabelecimento (em segundos): ' + str(estabelecimento_Tempo_insert))

#%%
# Arquivos de socios:
socios_insert_start = time.time()
print("""
######################
## Arquivos de SOCIOS:
######################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "socios";')
conn.commit()

for e in range(0, len(arquivos_socios)):
    print('Trabalhando no arquivo: '+arquivos_socios[e]+' [...]')
    try:
        del socios
    except:
        pass

    socios_dtypes = {0: object, 1: 'Int32', 2: object, 3: object, 4: 'Int32', 5: 'Int32', 6: 'Int32',
                     7: object, 8: object, 9: 'Int32', 10: 'Int32'}
    extracted_file_path = os.path.join(extracted_files, arquivos_socios[e])
    socios = pd.read_csv(filepath_or_buffer=extracted_file_path,
                          sep=';',
                          #nrows=100,
                          skiprows=0,
                          header=None,
                          dtype=socios_dtypes,
                          encoding='latin-1',
    )

    # Tratamento do arquivo antes de inserir na base:
    socios = socios.reset_index()
    del socios['index']

    # Renomear colunas
    socios.columns = ['cnpj_basico',
                      'identificador_socio',
                      'nome_socio_razao_social',
                      'cpf_cnpj_socio',
                      'qualificacao_socio',
                      'data_entrada_sociedade',
                      'pais',
                      'representante_legal',
                      'nome_do_representante',
                      'qualificacao_representante_legal',
                      'faixa_etaria']

    # Gravar dados no banco:
    # socios
    to_sql(socios, name='socios', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_socios[e] + ' inserido com sucesso no banco de dados!')

try:
    del socios
except:
    pass
print('Arquivos de socios finalizados!')
socios_insert_end = time.time()
socios_Tempo_insert = round((socios_insert_end - socios_insert_start))
print('Tempo de execução do processo de sócios (em segundos): ' + str(socios_Tempo_insert))

#%%
# Arquivos de simples:
simples_insert_start = time.time()
print("""
################################
## Arquivos do SIMPLES NACIONAL:
################################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "simples";')
conn.commit()

for e in range(0, len(arquivos_simples)):
    print('Trabalhando no arquivo: '+arquivos_simples[e]+' [...]')
    try:
        del simples
    except:
        pass

    # Verificar tamanho do arquivo:
    print('Lendo o arquivo ' + arquivos_simples[e]+' [...]')
    simples_dtypes = ({0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: 'Int32'})
    extracted_file_path = os.path.join(extracted_files, arquivos_simples[e])

    simples_lenght = sum(1 for line in open(extracted_file_path, "r"))
    print('Linhas no arquivo do Simples '+ arquivos_simples[e] +': '+str(simples_lenght))

    tamanho_das_partes = 1000000 # Registros por carga
    partes = round(simples_lenght / tamanho_das_partes)
    nrows = tamanho_das_partes
    skiprows = 0

    print('Este arquivo será dividido em ' + str(partes) + ' partes para inserção no banco de dados')

    for i in range(0, partes):
        print('Iniciando a parte ' + str(i+1) + ' [...]')
        simples = pd.DataFrame(columns=[1,2,3,4,5,6])

        simples = pd.read_csv(filepath_or_buffer=extracted_file_path,
                              sep=';',
                              nrows=nrows,
                              skiprows=skiprows,
                              header=None,
                              dtype=simples_dtypes,
                              encoding='latin-1',
        )

        # Tratamento do arquivo antes de inserir na base:
        simples = simples.reset_index()
        del simples['index']

        # Renomear colunas
        simples.columns = ['cnpj_basico',
                           'opcao_pelo_simples',
                           'data_opcao_simples',
                           'data_exclusao_simples',
                           'opcao_mei',
                           'data_opcao_mei',
                           'data_exclusao_mei']

        skiprows = skiprows+nrows

        # Gravar dados no banco:
        # simples
        to_sql(simples, name='simples', con=engine, if_exists='append', index=False)
        print('Arquivo ' + arquivos_simples[e] + ' inserido com sucesso no banco de dados! - Parte '+ str(i+1))

        try:
            del simples
        except:
            pass

try:
    del simples
except:
    pass

print('Arquivos do simples finalizados!')
simples_insert_end = time.time()
simples_Tempo_insert = round((simples_insert_end - simples_insert_start))
print('Tempo de execução do processo do Simples Nacional (em segundos): ' + str(simples_Tempo_insert))

#%%
# Arquivos de cnae:
cnae_insert_start = time.time()
print("""
######################
## Arquivos de cnae:
######################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "cnae";')
conn.commit()

for e in range(0, len(arquivos_cnae)):
    print('Trabalhando no arquivo: '+arquivos_cnae[e]+' [...]')
    try:
        del cnae
    except:
        pass

    extracted_file_path = os.path.join(extracted_files, arquivos_cnae[e])
    cnae = pd.DataFrame(columns=[1,2])
    cnae = pd.read_csv(filepath_or_buffer=extracted_file_path, sep=';', skiprows=0, header=None, dtype='object', encoding='latin-1')

    # Tratamento do arquivo antes de inserir na base:
    cnae = cnae.reset_index()
    del cnae['index']

    # Renomear colunas
    cnae.columns = ['codigo', 'descricao']

    # Gravar dados no banco:
    # cnae
    to_sql(cnae, name='cnae', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_cnae[e] + ' inserido com sucesso no banco de dados!')

try:
    del cnae
except:
    pass
print('Arquivos de cnae finalizados!')
cnae_insert_end = time.time()
cnae_Tempo_insert = round((cnae_insert_end - cnae_insert_start))
print('Tempo de execução do processo de cnae (em segundos): ' + str(cnae_Tempo_insert))

#%%
# Arquivos de moti:
moti_insert_start = time.time()
print("""
#########################################
## Arquivos de motivos da situação atual:
#########################################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "moti";')
conn.commit()

for e in range(0, len(arquivos_moti)):
    print('Trabalhando no arquivo: '+arquivos_moti[e]+' [...]')
    try:
        del moti
    except:
        pass

    moti_dtypes = ({0: 'Int32', 1: object})
    extracted_file_path = os.path.join(extracted_files, arquivos_moti[e])
    moti = pd.read_csv(filepath_or_buffer=extracted_file_path, sep=';', skiprows=0, header=None, dtype=moti_dtypes, encoding='latin-1')

    # Tratamento do arquivo antes de inserir na base:
    moti = moti.reset_index()
    del moti['index']

    # Renomear colunas
    moti.columns = ['codigo', 'descricao']

    # Gravar dados no banco:
    # moti
    to_sql(moti, name='moti', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_moti[e] + ' inserido com sucesso no banco de dados!')

try:
    del moti
except:
    pass
print('Arquivos de moti finalizados!')
moti_insert_end = time.time()
moti_Tempo_insert = round((moti_insert_end - moti_insert_start))
print('Tempo de execução do processo de motivos da situação atual (em segundos): ' + str(moti_Tempo_insert))

#%%
# Arquivos de munic:
munic_insert_start = time.time()
print("""
##########################
## Arquivos de municípios:
##########################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "munic";')
conn.commit()

for e in range(0, len(arquivos_munic)):
    print('Trabalhando no arquivo: '+arquivos_munic[e]+' [...]')
    try:
        del munic
    except:
        pass

    munic_dtypes = ({0: 'Int32', 1: object})
    extracted_file_path = os.path.join(extracted_files, arquivos_munic[e])
    munic = pd.read_csv(filepath_or_buffer=extracted_file_path, sep=';', skiprows=0, header=None, dtype=munic_dtypes, encoding='latin-1')

    # Tratamento do arquivo antes de inserir na base:
    munic = munic.reset_index()
    del munic['index']

    # Renomear colunas
    munic.columns = ['codigo', 'descricao']

    # Gravar dados no banco:
    # munic
    to_sql(munic, name='munic', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_munic[e] + ' inserido com sucesso no banco de dados!')

try:
    del munic
except:
    pass
print('Arquivos de munic finalizados!')
munic_insert_end = time.time()
munic_Tempo_insert = round((munic_insert_end - munic_insert_start))
print('Tempo de execução do processo de municípios (em segundos): ' + str(munic_Tempo_insert))

#%%
# Arquivos de natju:
natju_insert_start = time.time()
print("""
#################################
## Arquivos de natureza jurídica:
#################################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "natju";')
conn.commit()

for e in range(0, len(arquivos_natju)):
    print('Trabalhando no arquivo: '+arquivos_natju[e]+' [...]')
    try:
        del natju
    except:
        pass

    natju_dtypes = ({0: 'Int32', 1: object})
    extracted_file_path = os.path.join(extracted_files, arquivos_natju[e])
    natju = pd.read_csv(filepath_or_buffer=extracted_file_path, sep=';', skiprows=0, header=None, dtype=natju_dtypes, encoding='latin-1')

    # Tratamento do arquivo antes de inserir na base:
    natju = natju.reset_index()
    del natju['index']

    # Renomear colunas
    natju.columns = ['codigo', 'descricao']

    # Gravar dados no banco:
    # natju
    to_sql(natju, name='natju', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_natju[e] + ' inserido com sucesso no banco de dados!')

try:
    del natju
except:
    pass
print('Arquivos de natju finalizados!')
natju_insert_end = time.time()
natju_Tempo_insert = round((natju_insert_end - natju_insert_start))
print('Tempo de execução do processo de natureza jurídica (em segundos): ' + str(natju_Tempo_insert))

#%%
# Arquivos de pais:
pais_insert_start = time.time()
print("""
######################
## Arquivos de país:
######################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "pais";')
conn.commit()

for e in range(0, len(arquivos_pais)):
    print('Trabalhando no arquivo: '+arquivos_pais[e]+' [...]')
    try:
        del pais
    except:
        pass

    pais_dtypes = ({0: 'Int32', 1: object})
    extracted_file_path = os.path.join(extracted_files, arquivos_pais[e])
    pais = pd.read_csv(filepath_or_buffer=extracted_file_path, sep=';', skiprows=0, header=None, dtype=pais_dtypes, encoding='latin-1')

    # Tratamento do arquivo antes de inserir na base:
    pais = pais.reset_index()
    del pais['index']

    # Renomear colunas
    pais.columns = ['codigo', 'descricao']

    # Gravar dados no banco:
    # pais
    to_sql(pais, name='pais', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_pais[e] + ' inserido com sucesso no banco de dados!')

try:
    del pais
except:
    pass
print('Arquivos de pais finalizados!')
pais_insert_end = time.time()
pais_Tempo_insert = round((pais_insert_end - pais_insert_start))
print('Tempo de execução do processo de país (em segundos): ' + str(pais_Tempo_insert))

#%%
# Arquivos de qualificação de sócios:
quals_insert_start = time.time()
print("""
######################################
## Arquivos de qualificação de sócios:
######################################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "quals";')
conn.commit()

for e in range(0, len(arquivos_quals)):
    print('Trabalhando no arquivo: '+arquivos_quals[e]+' [...]')
    try:
        del quals
    except:
        pass

    quals_dtypes = ({0: 'Int32', 1: object})
    extracted_file_path = os.path.join(extracted_files, arquivos_quals[e])
    quals = pd.read_csv(filepath_or_buffer=extracted_file_path, sep=';', skiprows=0, header=None, dtype=quals_dtypes, encoding='latin-1')

    # Tratamento do arquivo antes de inserir na base:
    quals = quals.reset_index()
    del quals['index']

    # Renomear colunas
    quals.columns = ['codigo', 'descricao']

    # Gravar dados no banco:
    # quals
    to_sql(quals, name='quals', con=engine, if_exists='append', index=False)
    print('Arquivo ' + arquivos_quals[e] + ' inserido com sucesso no banco de dados!')

try:
    del quals
except:
    pass
print('Arquivos de quals finalizados!')
quals_insert_end = time.time()
quals_Tempo_insert = round((quals_insert_end - quals_insert_start))
print('Tempo de execução do processo de qualificação de sócios (em segundos): ' + str(quals_Tempo_insert))

#%%
insert_end = time.time()
Tempo_insert = round((insert_end - insert_start))

print("""
#############################################
## Processo de carga dos arquivos finalizado!
#############################################
""")

print('Tempo total de execução do processo de carga (em segundos): ' + str(Tempo_insert)) # Tempo de execução do processo (em segundos): 17.770 (4hrs e 57 min)

# ###############################
# Tamanho dos arquivos:
# empresa = 45.811.638
# estabelecimento = 48.421.619
# socios = 20.426.417
# simples = 27.893.923
# ###############################

#%%
# Criar índices na base de dados:
index_start = time.time()
print("""
#######################################
## Criar índices na base de dados [...]
#######################################
""")
cur.execute("""
create index if not exists empresa_cnpj on empresa(cnpj_basico);
commit;
create index if not exists estabelecimento_cnpj on estabelecimento(cnpj_basico);
commit;
create index if not exists socios_cnpj on socios(cnpj_basico);
commit;
create index if not exists simples_cnpj on simples(cnpj_basico);
commit;
""")
conn.commit()
print("""
############################################################
## Índices criados nas tabelas, para a coluna `cnpj_basico`:
   - empresa
   - estabelecimento
   - socios
   - simples
############################################################
""")
index_end = time.time()
index_time = round(index_end - index_start)
print('Tempo para criar os índices (em segundos): ' + str(index_time))

async def main():
    '''
    Função principal que executa todo o processo de ETL de forma assíncrona
    '''
    print("Iniciando processo ETL assíncrono...")
    
    # Criar pool de conexões
    pool = await create_db_pool()
    
    try:
        # Configurar tabelas
        await setup_tables(pool)
        
        # Processar arquivos de empresa
        await process_empresa_files(pool)
        
        # Processar outros tipos de arquivo (estabelecimento, simples, etc.)
        # await process_estabelecimento_files(pool)
        # await process_socios_files(pool)
        # ... outros arquivos
        
        print("""Processo 100% finalizado! Você já pode usar seus dados no BD!
 - Desenvolvido por: Aphonso Henrique do Amaral Rafael
 - Contribua com esse projeto aqui: https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ
""")
        
    finally:
        # Fechar pool de conexões
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
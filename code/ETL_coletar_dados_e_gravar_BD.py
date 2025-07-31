import datetime
import gc
import io
import pathlib
from dotenv import load_dotenv
import bs4 as bs
import ftplib
import gzip
import os
import pandas as pd
import psycopg2
import re
import sys
import time
import requests
import urllib.request
import urllib.parse
import wget
import zipfile


def urlopen_with_retry(url):
    """
    Tenta abrir uma URL com um número máximo de retentativas em caso de falha.
    """
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 10
    for attempt in range(MAX_RETRIES):
        try:
            return urllib.request.urlopen(url, timeout=60)
        except urllib.error.URLError as e:
            print(f"AVISO: Falha ao acessar {url}. Erro: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"Aguardando {RETRY_DELAY_SECONDS} segundos antes de tentar novamente...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                print(f"ERRO: Todas as tentativas de conexão com {url} falharam. Abortando.")
                raise  # Relança a exceção para ser tratada pelo chamador

def check_diff(url, file_name):
    '''
    Verifica se o arquivo no servidor existe no disco e se ele tem o mesmo
    tamanho no servidor.
    '''
    if not os.path.isfile(file_name):
        return True # ainda nao foi baixado

    response = requests.head(url)
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

def copy_from_stringio(cursor, df, table_name):
    """
    Usa o método copy_from do psycopg2 para inserir um dataframe do pandas
    em uma tabela do banco de dados de forma muito mais rápida.
    """
    # Salva o dataframe em um buffer na memória
    buffer = io.StringIO()
    # NaN é convertido para string vazia, que será tratada como NULL pelo copy_from
    df.to_csv(buffer, sep=';', header=False, index=False, na_rep='')
    buffer.seek(0) # "rebobina" o buffer para o início

    try:
        # Insere os dados no banco
        # O parâmetro null='' garante que strings vazias sejam convertidas para NULL no BD
        cursor.copy_from(buffer, table_name, sep=';', null='')
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"\nErro ao inserir dados na tabela {table_name}: {error}")
        conn.rollback()
        # Opcional: Sair do script se houver erro
        # sys.exit(1)

#%%
# Ler arquivo de configuração de ambiente # https://dev.to/jakewitcher/using-env-files-for-environment-variables-in-python-applications-55a1
def getEnv(env):
    return os.getenv(env)


# O script deve procurar o .env na mesma pasta em que ele está (a pasta 'code')
script_dir = pathlib.Path(__file__).parent.resolve()
dotenv_path = os.path.join(script_dir, '.env')

# Se o .env não for encontrado, o script para e avisa o usuário.
if not os.path.isfile(dotenv_path):
    print(f"ERRO: Arquivo de configuração '.env' não encontrado em '{script_dir}'.")
    print(r"Por favor, copie o arquivo '.env_template' para '.env' e preencha suas configurações.")
    print(r"Você pode usar os scripts 'setup.sh' ou 'setup.bat' para criar o arquivo automaticamente.")
    sys.exit(1)

print(f"Carregando configurações de: {dotenv_path}")
load_dotenv(dotenv_path=dotenv_path)

dados_rf = getEnv('DADOS_RF_URL')

#%%
# Read details from ".env" file:
output_files = None
extracted_files = None
try:
    output_files = getEnv('OUTPUT_FILES_PATH')
    makedirs(output_files)

    extracted_files = getEnv('EXTRACTED_FILES_PATH')
    makedirs(extracted_files)

    if not dados_rf:
        raise ValueError('A variável DADOS_RF_URL não foi definida no arquivo .env')

    print('Diretórios definidos: \n' +
          'output_files: ' + str(output_files)  + '\n' +
          'extracted_files: ' + str(extracted_files))
    print(f'URL dos dados: {dados_rf}')
except Exception as e:
    print(f'Erro na definição das variáveis de ambiente: {e}, verifique o arquivo ".env" ou o local informado do seu arquivo de configuração.')
    sys.exit(1)

def get_latest_data_url(base_url):
    """
    Acessa a URL base e encontra o link para o diretório de dados mais recente.
    """
    print(f"Buscando diretórios de dados em: {base_url}")
    response = urlopen_with_retry(base_url)
    raw_html = response.read()
    page = bs.BeautifulSoup(raw_html, 'lxml')

    # Encontra todos os links que parecem ser diretórios (terminam com /)
    dir_links = [a['href'] for a in page.find_all('a') if a['href'].endswith('/') and a['href'] != '../']

    if not dir_links:
        print("ERRO: Nenhum diretório de dados encontrado na URL base.")
        sys.exit(1)

    # Assume que o último link é o mais recente (geralmente ordenado)
    latest_dir = sorted(dir_links)[-1]
    latest_data_url = urllib.parse.urljoin(base_url, latest_dir)
    print(f"Diretório de dados mais recente encontrado: {latest_data_url}")
    return latest_data_url

def get_zip_files_from_url(data_url):
    """
    Acessa a URL do diretório de dados e extrai a lista de todos os arquivos .zip.
    """
    print(f"Buscando arquivos .zip em: {data_url}")
    response = urlopen_with_retry(data_url)
    raw_html = response.read()
    page = bs.BeautifulSoup(raw_html, 'lxml')

    zip_files = [a['href'] for a in page.find_all('a') if a['href'].endswith('.zip')]

    if not zip_files:
        print("ERRO: Nenhum arquivo .zip encontrado no diretório de dados.")
        sys.exit(1)

    return zip_files

#%%
# --- Lógica de Descoberta de Arquivos ---
try:
    latest_data_url = get_latest_data_url(dados_rf)
    Files = get_zip_files_from_url(latest_data_url)
    # A URL base para download passa a ser a do diretório de dados mais recente
    dados_rf = latest_data_url
except urllib.error.URLError:
    # A função urlopen_with_retry já imprimiu o erro detalhado.
    # Apenas encerramos o script.
    sys.exit(1)

print('Arquivos que serão baixados:')
i_f = 0
for f in Files:
    i_f += 1
    print(str(i_f) + ' - ' + f)

#%%
########################################################################################################################
## DOWNLOAD ############################################################################################################
########################################################################################################################
# Create this bar_progress method which is invoked automatically from wget:
def bar_progress(current, total, width=80):
  progress_message = "Downloading: %d%% [%d / %d] bytes - " % (current / total * 100, current, total)
  # Don't use print() as it will print in new line every time.
  sys.stdout.write("\r" + progress_message)
  sys.stdout.flush()

#%%
# Download arquivos ################################################################################################################################
i_l = 0
for l in Files:
    # Download dos arquivos
    i_l += 1
    print('Baixando arquivo:')
    print(str(i_l) + ' - ' + l)
    url = dados_rf+l
    file_name = os.path.join(output_files, l)
    if check_diff(url, file_name):
        wget.download(url, out=output_files, bar=bar_progress)

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
    i_l += 1
    print(f'Descompactando arquivo: {i_l} - {l}')
    full_path = os.path.join(output_files, l)
    try:
        with zipfile.ZipFile(full_path, 'r') as zip_ref:
            zip_ref.extractall(extracted_files)
    except zipfile.BadZipFile:
        print(f"AVISO: O arquivo {l} não é um arquivo zip válido ou está corrompido. O arquivo será ignorado.")
    except Exception as e:
        print(f"AVISO: Ocorreu um erro inesperado ao descompactar o arquivo {l}: {e}. O arquivo será ignorado.")

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
for item in Items:
    item_upper = item.upper()
    if 'EMPRESAS' in item_upper:
        arquivos_empresa.append(item)
    elif 'ESTABELECIMENTOS' in item_upper:
        arquivos_estabelecimento.append(item)
    elif 'SOCIOS' in item_upper:
        arquivos_socios.append(item)
    elif 'SIMPLES' in item_upper:
        arquivos_simples.append(item)
    elif 'CNAES' in item_upper:
        arquivos_cnae.append(item)
    elif 'MOTIVOS' in item_upper:
        arquivos_moti.append(item)
    elif 'MUNICIPIOS' in item_upper:
        arquivos_munic.append(item)
    elif 'NATUREZAS' in item_upper:
        arquivos_natju.append(item)
    elif 'PAISES' in item_upper:
        arquivos_pais.append(item)
    elif 'QUALIFICACOES' in item_upper:
        arquivos_quals.append(item)
    else:
        print(f"AVISO: Arquivo '{item}' não classificado e será ignorado.")

#%%
# Conectar no banco de dados:
# Dados da conexão com o BD
user=getEnv('DB_USER')
passw=getEnv('DB_PASSWORD')
host=getEnv('DB_HOST')
port=getEnv('DB_PORT')
database=getEnv('DB_NAME')

# Conectar:
conn = psycopg2.connect('dbname='+database+' '+'user='+user+' '+'host='+host+' '+'port='+port+' '+'password='+passw)
cur = conn.cursor()

# #%%
# # Arquivos de empresa:
empresa_insert_start = time.time()
print("""
#######################
## Arquivos de EMPRESA:
#######################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "empresa";')
conn.commit()

CHUNKSIZE = 1_000_000
empresa_dtypes = {0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: object}
empresa_cols = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']

for e in range(0, len(arquivos_empresa)):
    print('Trabalhando no arquivo: '+arquivos_empresa[e]+' [...]')
    extracted_file_path = os.path.join(extracted_files, arquivos_empresa[e])

    reader = pd.read_csv(filepath_or_buffer=extracted_file_path,
                          sep=';',
                          header=None,
                          dtype=empresa_dtypes,
                          encoding='latin-1',
                          chunksize=CHUNKSIZE,
    )

    for i, chunk in enumerate(reader):
        chunk.columns = empresa_cols
        # Replace "," por "."
        chunk['capital_social'] = chunk['capital_social'].apply(lambda x: str(x).replace(',','.'))
        chunk['capital_social'] = chunk['capital_social'].astype(float)

        # Gravar dados no banco:
        copy_from_stringio(cur, chunk, 'empresa')
        print(f'\rChunk {i} do arquivo {arquivos_empresa[e]} inserido com sucesso no banco de dados!', end='')

    print(f'\nArquivo {arquivos_empresa[e]} finalizado.')

    gc.collect()

print('Arquivos de empresa finalizados!')
empresa_insert_end = time.time()
empresa_Tempo_insert = round((empresa_insert_end - empresa_insert_start))
print('Tempo de execução do processo de empresa (em segundos): ' + str(empresa_Tempo_insert))

#%%
# Arquivos de estabelecimento:
estabelecimento_insert_start = time.time()
print("""
###############################
## Arquivos de ESTABELECIMENTO:
###############################
""")

# Drop table antes do insert
cur.execute('DROP TABLE IF EXISTS "estabelecimento";')
conn.commit()

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
        copy_from_stringio(cur, estabelecimento, 'estabelecimento')
        print(f'\rArquivo {arquivos_estabelecimento[e]} / parte {part} inserido com sucesso!', end='')
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

socios_dtypes = {0: object, 1: 'Int32', 2: object, 3: object, 4: 'Int32', 5: 'Int32', 6: 'Int32',
                 7: object, 8: object, 9: 'Int32', 10: 'Int32'}
socios_cols = ['cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 'cpf_cnpj_socio', 'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal', 'nome_do_representante', 'qualificacao_representante_legal', 'faixa_etaria']

for e in range(0, len(arquivos_socios)):
    print('Trabalhando no arquivo: '+arquivos_socios[e]+' [...]')
    extracted_file_path = os.path.join(extracted_files, arquivos_socios[e])

    reader = pd.read_csv(filepath_or_buffer=extracted_file_path,
                          sep=';',
                          header=None,
                          dtype=socios_dtypes,
                          encoding='latin-1',
                          chunksize=CHUNKSIZE,
    )

    for i, chunk in enumerate(reader):
        chunk.columns = socios_cols
        copy_from_stringio(cur, chunk, 'socios')
        print(f'\rChunk {i} do arquivo {arquivos_socios[e]} inserido com sucesso no banco de dados!', end='')

    print(f'\nArquivo {arquivos_socios[e]} finalizado.')

    gc.collect()

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
        copy_from_stringio(cur, simples, 'simples')
        print(f'\rArquivo {arquivos_simples[e]} / parte {i+1} de {partes} inserido com sucesso!', end='')

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
    copy_from_stringio(cur, cnae, 'cnae')
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
    copy_from_stringio(cur, moti, 'moti')
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
    copy_from_stringio(cur, munic, 'munic')
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
    copy_from_stringio(cur, natju, 'natju')
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
    copy_from_stringio(cur, pais, 'pais')
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
    copy_from_stringio(cur, quals, 'quals')
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

#%%
print("""Processo 100% finalizado! Você já pode usar seus dados no BD!
 - Desenvolvido por: Aphonso Henrique do Amaral Rafael
 - Contribua com esse projeto aqui: https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ
""")
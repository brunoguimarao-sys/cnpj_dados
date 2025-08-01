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

def create_tables(cursor):
    """
    Cria todas as tabelas no banco de dados com base no layout oficial,
    dropando as existentes antes para garantir um ambiente limpo.
    """
    print("Criando/recriando tabelas no banco de dados conforme layout oficial...")

    # DDLs baseados no documento "NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf"
    ddl_commands = {
        "empresa": """CREATE TABLE empresa (
            cnpj_basico VARCHAR(8),
            razao_social VARCHAR,
            natureza_juridica INTEGER,
            qualificacao_responsavel INTEGER,
            capital_social FLOAT,
            porte_empresa INTEGER,
            ente_federativo_responsavel VARCHAR
        );""",
        "estabelecimento": """CREATE TABLE estabelecimento (
            cnpj_basico VARCHAR(8),
            cnpj_ordem VARCHAR(4),
            cnpj_dv VARCHAR(2),
            identificador_matriz_filial INTEGER,
            nome_fantasia VARCHAR,
            situacao_cadastral INTEGER,
            data_situacao_cadastral VARCHAR(8),
            motivo_situacao_cadastral INTEGER,
            nome_cidade_exterior VARCHAR,
            pais VARCHAR,
            data_inicio_atividade VARCHAR(8),
            cnae_fiscal_principal INTEGER,
            cnae_fiscal_secundaria VARCHAR,
            tipo_logradouro VARCHAR,
            logradouro VARCHAR,
            numero VARCHAR,
            complemento VARCHAR,
            bairro VARCHAR,
            cep VARCHAR(8),
            uf VARCHAR(2),
            municipio INTEGER,
            ddd_1 VARCHAR,
            telefone_1 VARCHAR,
            ddd_2 VARCHAR,
            telefone_2 VARCHAR,
            ddd_fax VARCHAR,
            fax VARCHAR,
            correio_eletronico VARCHAR,
            situacao_especial VARCHAR,
            data_situacao_especial VARCHAR(8)
        );""",
        "socios": """CREATE TABLE socios (
            cnpj_basico VARCHAR(8),
            identificador_socio INTEGER,
            nome_socio_razao_social VARCHAR,
            cpf_cnpj_socio VARCHAR(14),
            qualificacao_socio INTEGER,
            data_entrada_sociedade VARCHAR(8),
            pais INTEGER,
            representante_legal VARCHAR(14),
            nome_do_representante VARCHAR,
            qualificacao_representante_legal INTEGER,
            faixa_etaria INTEGER
        );""",
        "simples": """CREATE TABLE simples (
            cnpj_basico VARCHAR(8),
            opcao_pelo_simples VARCHAR(1),
            data_opcao_simples VARCHAR(8),
            data_exclusao_simples VARCHAR(8),
            opcao_mei VARCHAR(1),
            data_opcao_mei VARCHAR(8),
            data_exclusao_mei VARCHAR(8)
        );""",
        "cnae": "CREATE TABLE cnae (codigo INTEGER, descricao VARCHAR);",
        "moti": "CREATE TABLE moti (codigo INTEGER, descricao VARCHAR);",
        "munic": "CREATE TABLE munic (codigo INTEGER, descricao VARCHAR);",
        "natju": "CREATE TABLE natju (codigo INTEGER, descricao VARCHAR);",
        "pais": "CREATE TABLE pais (codigo INTEGER, descricao VARCHAR);",
        "quals": "CREATE TABLE quals (codigo INTEGER, descricao VARCHAR);"
    }

    for table_name, ddl in ddl_commands.items():
        print(f"  - Recriando tabela '{table_name}'...")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        cursor.execute(ddl)

    conn.commit()
    print("Tabelas criadas com sucesso.")

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

    # Filtra os links para encontrar apenas diretórios que seguem o padrão YYYY-MM/
    date_pattern = re.compile(r'^\d{4}-\d{2}/$')
    dir_links = [a['href'] for a in page.find_all('a') if date_pattern.match(a['href'])]

    if not dir_links:
        print("ERRO: Nenhum diretório de dados no formato AAAA-MM/ foi encontrado na URL base.")
        sys.exit(1)

    # Ordena os diretórios e pega o mais recente
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
    # Primeiro, tenta obter a lista de zips da URL base fornecida
    Files = get_zip_files_from_url(dados_rf)
    print("Arquivos .zip encontrados diretamente na URL base.")
except (urllib.error.URLError, SystemExit) as e:
    # Se falhar ou não encontrar zips, assume que é um diretório pai
    # e tenta encontrar o subdiretório mais recente.
    print("Nenhum arquivo .zip encontrado na URL base, procurando por subdiretórios...")
    try:
        latest_data_url = get_latest_data_url(dados_rf)
        Files = get_zip_files_from_url(latest_data_url)
        # A URL base para download passa a ser a do diretório de dados mais recente
        dados_rf = latest_data_url
    except (urllib.error.URLError, SystemExit):
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
    if 'EMPRECSV' in item_upper:
        arquivos_empresa.append(item)
    elif 'ESTABELE' in item_upper:
        arquivos_estabelecimento.append(item)
    elif 'SOCIOCSV' in item_upper:
        arquivos_socios.append(item)
    elif 'SIMPLES.CSV' in item_upper:
        arquivos_simples.append(item)
    elif 'CNAECSV' in item_upper:
        arquivos_cnae.append(item)
    elif 'MOTICSV' in item_upper:
        arquivos_moti.append(item)
    elif 'MUNICCSV' in item_upper:
        arquivos_munic.append(item)
    elif 'NATJUCSV' in item_upper:
        arquivos_natju.append(item)
    elif 'PAISCSV' in item_upper:
        arquivos_pais.append(item)
    elif 'QUALSCSV' in item_upper:
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

# Cria/recria todas as tabelas para garantir um ambiente limpo
create_tables(cur)

def clean_empresa_line(line, delimiter=';'):
    """
    Limpa uma linha do arquivo de empresa, lidando com delimitadores extras
    no campo de razão social.
    """
    parts = line.strip().split(delimiter)
    # A lógica assume que os 5 últimos campos estão sempre corretos.
    # O campo problemático (razão social) está entre o primeiro e os 5 últimos.
    if len(parts) > 7:
        razao_social_parts = parts[1:-5]
        # Junta as partes da razão social e coloca entre aspas para tratar como um campo único
        razao_social = f'"{" ".join(razao_social_parts)}"'
        # Remonta a linha com a razão social corrigida
        corrected_parts = [parts[0]] + [razao_social] + parts[-5:]
        return delimiter.join(corrected_parts) + '\n'
    return line

def empresa_chunk_generator(filepath, chunksize):
    """
    Um gerador que lê um arquivo de empresa em pedaços, limpa as linhas
    e retorna um DataFrame do pandas para cada pedaço.
    """
    buffer = io.StringIO()
    lines_in_buffer = 0
    with open(filepath, 'r', encoding='latin-1') as f:
        for line in f:
            buffer.write(clean_empresa_line(line))
            lines_in_buffer += 1
            if lines_in_buffer >= chunksize:
                buffer.seek(0)
                yield pd.read_csv(buffer, sep=';', header=None, names=empresa_cols, dtype=empresa_dtypes, quotechar='"')
                # Reseta o buffer
                buffer.close()
                buffer = io.StringIO()
                lines_in_buffer = 0
    # Processa o que sobrou no buffer
    if lines_in_buffer > 0:
        buffer.seek(0)
        yield pd.read_csv(buffer, sep=';', header=None, names=empresa_cols, dtype=empresa_dtypes, quotechar='"')
        buffer.close()

# #%%
# # Arquivos de empresa:
empresa_insert_start = time.time()
print("""
#######################
## Arquivos de EMPRESA:
#######################
""")

CHUNKSIZE = 500_000 # Reduzido para acomodar o pré-processamento em memória
# Schema para EMPRESAS
empresa_cols = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']
empresa_dtypes = {
    'cnpj_basico': str,
    'razao_social': str,
    'natureza_juridica': 'Int32',
    'qualificacao_responsavel': 'Int32',
    'capital_social': str, # Lemos como string para tratar a vírgula
    'porte_empresa': 'Int32',
    'ente_federativo_responsavel': str
}

for e in range(0, len(arquivos_empresa)):
    print('Trabalhando no arquivo: '+arquivos_empresa[e]+' [...]')
    extracted_file_path = os.path.join(extracted_files, arquivos_empresa[e])

    reader = empresa_chunk_generator(extracted_file_path, CHUNKSIZE)

    for i, chunk in enumerate(reader):
        chunk['capital_social'] = chunk['capital_social'].str.replace(',', '.').astype(float)
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


print('Tem %i arquivos de estabelecimento!' % len(arquivos_estabelecimento))
for e in range(0, len(arquivos_estabelecimento)):
    print('Trabalhando no arquivo: '+arquivos_estabelecimento[e]+' [...]')
    try:
        del estabelecimento
        gc.collect()
    except:
        pass

    # Schema para ESTABELECIMENTOS
    est_cols = [
        'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia',
        'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais',
        'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro',
        'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio',
        'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_fax', 'fax',
        'correio_eletronico', 'situacao_especial', 'data_situacao_especial'
    ]
    est_dtypes = {col: str for col in est_cols} # Ler tudo como string para evitar erros de tipo

    extracted_file_path = os.path.join(extracted_files, arquivos_estabelecimento[e])

    reader = pd.read_csv(
        filepath_or_buffer=extracted_file_path,
        sep=';',
        header=None,
        names=est_cols,
        dtype=est_dtypes,
        encoding='latin-1',
        chunksize=CHUNKSIZE,
        quotechar='"',
        engine='python'
    )

    for i, chunk in enumerate(reader):
        # Nenhum tratamento extra necessário por enquanto
        copy_from_stringio(cur, chunk, 'estabelecimento')
        print(f'\rChunk {i} do arquivo {arquivos_estabelecimento[e]} inserido com sucesso!', end='')

    print(f'\nArquivo {arquivos_estabelecimento[e]} finalizado.')
    gc.collect()

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


# Schema para SOCIOS
socios_cols = [
    'cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 'cpf_cnpj_socio', 'qualificacao_socio',
    'data_entrada_sociedade', 'pais', 'representante_legal', 'nome_do_representante',
    'qualificacao_representante_legal', 'faixa_etaria'
]
socios_dtypes = {col: str for col in socios_cols} # Ler tudo como string

for e in range(0, len(arquivos_socios)):
    print('Trabalhando no arquivo: '+arquivos_socios[e]+' [...]')
    extracted_file_path = os.path.join(extracted_files, arquivos_socios[e])

    reader = pd.read_csv(filepath_or_buffer=extracted_file_path,
                          sep=';',
                          header=None,
                          names=socios_cols,
                          dtype=socios_dtypes,
                          encoding='latin-1',
                          chunksize=CHUNKSIZE,
                          quotechar='"',
                          engine='python'
    )

    for i, chunk in enumerate(reader):
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


for e in range(0, len(arquivos_simples)):
    print('Trabalhando no arquivo: '+arquivos_simples[e]+' [...]')
    try:
        del simples
    except:
        pass

    # Schema para SIMPLES
    simples_cols = [
        'cnpj_basico', 'opcao_pelo_simples', 'data_opcao_simples', 'data_exclusao_simples',
        'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei'
    ]
    simples_dtypes = {col: str for col in simples_cols}

    # Verificar tamanho do arquivo:
    print('Lendo o arquivo ' + arquivos_simples[e]+' [...]')
    extracted_file_path = os.path.join(extracted_files, arquivos_simples[e])

    simples_lenght = sum(1 for line in open(extracted_file_path, "r", encoding='latin-1'))
    print('Linhas no arquivo do Simples '+ arquivos_simples[e] +': '+str(simples_lenght))

    tamanho_das_partes = 1000000 # Registros por carga
    partes = round(simples_lenght / tamanho_das_partes)
    nrows = tamanho_das_partes
    skiprows = 0

    print('Este arquivo será dividido em ' + str(partes) + ' partes para inserção no banco de dados')

    for i in range(0, partes):
        print('Iniciando a parte ' + str(i+1) + ' [...]')
        simples = pd.read_csv(filepath_or_buffer=extracted_file_path,
                              sep=';',
                              nrows=nrows,
                              skiprows=skiprows,
                              header=None,
                              names=simples_cols,
                              dtype=simples_dtypes,
                              encoding='latin-1',
                              quotechar='"',
                              engine='python'
        )

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
def process_lookup_table(files, table_name, file_pattern, columns, dtypes):
    """
    Função genérica para processar tabelas de lookup (domínio).
    """
    insert_start = time.time()
    print(f"\n######################\n## Arquivos de {table_name.upper()}:\n######################")

    for e in range(0, len(files)):
        print(f'Trabalhando no arquivo: {files[e]} [...]')
        extracted_file_path = os.path.join(extracted_files, files[e])

        df = pd.read_csv(
            filepath_or_buffer=extracted_file_path,
            sep=';',
            header=None,
            names=columns,
            dtype=dtypes,
            encoding='latin-1',
            quotechar='"',
            engine='python'
        )

        copy_from_stringio(cur, df, table_name)
        print(f'Arquivo {files[e]} inserido com sucesso na tabela {table_name}!')

    insert_end = time.time()
    tempo_insert = round(insert_end - insert_start)
    print(f'Arquivos de {table_name} finalizados! Tempo de execução (em segundos): {tempo_insert}')

# Processar todas as tabelas de lookup
lookup_columns = ['codigo', 'descricao']
lookup_dtypes = {'codigo': 'Int32', 'descricao': str}

process_lookup_table(arquivos_cnae, 'cnae', 'CNAECSV', lookup_columns, lookup_dtypes)
process_lookup_table(arquivos_moti, 'moti', 'MOTICSV', lookup_columns, lookup_dtypes)
process_lookup_table(arquivos_munic, 'munic', 'MUNICCSV', lookup_columns, lookup_dtypes)
process_lookup_table(arquivos_natju, 'natju', 'NATJUCSV', lookup_columns, lookup_dtypes)
process_lookup_table(arquivos_pais, 'pais', 'PAISCSV', lookup_columns, lookup_dtypes)
process_lookup_table(arquivos_quals, 'quals', 'QUALSCSV', lookup_columns, lookup_dtypes)

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
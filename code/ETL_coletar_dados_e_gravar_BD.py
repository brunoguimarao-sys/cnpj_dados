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
    # DDLs com tipos de dados flexíveis (VARCHAR) para evitar erros de parsing na carga.
    # A conversão de tipos deve ser feita posteriormente no próprio banco de dados.
    ddl_commands = {
        "empresa": """CREATE TABLE empresa (
            cnpj_basico VARCHAR,
            razao_social VARCHAR,
            natureza_juridica VARCHAR,
            qualificacao_responsavel VARCHAR,
            capital_social VARCHAR,
            porte_empresa VARCHAR,
            ente_federativo_responsavel VARCHAR
        );""",
        "estabelecimento": """CREATE TABLE estabelecimento (
            cnpj_basico VARCHAR,
            cnpj_ordem VARCHAR,
            cnpj_dv VARCHAR,
            identificador_matriz_filial VARCHAR,
            nome_fantasia VARCHAR,
            situacao_cadastral VARCHAR,
            data_situacao_cadastral VARCHAR,
            motivo_situacao_cadastral VARCHAR,
            nome_cidade_exterior VARCHAR,
            pais VARCHAR,
            data_inicio_atividade VARCHAR,
            cnae_fiscal_principal VARCHAR,
            cnae_fiscal_secundaria VARCHAR,
            tipo_logradouro VARCHAR,
            logradouro VARCHAR,
            numero VARCHAR,
            complemento VARCHAR,
            bairro VARCHAR,
            cep VARCHAR,
            uf VARCHAR,
            municipio VARCHAR,
            ddd_1 VARCHAR,
            telefone_1 VARCHAR,
            ddd_2 VARCHAR,
            telefone_2 VARCHAR,
            ddd_fax VARCHAR,
            fax VARCHAR,
            correio_eletronico VARCHAR,
            situacao_especial VARCHAR,
            data_situacao_especial VARCHAR
        );""",
        "socios": """CREATE TABLE socios (
            cnpj_basico VARCHAR,
            identificador_socio VARCHAR,
            nome_socio_razao_social VARCHAR,
            cpf_cnpj_socio VARCHAR,
            qualificacao_socio VARCHAR,
            data_entrada_sociedade VARCHAR,
            pais VARCHAR,
            representante_legal VARCHAR,
            nome_do_representante VARCHAR,
            qualificacao_representante_legal VARCHAR,
            faixa_etaria VARCHAR
        );""",
        "simples": """CREATE TABLE simples (
            cnpj_basico VARCHAR,
            opcao_pelo_simples VARCHAR,
            data_opcao_simples VARCHAR,
            data_exclusao_simples VARCHAR,
            opcao_mei VARCHAR,
            data_opcao_mei VARCHAR,
            data_exclusao_mei VARCHAR
        );""",
        "cnae": "CREATE TABLE cnae (codigo VARCHAR, descricao VARCHAR);",
        "moti": "CREATE TABLE moti (codigo VARCHAR, descricao VARCHAR);",
        "munic": "CREATE TABLE munic (codigo VARCHAR, descricao VARCHAR);",
        "natju": "CREATE TABLE natju (codigo VARCHAR, descricao VARCHAR);",
        "pais": "CREATE TABLE pais (codigo VARCHAR, descricao VARCHAR);",
        "quals": "CREATE TABLE quals (codigo VARCHAR, descricao VARCHAR);"
    }

    for table_name, ddl in ddl_commands.items():
        print(f"  - Recriando tabela '{table_name}'...")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        cursor.execute(ddl)

    conn.commit()
    print("Tabelas criadas com sucesso.")

def sanitize_file(filepath, num_expected_columns):
    """
    Lê um arquivo CSV malformado, corrige erros linha por linha e retorna um buffer
    em memória com os dados limpos.

    Erros tratados:
    - Quebras de linha e aspas dentro dos campos.
    - Número de colunas maior que o esperado (delimitador extra em um campo).
    - Número de colunas menor que o esperado (delimitador faltando).
    """
    print(f"    - Higienizando o arquivo: {os.path.basename(filepath)}")
    clean_buffer = io.StringIO()
    with open(filepath, 'r', encoding='latin-1') as f:
        for line in f:
            # 1. Remove quebras de linha e aspas problemáticas
            cleaned_line = line.replace('\n', '').replace('\r', '').replace('"', '')

            # 2. Divide a linha
            parts = cleaned_line.split(';')

            # 3. Corrige o número de colunas
            if len(parts) > num_expected_columns:
                # Heurística para colunas extras: assume que o problema está no segundo campo (razão social/nome)
                # e junta o excesso nele.
                reconstructed_field = " ".join(parts[1 : len(parts) - (num_expected_columns - 2)])
                corrected_parts = [parts[0]] + [reconstructed_field] + parts[-(num_expected_columns - 2):]
                final_line = ";".join(corrected_parts)
            elif len(parts) < num_expected_columns:
                # Preenche com campos vazios se faltarem colunas
                corrected_parts = parts + [''] * (num_expected_columns - len(parts))
                final_line = ";".join(corrected_parts)
            else:
                final_line = ";".join(parts)

            clean_buffer.write(final_line + '\n')

    clean_buffer.seek(0)
    return clean_buffer

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

# Definição dos schemas para cada tipo de arquivo
schemas = {
    'empresa': {
        'cols': ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel'],
        'dtype': {col: str for col in ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']}
    },
    'estabelecimento': {
        'cols': [
            'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia',
            'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais',
            'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro',
            'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio',
            'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_fax', 'fax',
            'correio_eletronico', 'situacao_especial', 'data_situacao_especial'
        ],
        'dtype': {col: str for col in [
            'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial', 'nome_fantasia',
            'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais',
            'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro',
            'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio',
            'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_fax', 'fax',
            'correio_eletronico', 'situacao_especial', 'data_situacao_especial'
        ]}
    },
    'socios': {
        'cols': [
            'cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 'cpf_cnpj_socio', 'qualificacao_socio',
            'data_entrada_sociedade', 'pais', 'representante_legal', 'nome_do_representante',
            'qualificacao_representante_legal', 'faixa_etaria'
        ],
        'dtype': {col: str for col in [
            'cnpj_basico', 'identificador_socio', 'nome_socio_razao_social', 'cpf_cnpj_socio', 'qualificacao_socio',
            'data_entrada_sociedade', 'pais', 'representante_legal', 'nome_do_representante',
            'qualificacao_representante_legal', 'faixa_etaria'
        ]}
    },
    'simples': {
        'cols': [
            'cnpj_basico', 'opcao_pelo_simples', 'data_opcao_simples', 'data_exclusao_simples',
            'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei'
        ],
        'dtype': {col: str for col in [
            'cnpj_basico', 'opcao_pelo_simples', 'data_opcao_simples', 'data_exclusao_simples',
            'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei'
        ]}
    },
    'cnae': {'cols': ['codigo', 'descricao'], 'dtype': {'codigo': str, 'descricao': str}},
    'moti': {'cols': ['codigo', 'descricao'], 'dtype': {'codigo': str, 'descricao': str}},
    'munic': {'cols': ['codigo', 'descricao'], 'dtype': {'codigo': str, 'descricao': str}},
    'natju': {'cols': ['codigo', 'descricao'], 'dtype': {'codigo': str, 'descricao': str}},
    'pais': {'cols': ['codigo', 'descricao'], 'dtype': {'codigo': str, 'descricao': str}},
    'quals': {'cols': ['codigo', 'descricao'], 'dtype': {'codigo': str, 'descricao': str}}
}

# Mapeamento de arquivos para seus schemas
file_to_schema_map = {
    'empresa': arquivos_empresa,
    'estabelecimento': arquivos_estabelecimento,
    'socios': arquivos_socios,
    'simples': arquivos_simples,
    'cnae': arquivos_cnae,
    'moti': arquivos_moti,
    'munic': arquivos_munic,
    'natju': arquivos_natju,
    'pais': arquivos_pais,
    'quals': arquivos_quals,
}

CHUNKSIZE = 500_000

def process_table(table_name, files, schema):
    """
    Função genérica para processar e carregar dados de qualquer tabela.
    """
    insert_start = time.time()
    print(f"\n#######################\n## Arquivos de {table_name.upper()}:\n#######################")

    total_chunks = 0
    for file_name in files:
        print(f'Trabalhando no arquivo: {file_name} [...]')
        extracted_file_path = os.path.join(extracted_files, file_name)

        # Sanitiza o arquivo antes de processar
        sanitized_buffer = sanitize_file(extracted_file_path, len(schema['cols']))

        # Pula o cabeçalho se o buffer não for vazio
        if sanitized_buffer.tell() > 0:
            sanitized_buffer.seek(0)

        reader = pd.read_csv(
            sanitized_buffer,
            sep=';',
            header=None,
            names=schema['cols'],
            dtype=schema['dtype'],
            encoding='latin-1',
            quotechar='"',
            chunksize=CHUNKSIZE,
            on_bad_lines='warn' # A sanitização deve prevenir isso, mas é uma segurança extra
        )

        for i, chunk in enumerate(reader):
            # O pós-processamento específico (como conversão de capital social) foi removido
            # porque a tabela agora aceita VARCHAR. A conversão de tipo deve ser feita no banco de dados.
            copy_from_stringio(cur, chunk, table_name)
            total_chunks += 1
            print(f'\rChunk {i+1} do arquivo {file_name} inserido com sucesso!', end='')

        print(f'\nArquivo {file_name} finalizado.')
        gc.collect()

    insert_end = time.time()
    tempo_insert = round(insert_end - insert_start)
    print(f'Arquivos de {table_name} finalizados! {total_chunks} chunks inseridos. Tempo de execução: {tempo_insert}s')

# Executa o processo para todas as tabelas
for table_name, files in file_to_schema_map.items():
    if files: # Apenas processa se houver arquivos para a tabela
        process_table(table_name, files, schemas[table_name])

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
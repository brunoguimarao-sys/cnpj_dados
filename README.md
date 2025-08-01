# Dados Públicos CNPJ da Receita Federal

Este repositório contém um pipeline de ETL (Extract, Transform, Load) para baixar, descompactar, tratar e inserir os dados públicos de CNPJ da Receita Federal do Brasil em um banco de dados PostgreSQL.

O processo é projetado para ser robusto e performático, mesmo lidando com dezenas de gigabytes de dados.

- **Fonte Oficial dos Dados:** [Cadastro Nacional da Pessoa Jurídica - CNPJ](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)
- **Layout dos Arquivos:** [Metadados CNPJ](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf)

---

### Melhorias Recentes
Esta versão do projeto inclui as seguintes melhorias:
- **Performance Acelerada:** A inserção de dados no banco de dados foi otimizada em ordens de magnitude, trocando a abordagem linha a linha por um carregamento em massa (`bulk-loading`).
- **Uso de Memória Reduzido:** O processamento de arquivos grandes agora é feito em pedaços (`chunks`), permitindo que o ETL rode em máquinas com menos recursos de RAM.
- **Tratamento de Erros Aprimorado:** O script agora detecta arquivos corrompidos durante a descompactação e reporta o erro, em vez de falhar silenciosamente.
- **Configuração Simplificada:** A URL de download dos dados foi movida para o arquivo de configuração, facilitando futuras atualizações.
- **Downloads Inteligentes:** O script verifica se um arquivo já existe localmente e compara seu tamanho com o do servidor. O download só é refeito se o arquivo for novo ou tiver sido atualizado na fonte, economizando tempo e banda.

---

### Infraestrutura Necessária
- [Python 3.8+](https://www.python.org/downloads/)
- [PostgreSQL 14+](https://www.postgresql.org/download/)

---

### Como Usar

#### 1. Configuração do Banco de Dados
- Com o PostgreSQL instalado e rodando, crie a base de dados que será usada pelo projeto. O script original sugere o nome `Dados_RFB`, que pode ser criado com o comando abaixo:
  ```sql
  CREATE DATABASE "Dados_RFB" WITH OWNER = postgres ENCODING = 'UTF8';
  ```

#### 2. Configuração do Ambiente
- Navegue até a pasta `code` e renomeie o arquivo `.env_template` para `.env`.
- Abra o arquivo `.env` e preencha as variáveis com as suas configurações locais (caminhos de pasta e credenciais do banco de dados).

  ```dotenv
  # Caminho para armazenar os arquivos .zip baixados
  OUTPUT_FILES_PATH=C:\temp\dados_rfb\output
  # Caminho para armazenar os arquivos extraídos
  EXTRACTED_FILES_PATH=C:\temp\dados_rfb\extracted

  # URL de onde os dados serão baixados (geralmente não precisa mudar)
  DADOS_RF_URL=http://200.152.38.155/CNPJ/

  # Credenciais do seu banco de dados PostgreSQL
  DB_HOST=localhost
  DB_PORT=5432
  DB_USER=postgres
  DB_PASSWORD=sua_senha_secreta
  DB_NAME=Dados_RFB
  ```

#### 3. Instalação das Dependências
- Instale todas as bibliotecas Python necessárias com um único comando:
  ```bash
  pip install -r requirements.txt
  ```

#### 4. Execução do Pipeline
- Execute o script principal de ETL e aguarde a finalização.
  ```bash
  python code/ETL_coletar_dados_e_gravar_BD.py
  ```
- **Atenção:** O processo é longo e pode levar várias horas, dependendo da sua conexão com a internet e da performance do seu computador. O volume de dados é de aproximadamente 5 GB compactados e mais de 17 GB descompactados.

---

### Tabelas Geradas
O processo de ETL criará e populará as seguintes tabelas no seu banco de dados:
- `empresa`: Dados cadastrais da empresa em nível de matriz.
- `estabelecimento`: Dados analíticos por unidade/estabelecimento (endereço, telefones, etc.).
- `socios`: Dados cadastrais dos sócios das empresas.
- `simples`: Informações sobre MEI e Simples Nacional.
- `cnae`: Tabela de códigos e descrições de atividades econômicas.
- `quals`: Tabela de qualificação dos sócios e responsáveis.
- `natju`: Tabela de naturezas jurídicas.
- `moti`: Tabela de motivos da situação cadastral.
- `pais`: Tabela de países.
- `munic`: Tabela de municípios.

Para otimizar consultas, as tabelas principais (`empresa`, `estabelecimento`, `socios`, `simples`) são indexadas pela coluna `cnpj_basico`.

### Modelo de Entidade e Relacionamento
![alt text](https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ/blob/master/Dados_RFB_ERD.png)
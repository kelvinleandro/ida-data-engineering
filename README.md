# Análise de Indicadores de Desempenho no Atendimento (IDA)

## Visão Geral

Este projeto implementa um Data Mart para os Índices de Desempenho no Atendimento (IDA) de grupos econômicos de telecomunicações no Brasil, com foco nos serviços de Telefonia Celular (SMP), Telefonia Fixa Local (STFC) e Banda Larga Fixa (SCM). Inclui um processo de ETL para carregar os dados, uma view para análise da taxa de variação de indicadores e está totalmente containerizado usando Docker e Docker Compose.

## Descrição do Problema

O projeto atende à necessidade de compilar e analisar os Índices de Desempenho no Atendimento (IDA) de grupos econômicos de telecomunicações, utilizando dados públicos da Anatel. O objetivo é permitir consultas rápidas e a criação de análises específicas, como a variação da "Taxa de Resolvidas em 5 dias úteis".

## Funcionalidades

* **Data Mart em Estrela:** Armazenamento otimizado dos dados de IDA no PostgreSQL.
* **ETL Automatizado:** Script Python para extrair dados de arquivos `.ods`, transformá-los e carregá-los no Data Mart.
* **View de Análise Específica:** Uma view SQL (`view_taxa_variacao_resolvidas_5_dias`) que calcula:
    * A taxa de variação mensal da "Taxa de Resolvidas em 5 dias úteis" (média entre serviços) para cada grupo econômico.
    * A taxa de variação média mensal entre todos os grupos.
    * A diferença da taxa de variação individual de cada grupo em relação à média, com grupos pivotados em colunas.
* **Containerização Completa:** Ambiente totalmente configurado com Docker e Docker Compose para fácil execução e portabilidade.

## Estrutura do Projeto

```
anatel_ida_project/
|
├── anatel_etl/
│   ├── __init__.py
│   ├── main.py                 # Ponto de entrada para executar o pipeline ETL
│   ├── config.py               # Configurações (conexão DB, diretório de dados, etc.)
│   ├── db_manager.py           # Classe para interagir com o PostgreSQL
│   └── etl_processor.py        # Classe para processar cada arquivo de dados
|
├── data/                       # Diretório para os arquivos de dados de entrada (.ods)
│   ├── SMPYYYY.ods             # Ex: SMP2015.ods
│   ├── SCMYYYY.ods             # Ex: SCM2015.ods
│   └── STFCYYYY.ods            # Ex: STFC2015.ods
|
├── sql/
│   └── init.sql                # Script DDL para criar o schema, tabelas e a view
|
├── .dockerignore
├── Dockerfile                  # Define a imagem Docker para a aplicação Python ETL
├── docker-compose.yml
├── requirements.txt
```

## Tecnologias Utilizadas

- **Banco de Dados:** PostgreSQL (Imagem Docker: `postgres:17.5-bookworm`)
- **Linguagem ETL:** Python 3.11 (Imagem Docker: `python:3.11.12-bookworm`)
- **Bibliotecas Python:**
    - `pandas` (manipulação de dados)
    - `psycopg2-binary` (conector PostgreSQL)
    - `odfpy` (para leitura de arquivos `.ods`)
- **Containerização:** Docker, Docker Compose

## Configuração

Siga os passos abaixo para configurar o ambiente do projeto:

1.  **Clonar o Repositório:**
    ```bash
    git clone https://github.com/kelvinleandro/ida-data-engineering.git
    cd ida-data-engineering
    ```

2.  **Arquivo de Configuração `.env`:**
    Copie o arquivo de exemplo `.env.example` para criar seu próprio arquivo de configuração local.

    No seu terminal, na raiz do projeto, execute:

    ```bash
    cp .env.example .env
    ```

    Após copiar, abra o novo arquivo `.env` e substitua os valores de exemplo.

## Como Executar

Com o Docker e Docker Compose instalados e as configurações acima realizadas:

1.  **Construir as imagens e iniciar os contêineres:**
    Na raiz do projeto (`anatel_ida_project/`), execute:
    ```bash
    docker compose up --build
    ```
    - Na primeira execução, isso irá:
        - Baixar a imagem do PostgreSQL.
        - Construir a imagem da aplicação Python ETL.
        - Iniciar os contêineres do PostgreSQL e da aplicação ETL.
        - O PostgreSQL executará o `sql/init.sql` para criar a estrutura do banco.
        - A aplicação ETL tentará se conectar ao banco, encontrar os arquivos de dados em `./data` e processá-los.

2.  **Acompanhar os Logs:**
    Você verá os logs de ambos os serviços no terminal. Acompanhe os logs do serviço `etl` para verificar se os dados foram carregados com sucesso.

3.  **Acessando o Banco de Dados (via `psql`):**
    1. **Abra um novo terminal** e execute o comando abaixo para se conectar ao contêiner do banco de dados. Substitua POSTGRES_USER e POSTGRES_DB pelos valores que você definiu no seu arquivo .env.
        ```bash
        docker compose exec -it db psql -U POSTGRES_USER -d POSTGRES_DB
        ```

    2. Dentro do `psql`, use os seguintes comandos para verificar se tudo foi criado corretamente:
        - Listar schemas:
            ```bash
            \dn
            ```

        - Listar tabelas:
            ```bash
            \dt anatel_datamart.*
            ```

        - Listar views:
            ```bash
            \dv anatel_datamart.*
            ```

    3. Realize consultas SQL, como por exemplo:
        ```bash
        SELECT * FROM anatel_datamart.fact_indicador_desempenho LIMIT 5;
        ```

        ```bash
        SELECT * FROM anatel_datamart.view_taxa_variacao_resolvidas_5_dias LIMIT 5;
        ```

    4. Para sair do `psql`, digite `\q` e pressione Enter.

4.  **Parando os Contêineres:**
    Pressione `Ctrl+C` no terminal onde o `docker compose up` está rodando, ou execute em outro terminal:
    ```bash
    docker compose down
    ```
    Para remover os volumes (incluindo os dados do banco, para um reinício limpo), use:
    ```bash
    docker compose down -v
    ```
# Pipeline de Despesas

Pipeline de dados que consome uma API REST de controle de despesas pessoais, carrega os dados no Snowflake e os transforma em camadas analíticas com dbt, expondo os resultados no Looker Studio.

<a href='https://datastudio.google.com/reporting/b1d19823-5d2b-4de3-a0b9-cb72b5ea2ba5'> Acesse o dashboard </a>

## Arquitetura

```
API REST (Spring Boot)
        │
        ▼
   Apache Airflow       ← orquestra o pipeline
        │
        ├── get_max_id  ← busca o último ID carregado no Snowflake
        ├── extract     ← consome GET /despesa/listar/min/{id}
        └── load        ← INSERT incremental no Snowflake
                │
                ▼
          Snowflake (RAW.DESPESAS_RAW)
                │
                ▼
              dbt
                ├── staging/stg_despesas
                ├── intermediate/int_despesas_parceladas
                └── marts/fct_despesas + dim_parcelamento
                        │
                        ▼
                  Looker Studio
```

## Tecnologias

- **Java / Spring Boot** — API REST de controle de despesas
- **Python** — DAG do Airflow
- **Apache Airflow** — orquestração do pipeline
- **Docker** — containerização da API e do banco
- **AWS EC2** — hospedagem do Airflow e da API
- **Snowflake** — data warehouse
- **dbt** — transformação e modelagem dos dados
- **Looker Studio** — visualização

## Estrutura do Projeto

```
despesas-pipeline/
├── dags/
│   └── dag_despesas.py
├── dbt/
│   └── despesas_pipeline/
│       ├── dbt_project.yml
│       ├── packages.yml
│       └── models/
│           ├── staging/
│           │   ├── sources.yml
│           │   └── stg_despesas.sql
│           ├── intermediate/
│           │   └── int_despesas_parceladas.sql
│           └── marts/
│               ├── fct_despesas.sql
│               └── dim_parcelamento.sql
└── requirements.txt
```

## Modelagem

### Snowflake RAW

Tabela `DESPESAS_RAW` — dados brutos carregados pela DAG, com todos os campos da API flattenizados (despesa + parcelamento em uma única linha).

### dbt

**staging** → `stg_despesas` — limpeza e renomeação de colunas, materializado como view.

**intermediate** → `int_despesas_parceladas` — filtra apenas despesas parceladas e deriva colunas analíticas: `parcelas_restantes` e `valor_restante`.

**marts** → tabelas finais materializadas como table:
- `fct_despesas` — fato principal com todas as despesas
- `dim_parcelamento` — dimensão com detalhes do parcelamento

### Testes dbt

- `unique` e `not_null` nas colunas chave
- `accepted_values` nos enums (`tipo`, `prioridade`, `status`)
- Regras de negócio: despesa `A_VISTA` não pode ter parcelamento, despesa `PARCELADA` deve ter parcelamento

## Carga Incremental

A DAG busca o `MAX(id)` da tabela `DESPESAS_RAW` no Snowflake e passa como parâmetro para o endpoint `/despesa/listar/min/{id}`, que retorna apenas as despesas com ID maior. Isso evita reprocessar registros já carregados.

## Como Rodar

### Pré-requisitos

- Docker e Docker Compose
- Apache Airflow configurado
- Conta no Snowflake
- dbt instalado (`pip install dbt-snowflake`)

### API

```bash
cd controle-de-despesas
./mvnw clean package -DskipTests
docker-compose up -d
```

### Airflow

Copie o arquivo `dags/dag_despesas.py` para a pasta de DAGs do seu Airflow e configure a conexão `snowflake` na UI.

### dbt

```bash
cd dbt/despesas_pipeline
dbt deps
dbt run
dbt test
```

## Variáveis de Ambiente

| Variável | Descrição |
|---|---|
| `SF_ACCOUNT` | Account identifier do Snowflake |
| `SF_USER` | Usuário do Snowflake |
| `SF_PASSWORD` | Senha do Snowflake |
| `SF_WAREHOUSE` | Warehouse |
| `SF_DATABASE` | Database |
| `SF_SCHEMA` | Schema |
| `DB_URL` | URL do PostgreSQL (para a API) |
| `DB_USERNAME` | Usuário do PostgreSQL |
| `DB_PASSWORD` | Senha do PostgreSQL |
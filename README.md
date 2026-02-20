# Lakehouse Dev

[![Python](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/pyspark-4.0-orange)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/delta-lake-007ec6)](https://delta.io/)
[![Airflow](https://img.shields.io/badge/airflow-3.0-red)](https://airflow.apache.org/)

Projeto de referência para implementação de uma arquitetura **Data Lakehouse**, contemplando pipelines de ingestão, transformação e orquestração de dados.

O ambiente é totalmente containerizado com **Docker**, garantindo reprodutibilidade, padronização e facilidade de execução local.

A modelagem segue o padrão clássico de camadas do Lakehouse:

**Bronze → Silver → Gold**

* **Bronze**: dados brutos (raw ingestion)
* **Silver**: dados tratados, limpos e enriquecidos
* **Gold**: dados modelados e prontos para consumo analítico

---

## Tecnologias Utilizadas

* **Python 3.12** – Linguagem principal para desenvolvimento de pipelines e scripts
* **PySpark 4** – Processamento distribuído de dados
* **Delta Lake** – Armazenamento transacional com versionamento e otimizações para consultas
* **MinIO** – Armazenamento compatível com S3 utilizado como Data Lake local
* **Airflow 3** – Orquestração e agendamento de pipelines
* **Selenium** – Coleta automatizada de dados externos

---

# Estrutura de Pastas e Arquitetura do Projeto

A organização do projeto foi definida com foco em:

* Separação clara de responsabilidades
* Modularidade
* Facilidade de manutenção
* Escalabilidade futura
* Clareza arquitetural

Cada diretório possui uma função específica dentro da arquitetura Lakehouse adotada.

## Estrutura de Diretórios

```txt
lakehouse_dev/
│
├── README.md                  # Documentação principal do projeto
├── requirements.txt           # Dependências Python adicionais
├── pyproject.toml             # Configurações e metadados do projeto
├── Makefile                   # Automação de build e execução
├── compose.yaml               # Orquestração dos containers Docker
├── .env                       # Variáveis de ambiente e credenciais
│
├── core/                      # Módulos e utilitários compartilhados
│   └── spark/
│       └── spark_session.py   # Configuração centralizada da SparkSession
│
├── data_contracts/            # Contratos e validações de dados
│
├── airflow/                   # Estrutura e configuração do Airflow
│   ├── dags/                  # Definição das DAGs
│   ├── configs/               # Configurações específicas
│   └── plugins/               # Plugins customizados
│
├── pipelines/                 # Lógica de ingestão e transformação
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── config/                # Configurações específicas de pipelines
│
├── crawlers/                  # Coleta automatizada de dados externos
│   └── selenium/              # Implementações com Selenium
│
├── docker/
│   ├── .dockerignore
│   └── Dockerfile             # Imagem base (Airflow 3 + Spark 4 + Delta Lake)
│
├── scripts/                   # Scripts auxiliares
└── tests/                     # Testes unitários e de integração
```

---

# Organização Arquitetural

A arquitetura segue o modelo **Data Lakehouse**, estruturado em quatro pilares principais:

* **Orquestração** → Airflow
* **Processamento** → Spark + Delta Lake
* **Armazenamento** → MinIO (S3-compatible)
* **Ingestão externa** → Crawlers

Além disso, a camada de **Data Contracts** reforça a governança e a qualidade dos dados.

Essa abordagem permite evolução modular do projeto e facilita futura migração para ambientes em cloud (AWS, Azure, GCP).

---

# Descrição dos Componentes

### `requirements.txt`

Gerencia dependências adicionais ao ambiente base da imagem Docker.

Sempre que novas bibliotecas forem adicionadas, é necessário reconstruir a imagem:

```bash
docker compose build
```

---

### `Makefile`

Centraliza os principais comandos operacionais do projeto (build, start, stop, logs etc.), reduzindo complexidade e padronizando a execução.

---

### `.env`

Arquivo responsável pela definição de:

* Credenciais
* Variáveis de ambiente
* Configurações sensíveis

⚠️ Não deve ser versionado em ambientes produtivos.

---

### `core/`

Contém módulos reutilizáveis e utilitários compartilhados entre DAGs e pipelines.
Evita duplicação de código e promove padronização técnica.

---

### `data_contracts/`

Responsável por garantir qualidade e consistência dos dados por meio de:

* Definição de schemas
* Regras de validação
* Contratos de dados

---

### `airflow/`

Agrupa todos os artefatos relacionados à orquestração:

* `dags/` → definição dos fluxos
* `configs/` → configurações auxiliares
* `plugins/` → extensões customizadas

Mantém a orquestração desacoplada da lógica de negócio.

---

### `pipelines/`

Contém a lógica de ingestão e transformação organizada por camadas:

* `bronze/` → ingestão bruta
* `silver/` → dados tratados e padronizados
* `gold/` → dados prontos para consumo analítico

O diretório é montado no container do Airflow, permitindo que novos desenvolvimentos sejam automaticamente reconhecidos pelas DAGs.

---

### `crawlers/`

Responsável exclusivamente pela coleta de dados externos.

A separação entre `pipelines` e `crawlers` foi uma decisão arquitetural para:

* Isolar responsabilidades
* Facilitar manutenção
* Permitir evolução independente da camada de ingestão

---

### `docker/`

Contém os artefatos necessários para construção da imagem personalizada do projeto, baseada em:

* Airflow 3
* Spark 4
* Delta Lake

Garante reprodutibilidade e padronização do ambiente.

---

### `tests/`

Diretório dedicado a testes unitários e de integração, promovendo confiabilidade e evolução segura do código.

---

# Pré-requisitos

Antes de iniciar, certifique-se de possuir:

* Docker
* Docker Compose
* Git

---

# Instalação

## 1. Clone o repositório

```bash
git clone git@github.com:Alberto-Oliveira-Barbosa/lakehouse_dev.git
cd lakehouse_dev
```

## 2. Configure o ambiente

```bash
cp .env.example .env
```

Edite o `.env` conforme necessário.

---

# Execução com Docker

## Subir todos os serviços

```bash
make up
```

ou

```bash
docker compose up --build -d
```

## Parar os serviços

```bash
make down
```

## Visualizar logs

```bash
make logs
make logs_airflow
make logs_minio
```

---

# Configurações Obrigatórias Antes de Executar as DAGs

## Senha automática no Apache Airflow 3

A partir da versão 3, o Airflow gera automaticamente a senha do usuário `admin` na inicialização do container.

Ela não é fixa e deve ser obtida nos logs:

```bash
make logs_airflow
```

ou

```bash
docker compose logs airflow
```

Exemplo de saída:

```
lakehouse-airflow  | Simple auth manager | Password for user 'admin': XXXXXXXX
```

Acesse:

```
http://localhost:8081
```

Usuário padrão:

```
admin
```

Sem recuperar a senha nos logs, não será possível acessar a interface.

---

## Criação do Bucket no MinIO

Antes de executar as DAGs de exemplo, é obrigatório criar manualmente o bucket no MinIO.

### Acessar o MinIO

```
http://localhost:9001
```

Utilize as credenciais definidas no `.env`:

```
MINIO_ROOT_USER
MINIO_ROOT_PASSWORD
```

### Criar o Bucket

1. Acesse o console do MinIO
2. Clique em **Buckets**
3. Selecione **Create Bucket**
4. Crie o bucket esperado pelas DAGs

Por padrão, este template requer ao menos um bucket chamado:

```
lakehouse
```

As camadas (bronze, silver, gold) e subdiretórios são criadas automaticamente durante a escrita.

⚠️ Caso o bucket não exista, as DAGs falharão ao tentar persistir dados.

---

# Executando as DAGs

Após:

* Subir os containers
* Recuperar a senha do Airflow
* Criar o bucket no MinIO

Acesse:

```
http://localhost:8081
```

Ative e execute as DAGs disponíveis.

---
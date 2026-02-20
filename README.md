# Lakehouse Dev

[![Python](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/pyspark-4.0-orange)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/delta-lake-007ec6)](https://delta.io/)
[![Airflow](https://img.shields.io/badge/airflow-3.0-red)](https://airflow.apache.org/)

Projeto de referência para arquitetura **Data Lakehouse**, com pipelines de ingestão, transformação e orquestração, utilizando **Docker** para facilitar o desenvolvimento e testes locais.

O projeto adota camadas de dados típicas do Lakehouse: **bronze → silver → gold**, separando ingestão, limpeza e modelagem.

---

## Tecnologias utilizadas

* **Python 3.12** – linguagem principal para pipelines e scripts
* **PySpark 4** – processamento distribuído de dados
* **Delta Tables 4** – armazenamento transacional, versionado e otimizado para queries
* **MinIO** – armazenamento compatível com S3, usado como data lake local
* **Airflow 3** – orquestração e scheduling de pipelines
* **Selenium** – coleta automatizada de dados da web

---

Segue a versão revisada, com linguagem mais clara, técnica e profissional:

---

## Estrutura de Pastas e Arquitetura do Projeto

A organização do projeto foi definida com foco em **separação de responsabilidades**, **manutenibilidade**, **escalabilidade** e **clareza arquitetural**.

Cada diretório possui um papel bem definido dentro da arquitetura Lakehouse adotada.

### Estrutura de Diretórios

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
├── data_contracts/            # Definição de contratos e validações de dados
│
├── airflow/                   # Configurações e estrutura do Airflow
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
├── crawlers/                  # Automações e coleta de dados externos
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

## Organização Arquitetural

A arquitetura segue o padrão **Data Lakehouse**, com separação clara entre:

* **Orquestração** (Airflow)
* **Processamento** (Spark + Delta Lake)
* **Armazenamento** (MinIO)
* **Ingestão externa** (Crawlers)
* **Governança e qualidade** (Data Contracts)

Essa estrutura permite evolução modular, testes isolados e adaptação futura para ambientes em cloud.

---

## Descrição dos Principais Componentes

### `requirements.txt`

Gerencia dependências Python adicionais ao ambiente base da imagem Docker.
Sempre que novas bibliotecas forem adicionadas, é necessário **reconstruir a imagem Docker** para refletir as mudanças no ambiente.

---

### `Makefile`

Centraliza comandos recorrentes do projeto (build, start, stop, logs, etc.).
Reduz complexidade operacional e padroniza a execução do ambiente.

---

### `.env`

Responsável pela definição de:

* Credenciais
* Variáveis de ambiente
* Configurações sensíveis

Esse arquivo **não deve ser versionado** em ambientes produtivos.

---

### `core/`

Contém módulos reutilizáveis e utilitários compartilhados entre pipelines e DAGs.
A centralização evita duplicação de código e melhora a padronização técnica.

---

### `data_contracts/`

Destinado à definição de contratos de dados, schemas esperados e validações.
Tem como objetivo garantir:

* Qualidade
* Consistência
* Governança dos dados

---

### `airflow/`

Contém todos os artefatos relacionados à orquestração:

* `dags/` → definição dos fluxos de execução
* `configs/` → configurações específicas
* `plugins/` → extensões customizadas

Essa estrutura mantém o Airflow desacoplado da lógica de negócio.

---

### `pipelines/`

Contém a lógica de ingestão e transformação organizada nas camadas:

* **bronze/** → ingestão bruta (raw data)
* **silver/** → dados tratados e enriquecidos
* **gold/** → dados prontos para consumo analítico

O diretório é montado dentro do container do Airflow, garantindo que qualquer novo desenvolvimento seja automaticamente reconhecido pelas DAGs.

---

### `crawlers/`

Destinado exclusivamente à coleta de dados externos.

A separação entre `pipelines` e `crawlers` foi uma decisão arquitetural para:

* Isolar responsabilidades
* Facilitar manutenção
* Permitir futura substituição da estratégia de ingestão

Esse diretório também é montado dentro do container do Airflow.

---

### `docker/`

Contém os artefatos necessários para construção da imagem personalizada do projeto, baseada em:

* Airflow 3
* Spark 4
* Delta Lake

Essa abordagem garante reprodutibilidade e padronização do ambiente.

---

### `scripts/`

Armazena scripts auxiliares que não fazem parte diretamente dos pipelines, mas apoiam o desenvolvimento ou operação.

---

### `tests/`

Diretório dedicado a testes unitários e de integração.
Permite validação contínua da lógica de transformação e regras de negócio.

---

## Benefícios da Estrutura Adotada

* Separação clara de responsabilidades
* Modularização da lógica de negócio
* Facilidade de testes e manutenção
* Ambiente reprodutível via Docker
* Preparado para migração futura para ambientes em cloud

---

## Pré-requisitos

Antes de executar o projeto, você precisa ter instalado:

* Docker
* Docker Compose
* Git

---

## Instalação e Configuração

### 1 - Clone o repositório

```bash
git clone git@github.com:Alberto-Oliveira-Barbosa/lakehouse_dev.git
cd lakehouse_dev
```

### 2 - Configure o ambiente

```bash
cp .env.example .env
```

Edite o `.env` conforme necessário.

---

## Executando com Docker

### Subir todos os serviços

```bash
make up
```

ou

```bash
docker compose up --build -d
```

### Parar serviços

```bash
make down
```

### 📜 Logs

Logs gerais:

```bash
make logs
```

Logs do Airflow:

```bash
make logs_airflow
```

Logs do MinIO:

```bash
make logs_minio
```

---

## Configurações Obrigatórias Antes de Rodar as DAGs

---

### Senha automática no Apache Airflow 3

A partir da versão 3, o Airflow **gera automaticamente a senha do usuário admin na inicialização do container**.

Ela **não é mais fixa**.

Para obter a senha:

```bash
make logs_airflow
```

ou

```bash
docker compose logs airflow
```

Procure nos logs por algo como:

```
lakehouse-airflow  | Simple auth manager | Password for user 'admin': UsAxMF67F86Wh3Dw

```

Acesse a interface:

```
http://localhost:8081
```

Usuário padrão:

```
admin
```

Sem recuperar essa senha nos logs, não será possível acessar o painel.

---

### Criação do Bucket no MinIO

Antes de executar as DAGs de exemplo, é **obrigatório criar manualmente o bucket no MinIO**.

### Acessar o MinIO

Disponível em:

```
http://localhost:9001
```

Use as credenciais definidas no `.env`:

```
MINIO_ROOT_USER
MINIO_ROOT_PASSWORD
```

### Criar o Bucket

1. Acesse o console do MinIO
2. Clique em **Buckets**
3. Selecione **Create Bucket**
4. Crie o bucket com o nome esperado pelas DAGs (por default esse template espera ao menos um Bucket com o nome  `lakehouse`, demais camadas ou sub-diretórios ele consegue gerar na escrita.)

⚠️ Caso o bucket não exista, as DAGs irão falhar ao tentar gravar dados.
---

## Executando as DAGs

Após:

* Subir os containers
* Recuperar a senha do Airflow
* Criar o bucket no MinIO

Acesse o Airflow:

```
http://localhost:8081
```

Ative e execute as DAGs disponíveis.

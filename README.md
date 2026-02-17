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

## Estrutura de pastas

```txt
lakehouse_dev/
│
├── README.md                  # Documentação do projeto
├── requirements.txt           # Bibliotecas Python adicionais
├── pyproject.toml             # Configurações do projeto (poetry/pyproject)
├── makefile                   # Automação de comandos de build e execução
├── compose.yaml               # Orquestração de containers Docker
├── .env                       # Variáveis de ambiente e credenciais
│
├── core/                      # Funções, helpers e utilitários comuns
│   └── spark/
│       └── spark_session.py   # Configuração da SparkSession
│
├── data_contracts/            # Data contracts e arquivos de validação de dados
│
├── airflow/                   # Orquestração de pipelines
│   ├── dags/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── plugins/               # Plugins customizados do Airflow
│
├── crawlers/                  # Automação web
│   └── selenium/              # Crawlers Selenium
│
├── pipelines/                 # Ingestão e transformação de dados
│   ├── bronze/
│   ├── silver/
│   └── gold/
│   └── config/                # Configurações específicas dos pipelines
│
├── docker/
│   ├── .dockerignore
│   └── Dockerfile             # Imagem base: Airflow 3 + Spark 4 + Delta Lake
│
├── scripts/                   # Scripts utilitários e helpers adicionais
├── tests/                     # Testes unitários e de integração
```


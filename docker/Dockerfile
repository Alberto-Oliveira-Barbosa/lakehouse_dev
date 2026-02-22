FROM apache/airflow:3.0.0

USER root

# ========================
# Instala Java 17, wget, curl, unzip e git
# ========================
RUN apt-get update && \
apt-get install -y --no-install-recommends \
openjdk-17-jdk-headless \
wget \
curl \
unzip \
git && \
apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# ========================
# Cria diretório para JARs Spark/Hadoop
# ========================
RUN mkdir -p /opt/spark/jars

# ========================
# Baixa JARs necessários para Delta Lake + S3A
# ========================
RUN wget -P /opt/spark/jars/ \
https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.1/delta-spark_2.13-4.0.1.jar && \
wget -P /opt/spark/jars/ \
https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.1/delta-storage-4.0.1.jar && \
wget -P /opt/spark/jars/ \
https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
wget -P /opt/spark/jars/ \
https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN chmod 644 /opt/spark/jars/*.jar

# ========================
# Instala o google Chrome
# ========================

RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    gnupg \
    curl \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxcomposite1 \
    libxrandr2 \
    libxdamage1 \
    libgbm1 \
    libasound2 \
    libpangocairo-1.0-0 \
    libxshmfence1 \
    libx11-xcb1 \
    libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" \
    > /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable


USER airflow

# ========================
# Copia requirements.txt
# ========================
COPY requirements.txt /opt/airflow/requirements.txt

# ========================
# Configuração Airflow / Constraints
# ========================
ENV AIRFLOW_VERSION=3.0.0
ENV PYTHON_VERSION=3.12
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# ========================
# Instala PySpark 4.0.1 (fora das constraints para evitar conflito)
# ========================
RUN pip install --no-cache-dir pyspark==4.0.1
RUN pip install --no-cache-dir delta-spark==4.0.1

# ========================
# Instala outras libs do requirements.txt (Delta, Selenium, pandas, etc.)
# ========================
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt -c ${CONSTRAINT_URL}

# ========================
# Cria pastas para DAGs, Pipelines e Plugins e demais pastas usadas
# ========================
RUN mkdir -p /opt/airflow/dags /opt/airflow/pipelines /opt/airflow/plugins /opt/airflow/core /opt/airflow/data_contracts

# ========================
# Variáveis de ambiente Spark / Delta / S3A
# ========================
ENV SPARK_CLASSPATH=/opt/spark/jars/*
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/spark/jars/* \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
pyspark-shell"

# ========================
# Workdir
# ========================
WORKDIR /opt/airflow

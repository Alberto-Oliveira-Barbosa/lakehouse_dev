def get_spark(app_name="lakehouse", verbose=False):
    """
    Cria e configura uma SparkSession para trabalhar com Delta Lake + MinIO (S3A).
    
    Esta função resolve dois problemas críticos que afetam pipelines com Spark, 
    Delta Lake e MinIO:
    
    1 - NumberFormatException com strings como "60s", "5m", "24h"
    2 - ClassNotFoundException para classes do AWS SDK V2
    
    O problema raiz:
    -----------------
    O Hadoop tem configurações padrão que usam sufixos humanos (ex: "60s", "5m", "24h", "128M").
    O Delta Lake, ao inicializar o sistema de arquivos, tenta fazer parse desses valores como números,
    causando falhas como:
    
        java.lang.NumberFormatException: For input string: "60s"
        java.lang.NumberFormatException: For input string: "24h"
    
    Além disso, o Hadoop 3.3.4 inclui em suas configurações padrão provedores de credenciais
    do AWS SDK V2, que não estão presentes no classpath, causando:
    
        ClassNotFoundException: software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider
    
    A solução:
    -----------
    1. Converter TODAS as configurações com sufixos para valores numéricos (ms, bytes)
    2. Especificar explicitamente provedor de credenciais compatível com SDK V1
    3. Sobrescrever configurações diretamente no Hadoop Configuration (JVM)
    4. Verificar e corrigir automaticamente qualquer configuração residual
    
    Args:
        app_name (str): Nome da aplicação Spark
        verbose (bool): Se True, exibe logs detalhados das correções aplicadas.
                       Por padrão (False), mantém os logs limpos.
    
    Returns:
        SparkSession: Sessão configurada e pronta para uso
    """
    import os
    import logging
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    
    # ======================================================================
    # CONFIGURAÇÕES INICIAIS DA SPARK SESSION
    # ======================================================================
    
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        
        # ------------------------------------------------------------------
        # BIBLIOTECAS E DEPENDÊNCIAS
        # ------------------------------------------------------------------
        # IMPORTANTE: Hadoop 3.3.4 usa AWS SDK V1 (bundle 1.12.262)
        # Não misturar com SDK V2 para evitar ClassNotFoundException
        # ------------------------------------------------------------------
        .config(
            "spark.jars.packages",
            ",".join([
                "io.delta:delta-spark_2.13:4.0.1",  # Suporte a Delta Lake
                "org.apache.hadoop:hadoop-aws:3.3.4",  # Conector S3A (SDK V1)
                "com.amazonaws:aws-java-sdk-bundle:1.12.262"  # AWS SDK V1
            ])
        )
        
        # ------------------------------------------------------------------
        # CONFIGURAÇÕES DO DELTA LAKE
        # ------------------------------------------------------------------
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # ------------------------------------------------------------------
        # CONFIGURAÇÕES BASE DO MINIO (S3A)
        # ------------------------------------------------------------------
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")  # Necessário para MinIO
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        # ======================================================================
        # FIX 1: PROVEDOR DE CREDENCIAIS COMPATÍVEL COM SDK V1
        # ======================================================================
        # PROBLEMA: Hadoop 3.3.4 tem lista padrão que inclui provedores do SDK V2
        # SOLUÇÃO: Especificar explicitamente provedor do SDK V1
        # ----------------------------------------------------------------------
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        
        # ======================================================================
        # FIX 2: CONVERSÃO DE SUFIXOS PARA VALORES NUMÉRICOS
        # ======================================================================
        # PROBLEMA: Configurações como "60s", "5m", "24h" causam NumberFormatException
        # SOLUÇÃO: Converter todos os valores com sufixos para milissegundos/bytes
        # ----------------------------------------------------------------------
        
        # Timeouts base (em milissegundos)
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")           # 60 segundos
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") # 60 segundos
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")               # 60 segundos
        
        # Configurações de tempo (convertidas para milissegundos)
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")        # 60s → 60000ms
        .config("spark.hadoop.fs.s3a.connection.ttl", "300000")              # 5m → 300000ms
        .config("spark.hadoop.fs.s3a.retry.interval", "500")                 # 500ms → 500ms
        .config("spark.hadoop.fs.s3a.retry.throttle.interval", "100")        # 100ms → 100ms
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")       # 24h → 86400000ms
        
        # Tamanhos (convertidos para bytes)
        .config("spark.hadoop.fs.s3a.multipart.threshold", "134217728")      # 128M → bytes
        .config("spark.hadoop.fs.s3a.block.size", "33554432")                # 32M → bytes
        .config("spark.hadoop.fs.s3a.readahead.range", "65536")              # 64K → bytes
        .config("spark.hadoop.fs.s3a.multipart.size", "67108864")            # 64M → bytes
        
        # ------------------------------------------------------------------
        # OUTRAS CONFIGURAÇÕES NUMÉRICAS
        # ------------------------------------------------------------------
        .config("spark.hadoop.fs.s3a.retry.limit", "10")
        .config("spark.hadoop.fs.s3a.threads.max", "100")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.executor.capacity", "16")
        .config("spark.hadoop.fs.s3a.max.total.tasks", "32")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "5")
        .config("spark.hadoop.fs.s3a.retry.throttle.limit", "20")
    )

    # ======================================================================
    # CRIAÇÃO DA SPARK SESSION COM SUPORTE A DELTA
    # ======================================================================
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # ======================================================================
    # FIX 3: SOBRESCRITA DIRETA NO HADOOP CONFIGURATION (NÍVEL JVM)
    # ======================================================================
    # Por que isso é necessário?
    # -------------------------
    # As configurações feitas via .config("spark.hadoop.*") são passadas para o Hadoop,
    # mas alguns valores padrão podem vir de arquivos internos do Hadoop (core-default.xml)
    # e sobrescrever nossas configurações. Acessando o HadoopConfiguration diretamente
    # garantimos que nossos valores prevaleçam.
    # ----------------------------------------------------------------------
    
    hadoop_conf = spark._jsc.hadoopConfiguration()
    
    # Dicionário completo de todas as configurações que precisamos garantir
    configs_fix = {
        # Credenciais - garantir que não use SDK V2
        "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        
        # Timeouts base
        "fs.s3a.connection.timeout": "60000",
        "fs.s3a.socket.timeout": "60000",
        "fs.s3a.connection.establish.timeout": "60000",
        
        # Configurações de tempo (TODAS em ms)
        "fs.s3a.threads.keepalivetime": "60000",           # 60s → 60000ms
        "fs.s3a.connection.ttl": "300000",                 # 5m → 300000ms
        "fs.s3a.retry.interval": "500",                    # 500ms
        "fs.s3a.retry.throttle.interval": "100",           # 100ms
        "fs.s3a.multipart.purge.age": "86400000",          # 24h → 86400000ms
        
        # Tamanhos em bytes
        "fs.s3a.multipart.threshold": "134217728",         # 128M
        "fs.s3a.block.size": "33554432",                   # 32M
        "fs.s3a.readahead.range": "65536",                 # 64K
        "fs.s3a.multipart.size": "67108864",               # 64M
        
        # Outras configurações numéricas
        "fs.s3a.retry.limit": "10",
        "fs.s3a.threads.max": "100",
        "fs.s3a.connection.maximum": "100",
        "fs.s3a.executor.capacity": "16",
        "fs.s3a.max.total.tasks": "32",
        "fs.s3a.attempts.maximum": "5",
        "fs.s3a.retry.throttle.limit": "20"
    }
    
    # Aplica as configurações (sempre necessário, mas só loga se verbose)
    for key, value in configs_fix.items():
        hadoop_conf.set(key, value)
        if verbose:
            print(f"  Configurado: {key} = {value}")
    
    # ======================================================================
    # FIX 4: VERIFICAÇÃO E CORREÇÃO AUTOMÁTICA DE CONFIGS RESIDUAIS
    # ======================================================================
    # Por que isso é necessário?
    # -------------------------
    # O Hadoop pode ter dezenas de configurações internas com sufixos.
    # Fazemos uma varredura completa para garantir que nenhuma configuração
    # com sufixo "escapou" da nossa correção.
    # ----------------------------------------------------------------------
    
    # Lista de sufixos que podem causar problemas em configurações S3A
    sufixos_proibidos = ['s', 'm', 'h', 'ms', 'K', 'M', 'G']
    
    # Padrões de configurações que sabemos que não são problemas (para ignorar na verificação)
    configs_ignoradas = [
        "yarn.nodemanager.env-whitelist",
        "mapreduce.task.profile.params",
        "hadoop.system.tags",
        "hadoop.tags.system",
        "hadoop.http.cross-origin.allowed-headers",
        "mapreduce.jvm.system-properties-to-log",
        "hadoop.security.sensitive-config-keys",
        "ipc."
    ]
    
    iterator = hadoop_conf.iterator()
    problemas_encontrados = False
    
    while iterator.hasNext():
        entry = iterator.next()
        key = entry.getKey()
        value = entry.getValue()
        
        # Ignora configurações que sabemos que não são problemas
        if any(ignorada in key for ignorada in configs_ignoradas):
            continue
        
        # Só verifica configurações S3A para sufixos problemáticos
        if "s3a" in key.lower():
            # Verifica se o valor termina com algum sufixo proibido (padrão de tempo/tamanho)
            for sufixo in sufixos_proibidos:
                if value.endswith(sufixo):
                    problemas_encontrados = True
                    # Tenta corrigir baseado em casos conhecidos
                    if key == "fs.s3a.multipart.purge.age" and value == "24h":
                        hadoop_conf.set(key, "86400000")
                        if verbose:
                            print(f"  Corrigido: {key} = 86400000 (24h → ms)")
                    elif key in configs_fix:
                        hadoop_conf.set(key, configs_fix[key])
                        if verbose:
                            print(f"  Corrigido: {key} = {configs_fix[key]}")
                    break
    
    # ======================================================================
    # VERIFICAÇÃO ESPECÍFICA DA CONFIGURAÇÃO MAIS PROBLEMÁTICA
    # ======================================================================
    # A configuração fs.s3a.multipart.purge.age é particularmente crítica
    # porque tem valor padrão "24h" e sempre causa falha se não for corrigida
    # ----------------------------------------------------------------------
    purge_age = hadoop_conf.get("fs.s3a.multipart.purge.age", "não definida")
    if purge_age == "24h":
        hadoop_conf.set("fs.s3a.multipart.purge.age", "86400000")
        if verbose:
            print(f"  Correção emergencial: fs.s3a.multipart.purge.age = 86400000")
    
    # ======================================================================
    # CONFIGURAÇÕES FINAIS
    # ======================================================================
    spark.sparkContext.setLogLevel("WARN")  # apenas WARN e ERROR
    
    # Log único indicando que a SparkSession foi iniciada
    if verbose and problemas_encontrados:
        print(f"SparkSession '{app_name}' inicializada com correções aplicadas.")
    elif verbose:
        print(f"SparkSession '{app_name}' inicializada.")

    return spark

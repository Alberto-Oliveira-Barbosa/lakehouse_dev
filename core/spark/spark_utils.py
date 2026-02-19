import sys
import os
import logging
from subprocess import Popen
from typing import Any
from unittest.mock import patch
from pyspark.sql import SparkSession, DataFrame
from delta import configure_spark_with_delta_pip


def _get_spark(app_name="lakehouse", verbose=False):
    """
    Creates and configures a SparkSession to work with Delta Lake + MinIO (S3A).

    This function resolves two critical issues that affect Spark,
    Delta Lake, and MinIO pipelines:

    1 - NumberFormatException caused by strings such as "60s", "5m", "24h"
    2 - ClassNotFoundException for AWS SDK V2 credential provider classes

    Root Cause:
    -----------
    Hadoop has default configurations that use human-readable suffixes
    (e.g., "60s", "5m", "24h", "128M").
    When Delta Lake initializes the filesystem, it attempts to parse these
    values as pure numbers, which leads to errors such as:

        java.lang.NumberFormatException: For input string: "60s"
        java.lang.NumberFormatException: For input string: "24h"

    Additionally, Hadoop 3.3.4 includes AWS SDK V2 credential providers
    in its default configuration. Since those classes are not available
    in the classpath, this results in:

        ClassNotFoundException:
        software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider

    Solution:
    ---------
    1. Convert ALL configurations with suffixes into numeric values (milliseconds, bytes)
    2. Explicitly define a credentials provider compatible with AWS SDK V1
    3. Override configurations directly in the Hadoop Configuration (JVM level)
    4. Automatically detect and fix any remaining misconfigured settings

    Args:
        app_name (str): Name of the Spark application.
        verbose (bool): If True, prints detailed logs of applied fixes.
                        Defaults to False to keep logs clean.

    Returns:
        SparkSession: A fully configured and ready-to-use Spark session.
    """
    import os
    import logging
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    # ======================================================================
    # INITIAL SPARK SESSION CONFIGURATION
    # ======================================================================

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # ------------------------------------------------------------------
        # LIBRARIES AND DEPENDENCIES
        # ------------------------------------------------------------------
        # IMPORTANT: Hadoop 3.3.4 uses AWS SDK V1 (bundle 1.12.262)
        # Do NOT mix with SDK V2 to avoid ClassNotFoundException
        # ------------------------------------------------------------------
        .config(
            "spark.jars.packages",
            ",".join([
                "io.delta:delta-spark_2.13:4.0.1",  # Delta Lake support
                "org.apache.hadoop:hadoop-aws:3.3.4",  # S3A connector (SDK V1)
                "com.amazonaws:aws-java-sdk-bundle:1.12.262"  # AWS SDK V1
            ])
        )

        # ------------------------------------------------------------------
        # DELTA LAKE CONFIGURATION
        # ------------------------------------------------------------------
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # ------------------------------------------------------------------
        # MINIO (S3A) BASE CONFIGURATION
        # ------------------------------------------------------------------
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")  # Required for MinIO
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # ======================================================================
        # FIX 1: AWS SDK V1 COMPATIBLE CREDENTIAL PROVIDER
        # ======================================================================
        # ISSUE: Hadoop 3.3.4 includes default providers from AWS SDK V2
        # SOLUTION: Explicitly define SDK V1 provider
        # ----------------------------------------------------------------------
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )

        # ======================================================================
        # FIX 2: CONVERT SUFFIXED VALUES TO NUMERIC VALUES
        # ======================================================================
        # ISSUE: Configurations like "60s", "5m", "24h" cause NumberFormatException
        # SOLUTION: Convert all such values to milliseconds/bytes
        # ----------------------------------------------------------------------

        # Base timeouts (in milliseconds)
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")           # 60 seconds
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") # 60 seconds
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")               # 60 seconds

        # Time configurations (converted to milliseconds)
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")        # 60s → 60000ms
        .config("spark.hadoop.fs.s3a.connection.ttl", "300000")              # 5m → 300000ms
        .config("spark.hadoop.fs.s3a.retry.interval", "500")                 # 500ms → 500ms
        .config("spark.hadoop.fs.s3a.retry.throttle.interval", "100")        # 100ms → 100ms
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")       # 24h → 86400000ms

        # Size configurations (converted to bytes)
        .config("spark.hadoop.fs.s3a.multipart.threshold", "134217728")      # 128M → bytes
        .config("spark.hadoop.fs.s3a.block.size", "33554432")                # 32M → bytes
        .config("spark.hadoop.fs.s3a.readahead.range", "65536")              # 64K → bytes
        .config("spark.hadoop.fs.s3a.multipart.size", "67108864")            # 64M → bytes

        # Other numeric configurations
        .config("spark.hadoop.fs.s3a.retry.limit", "10")
        .config("spark.hadoop.fs.s3a.threads.max", "100")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.executor.capacity", "16")
        .config("spark.hadoop.fs.s3a.max.total.tasks", "32")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "5")
        .config("spark.hadoop.fs.s3a.retry.throttle.limit", "20")
    )

    # Create Spark session with Delta support
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # ======================================================================
    # FIX 3: DIRECT OVERRIDE AT HADOOP CONFIGURATION (JVM LEVEL)
    # ======================================================================
    # Some default Hadoop values may override Spark configs.
    # We enforce our settings directly at JVM level.
    # ----------------------------------------------------------------------

    hadoop_conf = spark._jsc.hadoopConfiguration()

    configs_fix = {
        "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "fs.s3a.connection.timeout": "60000",
        "fs.s3a.socket.timeout": "60000",
        "fs.s3a.connection.establish.timeout": "60000",
        "fs.s3a.threads.keepalivetime": "60000",           # 60s → 60000ms
        "fs.s3a.connection.ttl": "300000",                 # 5m → 300000ms
        "fs.s3a.retry.interval": "500",                    # 500ms
        "fs.s3a.retry.throttle.interval": "100",           # 100ms
        "fs.s3a.multipart.purge.age": "86400000",          # 24h → 86400000ms
        "fs.s3a.multipart.threshold": "134217728",         # 128M
        "fs.s3a.block.size": "33554432",                   # 32M
        "fs.s3a.readahead.range": "65536",                 # 64K
        "fs.s3a.multipart.size": "67108864",               # 64M
        "fs.s3a.retry.limit": "10",
        "fs.s3a.threads.max": "100",
        "fs.s3a.connection.maximum": "100",
        "fs.s3a.executor.capacity": "16",
        "fs.s3a.max.total.tasks": "32",
        "fs.s3a.attempts.maximum": "5",
        "fs.s3a.retry.throttle.limit": "20"
    }

    for key, value in configs_fix.items():
        hadoop_conf.set(key, value)
        if verbose:
            print(f"  Set: {key} = {value}")

    # ======================================================================
    # FIX 4: AUTOMATIC DETECTION AND CORRECTION OF RESIDUAL CONFIGS
    # ======================================================================

    forbidden_suffixes = ['s', 'm', 'h', 'ms', 'K', 'M', 'G']

    ignored_configs = [
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
    issues_found = False

    while iterator.hasNext():
        entry = iterator.next()
        key = entry.getKey()
        value = entry.getValue()

        if any(ignored in key for ignored in ignored_configs):
            continue

        if "s3a" in key.lower():
            for suffix in forbidden_suffixes:
                if value.endswith(suffix):
                    issues_found = True
                    if key == "fs.s3a.multipart.purge.age" and value == "24h":
                        hadoop_conf.set(key, "86400000")
                        if verbose:
                            print(f"  Fixed: {key} = 86400000 (24h → ms)")
                    elif key in configs_fix:
                        hadoop_conf.set(key, configs_fix[key])
                        if verbose:
                            print(f"  Fixed: {key} = {configs_fix[key]}")
                    break

    purge_age = hadoop_conf.get("fs.s3a.multipart.purge.age", "undefined")
    if purge_age == "24h":
        hadoop_conf.set("fs.s3a.multipart.purge.age", "86400000")
        if verbose:
            print("  Emergency fix: fs.s3a.multipart.purge.age = 86400000")

    spark.sparkContext.setLogLevel("WARN")

    if verbose and issues_found:
        print(f"SparkSession '{app_name}' initialized with fixes applied.")
    elif verbose:
        print(f"SparkSession '{app_name}' initialized.")

    return spark

def get_spark_session(app_name="lakehouse", verbose=False, suppress_init_logs=True):
    """
    Suppresses verbose initialization logs (Ivy, JVM incubator, etc.)
    without changing any configuration or behavior of the original SparkSession.

    Uses a temporary patch only during session creation
    (much safer than globally redirecting stdout/stderr).

    Parameters
    ----------
    app_name : str
        Spark application name.
    verbose : bool
        Verbose flag passed to the original _get_spark function.
    suppress_init_logs : bool
        If True, suppresses stdout/stderr during SparkSession initialization.
        If False, creates the session normally without log suppression.
    """
    import os
    import logging
    from subprocess import Popen
    from unittest.mock import patch

    def create_spark():
        # Calls the original function without any modification
        return _get_spark(app_name=app_name, verbose=verbose)

    if suppress_init_logs:
        # Suppress stdout/stderr ONLY during SparkSession initialization
        # (this is when Ivy runs and the JVM prints incubator module warnings)
        with patch(
            "pyspark.java_gateway.Popen",
            side_effect=lambda *args, **kwargs: Popen(
                *args,
                **kwargs,
                stdout=open(os.devnull, 'wb'),
                stderr=open(os.devnull, 'wb')
            ),
        ):
            spark = create_spark()
    else:
        spark = create_spark()

    spark.sparkContext.setLogLevel("ERROR")

    logging.getLogger(__name__).info(
        f"SparkSession '{app_name}' created "
        f"(init log suppression={'ON' if suppress_init_logs else 'OFF'})"
    )

    return spark
    

def save_delta(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
    **writer_options: Any
) -> None:
    """
    Delta writer that accepts any Spark writer options.

    Example:
        save_delta(
            df,
            path,
            mode="append",
            partition_by=["year", "month"],
            mergeSchema="true",
            overwriteSchema="true"
        )
    """

    
    logging.info(f"Writing Delta table to: {path}")

    try:
        writer = df.write.format("delta").mode(mode)

        # Apply partitioning if provided
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        # Apply any additional Spark options
        for key, value in writer_options.items():
            writer = writer.option(key, value)

        writer.save(path)

        logging.info("Delta write completed successfully.")

    except Exception as e:
        logging.exception("Delta write failed.")
        raise RuntimeError(f"Delta write failed for path: {path}") from e

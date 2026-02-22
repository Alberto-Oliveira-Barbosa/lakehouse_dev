def generate_gold_metrics():
    from pyspark.sql import functions as F
    from core.spark.spark_utils import get_spark_session, save_delta
    from core.utils import get_full_path

    spark = get_spark_session("aggregation_test",verbose=False, suppress_init_logs=True)
    df = spark.read.format("delta").load(get_full_path("silver", "test", "example"))

    # employees by department
    dept_total = (
        df.groupBy("DEPT_ID")
          .agg(F.count("*").alias("KPI_VALUE"))
          .withColumn("KPI_NAME", F.lit("TOTAL_EMPLOYEES"))
          .withColumn("SCOPE", F.lit("DEPT_ID"))
          .withColumn("SCOPE_VALUE", F.col("DEPT_ID").cast("string"))
          .select("KPI_NAME", "SCOPE", "SCOPE_VALUE", "KPI_VALUE")
    )

    # avg salary by gender
    gender_avg = (
        df.groupBy("GENDER")
          .agg(F.avg("SALARY").alias("KPI_VALUE"))
          .withColumn("KPI_NAME", F.lit("AVG_SALARY"))
          .withColumn("SCOPE", F.lit("GENDER"))
          .withColumn("SCOPE_VALUE", F.col("GENDER"))
          .select("KPI_NAME", "SCOPE", "SCOPE_VALUE", "KPI_VALUE")
    )

    # KPI global
    global_kpi = (
        df.agg(F.sum("SALARY").alias("KPI_VALUE"))
          .withColumn("KPI_NAME", F.lit("TOTAL_PAYROLL"))
          .withColumn("SCOPE", F.lit("GLOBAL"))
          .withColumn("SCOPE_VALUE", F.lit("ALL"))
          .select("KPI_NAME", "SCOPE", "SCOPE_VALUE", "KPI_VALUE")
    )

    final_df = dept_total.unionByName(gender_avg).unionByName(global_kpi)
    final_df = final_df.withColumn("EXECUTION_DATE", F.current_date())

    save_delta(final_df, get_full_path("gold", "test", "example"))

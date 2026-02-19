def transform_test():
    from core.spark.spark_utils import get_spark_session, save_delta
    from core.utils import get_full_path
    from pyspark.sql.functions import col

    spark = get_spark_session("transform_test", verbose=False, suppress_init_logs=True)

    # read table in bronze layer
    df = spark.read.format("delta").load(get_full_path("example", "bronze"))

    # transform example
    df = (
        df.select(
            col("deptId").cast("int").alias("DEPT_ID"),
            col("age").cast("int").alias("AGE"),
            col("name").cast("string").alias("NAME"),
            col("gender").cast("string").alias("GENDER"),
            col("salary").cast("double").alias("SALARY"),
        )
    )

    save_delta(df,get_full_path("example", "silver"), overwriteSchema=True)
    

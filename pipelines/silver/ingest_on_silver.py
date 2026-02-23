from core.spark.spark_utils import get_spark_session, save_delta
from core.lake_utils import get_full_path
from pyspark.sql import functions as F

def transform_test():

    spark = get_spark_session("transform_test", verbose=False, suppress_init_logs=True)

    # read table in bronze layer
    df = spark.read.format("delta").load(get_full_path(layer="bronze", domain="test", table_name="example"))

    # transform example
    df = (
        df.select(
            F.col("deptId").cast("int").alias("DEPT_ID"),
            F.col("age").cast("int").alias("AGE"),
            F.col("name").cast("string").alias("NAME"),
            F.col("gender").cast("string").alias("GENDER"),
            F.col("salary").cast("double").alias("SALARY"),
        )
    )

    save_delta(df,get_full_path(layer="silver", domain="test",table_name="example"), overwriteSchema=True)
    
def transform_news():
    spark = get_spark_session("transform_news", verbose=False, suppress_init_logs=True)

    df = spark.read.format("delta").load(get_full_path(layer="bronze", domain="test", table_name="news"))

    df = (
        df.select(
            F.col("title").cast("string").alias("TITLE"),
            F.col("link").cast("string").alias("LINK"),
            F.col("extraction_date").cast("date").alias("EXTRACTION_DATE")
        )
    )

    save_delta(df,get_full_path(layer="silver", domain="test",table_name="news"), overwriteSchema=True)
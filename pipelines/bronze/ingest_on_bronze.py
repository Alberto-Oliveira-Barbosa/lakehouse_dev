import logging
import pandas as pd
from datetime import datetime
from core.spark.spark_utils import get_spark_session, save_delta
from core.utils import get_full_path, list_files_in_directory,read_from_lake

def ingest_test():

    layer = "bronze"
    domain = "test"
    table_name = "example"

    full_path = get_full_path(layer,domain, table_name)

    spark = get_spark_session("ingest_test",verbose=False, suppress_init_logs=True)
    
    df = spark.createDataFrame([
        {"deptId": 1, "age": 40, "name": "Hyukjin Kwon", "gender": "M", "salary": 50},
        {"deptId": 1, "age": 50, "name": "Takuya Ueshin", "gender": "M", "salary": 100},
        {"deptId": 2, "age": 60, "name": "Xinrong Meng", "gender": "F", "salary": 150},
        {"deptId": 3, "age": 20, "name": "Haejoon Lee", "gender": "M", "salary": 200}
    ])

    save_delta(df, full_path)

def ingest_from_raw():
    paths = list_files_in_directory('raw/test/')

    df_final = None
    for path in paths:
        logging.info(f"Processing file: {path}")
        path = path.split("/")
        _df = read_from_lake(get_full_path(layer=path[0], domain=path[1], table_name=path[2]))
        date = path[-1].split(".")[0].split("_")[1]
        _df["extraction_date"] = datetime.strptime(date, "%Y-%m-%d")
        df_final = pd.concat([df_final, _df])
    
    spark = get_spark_session("ingest_from_raw",verbose=False, suppress_init_logs=True)

    df = spark.createDataFrame(df_final)
    save_delta(df,get_full_path("bronze", "test","news"))
    


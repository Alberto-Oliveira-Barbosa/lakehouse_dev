def ingest_test():
    from core.spark.spark_session import get_spark
    from core.utils import get_full_path, save_delta

    # Definindo variáveis da função 
    layer = "bronze"
    table_name = "example"
    full_path = get_full_path(table_name, layer)

    spark = get_spark("ingest_test")
    
    # Cria um dataframe de exemplo
    df = spark.createDataFrame([
        {"deptId": 1, "age": 40, "name": "Hyukjin Kwon", "gender": "M", "salary": 50},
        {"deptId": 1, "age": 50, "name": "Takuya Ueshin", "gender": "M", "salary": 100},
        {"deptId": 2, "age": 60, "name": "Xinrong Meng", "gender": "F", "salary": 150},
        {"deptId": 3, "age": 20, "name": "Haejoon Lee", "gender": "M", "salary": 200}
    ])

    # Salva como delta table no bucket do Minio
    save_delta(df, full_path)

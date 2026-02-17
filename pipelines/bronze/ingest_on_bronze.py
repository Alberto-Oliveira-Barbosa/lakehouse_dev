def ingest_test():
    import os
    import inspect
    import logging
    from core.spark.spark_session import get_spark

    # Definindo variáveis da função 
    protocol = "s3a://"
    bucket = "lakehouse"
    layer = "bronze"
    table_name = "example"
    full_path = os.path.join(protocol,bucket,layer,table_name)
    function_name = inspect.currentframe().f_code.co_name
    logging.info(f"Nome capturado na função: {function_name}")

    spark = get_spark("teste")
    
    # Cria um dataframe de exemplo
    df = spark.createDataFrame([
        {"deptId": 1, "age": 40, "name": "Hyukjin Kwon", "gender": "M", "salary": 50},
        {"deptId": 1, "age": 50, "name": "Takuya Ueshin", "gender": "M", "salary": 100},
        {"deptId": 2, "age": 60, "name": "Xinrong Meng", "gender": "F", "salary": 150},
        {"deptId": 3, "age": 20, "name": "Haejoon Lee", "gender": "M", "salary": 200}
    ])


    df.show()

    # Salva como delta table no bucket do Minio
    df.write.format("delta").mode("overwrite").save(full_path)


    

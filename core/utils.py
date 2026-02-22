import logging

def get_full_path(
        table_name:str, 
        layer:str, 
        bucket:str="lakehouse", 
        protocol:str="s3a://"
        ) -> str:

    full_path = f"{protocol}{bucket}/{layer}/{table_name}"

    logging.info(f"Full path: {full_path}")
    return full_path

def _get_credentials():
    import os

    return {
        "ENDPOINT" : os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "USER" : os.getenv("MINIO_ROOT_USER", "minioadmin"),
        "PASSWORD" : os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        }

def save_on_lake(df,save_path):
    import logging
    
    logging.info("Get credentials...")
    credentials = _get_credentials()
    
    storage_options={
            "key": credentials["USER"],
            "secret": credentials["PASSWORD"],
            "client_kwargs": {
                "endpoint_url": credentials["ENDPOINT"]
            }
        }
    
    save_path = save_path.replace("s3a://", "s3://")
    
    try:
        logging.info(f"Saving on {save_path}")
        df.to_excel(save_path, index=False, storage_options=storage_options)
        logging.info("Write completed successfully.")
    except Exception as e:
        logging.exception("Write file failed.")
        raise RuntimeError(f"Write failed for path: {save_path}") from e



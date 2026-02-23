import logging

def get_full_path(
        layer:str,
        domain:str,
        table_name:str, 
        bucket:str="lakehouse", 
        protocol:str="s3a://"
        ) -> str:

    full_path = f"{protocol}{bucket}/{layer}/{domain}/{table_name}"

    logging.info(f"Create a path: {full_path}")
    return full_path

def _get_credentials():
    import os

    return {
        "ENDPOINT" : os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "USER" : os.getenv("MINIO_ROOT_USER", "minioadmin"),
        "PASSWORD" : os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        }

def save_on_lake(df, save_path, **kwargs):
    """
    Save a DataFrame to a Data Lake (S3/MinIO), automatically detecting the file type
    from the file extension. Any extra keyword arguments are passed to the underlying
    pandas save function (to_excel, to_csv, to_parquet).

    Parameters:
    -----------
    df : pd.DataFrame
        The DataFrame to be saved.
    save_path : str
        Full path in the data lake, e.g., "s3://bucket/layer/file.xlsx"
    **kwargs : dict
        Extra keyword arguments to pass to pandas' save functions.

    Notes:
    ------
    - Requires `s3fs` to save to S3/MinIO.
    - For Excel, requires `openpyxl`.
    - Supported formats: Excel (.xlsx), CSV (.csv), Parquet (.parquet)
    """
    import logging
    import os

    logging.info("Get credentials...")
    credentials = _get_credentials()
    
    storage_options = {
        "key": credentials["USER"],
        "secret": credentials["PASSWORD"],
        "client_kwargs": {"endpoint_url": credentials["ENDPOINT"]}
    }

    save_path = save_path.replace("s3a://", "s3://")
    ext = os.path.splitext(save_path)[-1].lower()
    
    try:
        logging.info(f"Saving on {save_path} as {ext}")
        if ext == ".xlsx":
            df.to_excel(save_path, storage_options=storage_options, **kwargs)
        elif ext == ".csv":
            df.to_csv(save_path, storage_options=storage_options, **kwargs)
        elif ext == ".parquet":
            df.to_parquet(save_path, storage_options=storage_options, **kwargs)
        else:
            raise ValueError(f"Unsupported extension '{ext}'. Use '.xlsx', '.csv', or '.parquet'.")
        
        logging.info("Write completed successfully.")
    except Exception as e:
        logging.exception("Write file failed.")
        raise RuntimeError(f"Write failed for path: {save_path}") from e


def read_from_lake(file_path, **kwargs):
    """
    Read a file from a Data Lake (S3/MinIO) and return a pandas DataFrame. Any extra
    keyword arguments are passed to the underlying pandas read function (read_excel,
    read_csv, read_parquet).

    Parameters:
    -----------
    file_path : str
        Full path in the data lake, e.g., "s3://bucket/layer/file.xlsx"
    **kwargs : dict
        Extra keyword arguments to pass to pandas' read functions.

    Returns:
    --------
    pd.DataFrame
        The DataFrame read from the file.

    Notes:
    ------
    - Requires `s3fs` to access S3/MinIO.
    - For Excel files, requires `openpyxl`.
    - Supported formats: Excel (.xlsx), CSV (.csv), Parquet (.parquet)
    """
    import pandas as pd
    import logging
    import os

    logging.info("Get credentials...")
    credentials = _get_credentials()
    
    storage_options = {
        "key": credentials["USER"],
        "secret": credentials["PASSWORD"],
        "client_kwargs": {"endpoint_url": credentials["ENDPOINT"]}
    }

    file_path = file_path.replace("s3a://", "s3://")
    ext = os.path.splitext(file_path)[1].lower()
    
    try:
        logging.info(f"Reading file {file_path} with extension {ext}")
        if ext == ".xlsx":
            df = pd.read_excel(file_path, storage_options=storage_options, **kwargs)
        elif ext == ".csv":
            df = pd.read_csv(file_path, storage_options=storage_options, **kwargs)
        elif ext == ".parquet":
            df = pd.read_parquet(file_path, storage_options=storage_options, **kwargs)
        else:
            raise ValueError(f"Unsupported extension '{ext}'. Use '.xlsx', '.csv', or '.parquet'.")
        
        logging.info(f"File read successfully, {len(df)} rows loaded.")
        return df
    except Exception as e:
        logging.exception("Failed to read file from lake.")
        raise RuntimeError(f"Read failed for path: {file_path}") from e
    
def list_files_in_directory(directory_path, bucket_name="lakehouse"):
    import boto3
    import logging

    credentials = _get_credentials()
    _files = []

    s3 = boto3.client(
        "s3",
        endpoint_url=credentials["ENDPOINT"],
        aws_access_key_id=credentials["USER"],
        aws_secret_access_key=credentials["PASSWORD"],
    )

    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=directory_path
    )

    if "Contents" in response:
        for obj in response["Contents"]:
            logging.info(obj["Key"])
            _files.append(str(obj["Key"]))
    else:
        logging.info(f"No files found in {directory_path}")
    
    return _files
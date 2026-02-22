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

def save_on_lake(df, save_path):
    """
    Save a DataFrame to a Data Lake (S3/MinIO), automatically detecting the file type
    from the file extension.

    Parameters:
    -----------
    df : pd.DataFrame
        The DataFrame to be saved.
    save_path : str
        Full path in the data lake, e.g., "s3://bucket/layer/file.xlsx"

    Notes:
    ------
    - Requires `s3fs` to save to S3/MinIO.
    - For Excel, requires `openpyxl`.
    - Supported formats: Excel (.xlsx), CSV (.csv), Parquet (.parquet)
    """
    import logging
    import os

    # Retrieve credentials for MinIO/S3
    logging.info("Get credentials...")
    credentials = _get_credentials()
    
    storage_options = {
        "key": credentials["USER"],
        "secret": credentials["PASSWORD"],
        "client_kwargs": {"endpoint_url": credentials["ENDPOINT"]}
    }

    # Replace 's3a://' with 's3://' for fsspec compatibility
    save_path = save_path.replace("s3a://", "s3://")

    # Detect file type from the extension
    ext = os.path.splitext(save_path)[-1].lower()
    if ext == ".xlsx":
        file_type = "excel"
    elif ext == ".csv":
        file_type = "csv"
    elif ext == ".parquet":
        file_type = "parquet"
    else:
        raise ValueError(f"Unsupported extension '{ext}'. Use '.xlsx', '.csv', or '.parquet'.")

    try:
        logging.info(f"Saving on {save_path} as {file_type}")

        # Save according to detected file type
        if file_type == "excel":
            df.to_excel(save_path, index=False, storage_options=storage_options)
        elif file_type == "csv":
            df.to_csv(save_path, index=False, storage_options=storage_options)
        elif file_type == "parquet":
            df.to_parquet(save_path, index=False, storage_options=storage_options)

        logging.info("Write completed successfully.")
    except Exception as e:
        logging.exception("Write file failed.")
        raise RuntimeError(f"Write failed for path: {save_path}") from e

def read_from_lake(file_path):
    """
    Read a file from a Data Lake (S3/MinIO) and return a pandas DataFrame.

    Parameters:
    -----------
    file_path : str
        Full path in the data lake, e.g., "s3://bucket/layer/file.xlsx"

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

    # Retrieve credentials for MinIO/S3
    logging.info("Get credentials...")
    credentials = _get_credentials()
    
    storage_options = {
        "key": credentials["USER"],
        "secret": credentials["PASSWORD"],
        "client_kwargs": {"endpoint_url": credentials["ENDPOINT"]}
    }

    # Replace 's3a://' with 's3://' for fsspec compatibility
    file_path = file_path.replace("s3a://", "s3://")

    # Detect file type based on extension
    ext = os.path.splitext(file_path)[1].lower()
    try:
        logging.info(f"Reading file {file_path} with extension {ext}")
        if ext == ".xlsx":
            df = pd.read_excel(file_path, storage_options=storage_options)
        elif ext == ".csv":
            df = pd.read_csv(file_path, storage_options=storage_options)
        elif ext == ".parquet":
            df = pd.read_parquet(file_path, storage_options=storage_options)
        else:
            raise ValueError(f"Unsupported extension '{ext}'. Use '.xlsx', '.csv', or '.parquet'.")
        
        logging.info(f"File read successfully, {len(df)} rows loaded.")
        return df
    except Exception as e:
        logging.exception("Failed to read file from lake.")
        raise RuntimeError(f"Read failed for path: {file_path}") from e
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

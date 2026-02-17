import logging
from typing import Any
from pyspark.sql import DataFrame

def get_full_path(
        table_name:str, 
        layer:str, 
        bucket:str="lakehouse", 
        protocol:str="s3a://"
        ) -> str:

    full_path = f"{protocol}{bucket}/{layer}/{table_name}"

    logging.info(f"Full path: {full_path}")
    return full_path


def save_delta(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
    **writer_options: Any
) -> None:
    """
    Delta writer that accepts any Spark writer options.

    Example:
        save_delta(
            df,
            path,
            mode="append",
            partition_by=["year", "month"],
            mergeSchema="true",
            overwriteSchema="true"
        )
    """

    
    logging.info(f"Writing Delta table to: {path}")

    try:
        writer = df.write.format("delta").mode(mode)

        # Apply partitioning if provided
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        # Apply any additional Spark options
        for key, value in writer_options.items():
            writer = writer.option(key, value)

        writer.save(path)

        logging.info("Delta write completed successfully.")

    except Exception as e:
        logging.exception("Delta write failed.")
        raise RuntimeError(f"Delta write failed for path: {path}") from e

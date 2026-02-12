# utils.py
# small helper functions used across the project

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, count
import logging

# Spark Session creation

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("Utils")

def create_spark_session(app_name: str = "nyc_jobs_app") -> SparkSession:
    log.info("Starting Spark session")

    spark = SparkSession.builder.appName(app_name).getOrCreate()

    log.info("Spark session ready")
    return spark


def null_check(df: DataFrame) -> DataFrame:
    
    #Returns a dataframe showing null count for each column.

    null_counts = []

    for column in df.columns:
        null_counts.append(
            count(when(col(column).isNull(), column)).alias(column)
        )

    return df.select(null_counts)

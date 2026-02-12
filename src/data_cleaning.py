

#data_cleaning.py

#Handles:
#- Null handling
#- Salary normalization
#- Removing invalid records
#- Basic data filtering


from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
import logging

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger("data_cleaning")

def remove_invalid_salary(df: DataFrame) -> DataFrame:
    
    #Remove rows where salary range is null or invalid.
    
    logger.info("Removing invalid salary records")

    df = df.filter((col("Salary Range From").isNotNull()) & (col("Salary Range To").isNotNull()) & (col("Salary Range To") >= col("Salary Range From")))

    return df


def create_avg_salary(df: DataFrame) -> DataFrame:
    
    #Create average salary column.
    
    logger.info("Creating average salary column")

    df = df.withColumn("avg_salary", (col("Salary Range From") + col("Salary Range To")) / 2)

    return df


def normalize_salary(df: DataFrame) -> DataFrame:
    """
    Normalize salary to annual salary.
    Assumptions:
        Annual -> no change
        Hourly -> 2080 hrs/year
        Daily  -> 260 days/year
    """

    logger.info("Normalizing salary to annual basis...")

    df =  df.withColumn("normalized_salary", when(col("Salary Frequency") == "Annual", col("avg_salary"))
        .when(col("Salary Frequency") == "Hourly", col("avg_salary") * 2080)
        .when(col("Salary Frequency") == "Daily", col("avg_salary") * 260)
        .otherwise(col("avg_salary")))

    return df

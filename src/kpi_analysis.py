#kpi_analysis.py

#Contains KPIs.

from pyspark.sql import DataFrame
from pyspark.sql.functions import count, avg, col, year
from pyspark.sql.window import Window
from pyspark.sql.functions import rank


def top_job_categories(df: DataFrame, limit: int = 10) -> DataFrame:
    df = df.groupBy("Job Category")\
        .agg(count("*").alias("job_count"))\
        .orderBy(col("job_count").desc())\
        .limit(limit)

    return df


def salary_distribution_per_category(df: DataFrame) -> DataFrame:
    df = df.groupBy("Job Category")\
        .agg(avg("normalized_salary").alias("avg_salary"))\
        .orderBy(col("avg_salary").desc())
    
    return df


def highest_salary_per_agency(df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("Agency").orderBy(col("normalized_salary").desc())

    df = df.withColumn("rank", rank().over(window_spec).filter(col("rank") == 1).drop("rank"))

    return df


def avg_salary_last_two_years(df: DataFrame) -> DataFrame:
    df = df.filter(year(col("Posting Date")) >= year(col("Posting Date")) - 2)\
        .groupBy("Agency")\
        .agg(avg("normalized_salary").alias("avg_salary"))
    
    return df

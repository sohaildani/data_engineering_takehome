#kpi_analysis.py

#Contains KPIs.

from pyspark.sql import DataFrame
from pyspark.sql.functions import count, avg, col, year, round, max, min
from pyspark.sql.window import Window
from pyspark.sql.functions import rank


def top_job_categories(df: DataFrame, limit: int = 10) -> DataFrame:

    #returns top job categories

    df = df.groupBy("Job Category")\
        .agg(count("*").alias("job_count"))\
        .orderBy(col("job_count").desc())\
        .limit(limit)

    return df


def salary_distribution_per_category(df: DataFrame) -> DataFrame:

    #returns salary distribution per category

    df = df.groupBy("Job Category")\
        .agg(avg("normalized_salary").alias("avg_salary"))\
        .orderBy(col("avg_salary").desc())
    
    return df


def highest_salary_per_agency(df: DataFrame) -> DataFrame:

    #return highest salary per agncy

    window_spec = Window.partitionBy("Agency").orderBy(col("normalized_salary").desc())

    df = df.withColumn("rank", rank().over(window_spec).filter(col("rank") == 1).drop("rank"))

    return df


def avg_salary_last_two_years(df: DataFrame) -> DataFrame:

    #returns avg salary of last two years

    df = df.filter(year(col("Posting Date")) >= year(col("Posting Date")) - 2)\
        .groupBy("Agency")\
        .agg(avg("normalized_salary").alias("avg_salary"))
    
    return df


def top_hiring_agencies(df: DataFrame, limit=10) -> DataFrame:

    #Returns top agencies by number of job postings.

    df = df.groupBy("Agency").count().orderBy(col("count").desc()).limit(limit)

    return df

def salary_range_spread(df: DataFrame) -> DataFrame:

    #Calculates salary spread (difference between max and min salary).
    
    return (
        df.withColumn(
            "salary_spread",
            col("Salary Range To") - col("Salary Range From")
        )
        .select(
            round(avg("salary_spread"), 2).alias("avg_salary_spread"),
            max("salary_spread").alias("max_salary_spread"),
            min("salary_spread").alias("min_salary_spread")
        )
    )
    

def employment_type_distribution(df: DataFrame) -> DataFrame:
    
    #Counts number of Full-Time vs Part-Time jobs.
    
    return (
        df.groupBy("Full-Time/Part-Time indicator")
        .count()
        .orderBy(col("count").desc())
    )

def posting_type_distribution(df: DataFrame) -> DataFrame:

    #Returns distribution of posting types (Internal/External).

    return (
        df.groupBy("Posting Type")
        .count()
        .orderBy(col("count").desc())
    )
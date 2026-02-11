#feature_engineering.py

#Feature engineering:
#- Degree extraction
#- Skill extraction
#- Salary band classification


from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lower


def extract_degree(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "degree_required",
        when(lower(col("Minimum Qual Requirements")).rlike("phd"), "PhD")
        .when(lower(col("Minimum Qual Requirements")).rlike("master"), "Masters")
        .when(lower(col("Minimum Qual Requirements")).rlike("bachelor"), "Bachelors")
        .otherwise("Other")
    )


def extract_skills(df: DataFrame, skills_list: list) -> DataFrame:
    for skill in skills_list:
        df = df.withColumn(skill.lower(), when(lower(col("Preferred Skills")).rlike(skill.lower()), 1).otherwise(0))

    return df


def create_salary_band(df: DataFrame) -> DataFrame:
    return df.withColumn("salary_band", when(col("normalized_salary") < 60000, "Low")
        .when(col("normalized_salary") < 100000, "Mid")
        .otherwise("High")
    )

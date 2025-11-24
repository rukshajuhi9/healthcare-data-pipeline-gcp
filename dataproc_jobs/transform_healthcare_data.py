from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("transform_health_api").getOrCreate()

df = spark.read.format("bigquery").option("table", "leapproject2.health_api.covid_cases_api").load()

# Add sample transformations
df = df.withColumn(
    "age_group",
    when(col("age_group") == "0 - 17 years", "0-17")
    .when(col("age_group") == "18 to 49 years", "18-49")
    .when(col("age_group") == "50 to 64 years", "50-64")
    .otherwise("65+")
)

df.write.format("bigquery") \
    .option("table", "leapproject2.health_api.covid_cases_curated") \
    .mode("overwrite") \
    .save()

spark.stop()

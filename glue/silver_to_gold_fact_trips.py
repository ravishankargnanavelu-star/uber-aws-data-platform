from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

silver_df = spark.read.parquet(
    "s3://uber-data-lake-silver/trips/"
)

fact_trips = silver_df.filter(
    col("status") == "COMPLETED"
).select(
    "trip_id",
    "driver_id",
    "city",
    "fare",
    "distance",
    "event_ts"
)

fact_trips.write.mode("append") \
    .partitionBy("city") \
    .parquet("s3://uber-data-lake-gold/fact_trips/")

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, to_timestamp, row_number, sha2, concat_ws
)
from pyspark.sql.window import Window

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

bronze_df = spark.read.json("s3://uber-data-lake-bronze/trips/")

df = bronze_df.withColumn(
    "event_ts", to_timestamp("event_time")
)

valid_df = df.filter(
    col("trip_id").isNotNull() &
    col("driver_id").isNotNull() &
    (col("fare") >= 0)
)

invalid_df = df.subtract(valid_df)
invalid_df.write.mode("append") \
    .parquet("s3://uber-data-lake-quarantine/trips/")

window_spec = Window.partitionBy("trip_id").orderBy(col("event_ts").desc())

silver_df = (
    valid_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

silver_df = silver_df.withColumn(
    "rider_id_masked",
    sha2(concat_ws("||", col("rider_id")), 256)
).drop("rider_id")

silver_df.write.mode("append") \
    .partitionBy("city") \
    .parquet("s3://uber-data-lake-silver/trips/")

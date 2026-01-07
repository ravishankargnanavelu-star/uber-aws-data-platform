from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import lit, current_timestamp

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

new_df = spark.read.parquet(
    "s3://uber-data-lake-silver/drivers/"
)

old_df = spark.read.parquet(
    "s3://uber-data-lake-gold/dim_driver/"
)

changes = new_df.join(
    old_df,
    "driver_id",
    "inner"
).filter(
    (new_df.rating != old_df.rating) |
    (new_df.city != old_df.city)
)

expired = old_df.join(
    changes, "driver_id", "left_semi"
).withColumn(
    "end_ts", current_timestamp()
).withColumn(
    "is_current", lit(False)
)

current = changes.withColumn(
    "start_ts", current_timestamp()
).withColumn(
    "end_ts", lit(None)
).withColumn(
    "is_current", lit(True)
)

final_df = expired.unionByName(current)

final_df.write.mode("overwrite") \
    .parquet("s3://uber-data-lake-gold/dim_driver/")

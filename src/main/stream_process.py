import os, math
from functools import partial

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, split
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col, udf

url = os.getenv("db_url")


def postgres_sink(df, epoch_id, table_name):
    properties = {
        "driver": "org.postgresql.Driver",
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }
    df.persist(StorageLevel.MEMORY_AND_DISK)

    # df.write.jdbc(url=url, table=table_name, mode="append", properties=properties)

    df.write.format("com.databricks.spark.csv").option("header", True).mode(
        "append"
    ).save("app/results")

    df.unpersist()


def get_street_section(lat):

    if lat < 35.7:
        return "X1A"
    else:
        return "X1B"


udfStreet = udf(get_street_section, StringType())


def calc_speed(lat1, lng1, time1, lat2, lng2, time2):
    A = 69.1 * (lat2 - lat1)
    B = 69.1 * (lng2 - lng1) * math.cos(lat1 / 57.3)
    d = ((A ** 2 + B ** 2) ** 0.5) * 1609.344
    t = time2 - time1
    return (d / t.seconds) * 3.6


def apply_location_requirement(df):

    df = df.withColumn("driverId", df["driverId"].cast("int").alias("driverId"))
    df = df.withColumn("lng", df["lng"].cast("float").alias("lng"))
    df = df.withColumn("lat", df["lat"].cast("float").alias("lat"))
    df = df.withColumn("time", df["time"].cast("timestamp").alias("time"))

    df = df.withColumn("street", udfStreet("lat"))
    df = df.withColumn("street", df["street"].cast("string").alias("street"))

    return df


def main():
    TOPIC_NAME = os.getenv("TOPIC_NAME")
    brokerAddresses = os.getenv("brokerAddresses")

    # Read from kafka
    spark = SparkSession.builder.appName("Driver GPS Streaming").getOrCreate()

    sdf_locations = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokerAddresses)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)",)
    )

    sdf_locations_schema = StructType(
        [
            StructField("driverId", StringType(), True),
            StructField("lat", StringType(), True),
            StructField("lng", StringType(), True),
            StructField("time", StringType(), True),
        ]
    )

    # sdf_locations_data = (
    #     sdf_locations.select(col("value").cast("string")).alias("csv").select("csv.*")
    # )

    sdf_locations_data = sdf_locations.select(
        from_json("value", sdf_locations_schema).alias("a")
    ).select("a.*")

    sdf_locations_data = apply_location_requirement(sdf_locations_data)

    # sdf_locations_data = sdf_locations_data.withWatermark("time", "15 minutes")

    sdf_locations_data.printSchema()
    # sdf_locations_data.writeStream.format("console").start().awaitTermination()

    # query = sdf_locations_data.groupBy("driverId").count()

    # query.writeStream.outputMode("complete").format("console").option(
    #     "truncate", False
    # ).start().awaitTermination()

    sdf_locations_data.writeStream.outputMode("append").format("csv").foreachBatch(
        partial(postgres_sink, table_name="locations")
    ).trigger(processingTime="10 minutes").start().awaitTermination()


if __name__ == "__main__":
    main()

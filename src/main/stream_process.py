import os
from functools import partial

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, split
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel

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
        .selectExpr("CAST(value AS STRING)")
    )

    sdf_locations_schema = StructType(
        [
            StructField("driverId", IntegerType()),
            StructField("lat", DecimalType()),
            StructField("lng", DecimalType()),
            StructField("time", TimestampType()),
        ]
    )

    sdf_locations_data = sdf_locations.select(
        from_json("value", sdf_locations_schema).alias("a")
    ).select("a.*")

    sdf_locations_data = sdf_locations_data.withWatermark("time", "15 minutes")

    sdf_locations_data.printSchema()
    # sdf_locations_data.writeStream.format("console").start().awaitTermination()

    sdf_locations_data.writeStream.outputMode("append").format("csv").foreachBatch(
        partial(postgres_sink, table_name="locations")
    ).trigger(processingTime="10 minutes").start().awaitTermination()


if __name__ == "__main__":
    main()

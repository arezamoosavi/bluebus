import os, math
from functools import partial

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, split
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col, udf, when, lag, lit, isnull
from pyspark.sql.window import Window

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


def calc_speed(lat, d_lat, d_lng, time, prev_time):

    A = 69.1 * d_lat
    B = 69.1 * d_lng * math.cos(lat / 57.3)
    d = ((A ** 2 + B ** 2) ** 0.5) * 1609.344
    d_time = time - prev_time
    return (d / d_time.seconds) * 3.6


udfVelocity = udf(calc_speed, FloatType())


def apply_location_requirement(df):

    df = df.withColumn("driverId", df["driverId"].cast("int").alias("driverId"))
    df = df.withColumn("lng", df["lng"].cast("float").alias("lng"))
    df = df.withColumn("lat", df["lat"].cast("float").alias("lat"))
    df = df.withColumn("time", df["time"].cast("timestamp").alias("time"))

    df = df.withColumn("street", udfStreet("lat"))
    df = df.withColumn("street", df["street"].cast("string").alias("street"))

    w = Window().partitionBy("driverId").orderBy("time")
    distinct_driverId = df.select("driverId").distinct()
    _dfs = []

    for driver_id in distinct_driverId:
        ndf = df.filter(df.driverId == driver_id)
        ndf = ndf.withColumn("prev_lng", lag("lng", 1).over(w))
        ndf = ndf.withColumn(
            "prev_lng", ndf["prev_lng"].cast("float").alias("prev_lng")
        )

        ndf = ndf.withColumn("prev_lat", lag("lat", 1).over(w))
        ndf = ndf.withColumn(
            "prev_lat", ndf["prev_lat"].cast("float").alias("prev_lat")
        )

        ndf = ndf.withColumn("prev_time", lag("time", 1).over(w))

        ndf = ndf.withColumn(
            "prev_time", ndf["prev_time"].cast("timestamp").alias("prev_time")
        )

        ndf = ndf.withColumn(
            "d_lng",
            when(col("prev_lng").isNotNull(), col("lng") - col("prev_lng")).otherwise(
                0
            ),
        )
        ndf = ndf.withColumn(
            "d_lat",
            when(col("prev_lat").isNotNull(), col("lat") - col("prev_lat")).otherwise(
                0
            ),
        )

        _dfs.append(ndf)

    df = reduce(DataFrame.unionAll, _dfs)

    df = df.withColumn(
        "velocity", udfVelocity("lat", "d_lat", "d_lng", "time", "prev_time")
    )
    df = df.withColumn("velocity", df["velocity"].cast("float").alias("velocity"))

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

    # query = sdf_locations_data.groupBy("driverId").count()
    query = sdf_locations_data.filter(sdf_locations_data.driverId == 133)

    query.writeStream.format("console").start().awaitTermination()

    # sdf_locations_data.writeStream.outputMode("append").format("csv").foreachBatch(
    #     partial(postgres_sink, table_name="locations")
    # ).trigger(processingTime="10 minutes").start().awaitTermination()


if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import IntegerType, TimestampType

from config import settings
from models.vote import Vote

VOTE_SCHEMA = Vote.spark_schema()


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(settings.spark.app_name)
        .master(settings.spark.master)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.jars", settings.spark.postgresql_jar_path)
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def write_stream_to_kafka(df, topic: str, checkpoint: str):
    return (
        df.selectExpr("to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("topic", topic)
        .option("checkpointLocation", f"{settings.spark.checkpoint_dir}/{checkpoint}")
        .outputMode("update")
        .start()
    )


if __name__ == "__main__":
    spark = build_spark_session()

    votes_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", settings.kafka.votes_topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), VOTE_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("voting_time", col("voting_time").cast(TimestampType()))
        .withColumn("vote", col("vote").cast(IntegerType()))
        .withWatermark("voting_time", settings.spark.watermark_duration)
    )

    votes_per_candidate = votes_df.groupBy(
        "candidate_id", "candidate_name", "party_affiliation", "photo_url"
    ).agg(_sum("vote").alias("total_votes"))

    turnout_by_location = votes_df.groupBy("address.state").count()

    q1 = write_stream_to_kafka(
        votes_per_candidate,
        settings.kafka.aggregated_votes_per_candidate_topic,
        "checkpoint1",
    )
    q2 = write_stream_to_kafka(
        turnout_by_location,
        settings.kafka.aggregated_turnout_by_location_topic,
        "checkpoint2",
    )

    q1.awaitTermination()
    q2.awaitTermination()

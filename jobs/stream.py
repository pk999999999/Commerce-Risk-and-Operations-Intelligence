"""Kafka -> validated lake, PostgreSQL, ClickHouse and Elasticsearch."""
from __future__ import annotations

from datetime import datetime
import json
import logging
import os

from pyspark.sql import SparkSession, functions as F, types as T

from app.core import parse_time, validate_event

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
LOG = logging.getLogger("commerce.stream")
LAKE = os.getenv("LAKE_PATH", "/opt/project/lake")
CHECKPOINTS = os.getenv("CHECKPOINT_PATH", "/opt/project/checkpoints")


def validation_error(value: str) -> str | None:
    try:
        validate_event(json.loads(value))
        return None
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        return str(exc)


def archive_batch(batch, batch_id: int) -> None:
    if batch.isEmpty():
        return
    batch.select("key", "value", "kafka_timestamp").write.mode("append").parquet(f"{LAKE}/raw")
    error_udf = F.udf(validation_error, T.StringType())
    rejected = batch.withColumn("reason", error_udf("value")).filter(F.col("reason").isNotNull())
    if not rejected.isEmpty():
        rejected.select("key", "value", "kafka_timestamp", "reason").write.mode("append").parquet(f"{LAKE}/dead_letter")


def process_partition(rows) -> None:
    import clickhouse_connect
    from elasticsearch import Elasticsearch
    from app.store import process_event

    ch = clickhouse_connect.get_client(host=os.getenv("CLICKHOUSE_HOST", "clickhouse"), username="commerce", password=os.environ["CLICKHOUSE_PASSWORD"])
    es = Elasticsearch(os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200"))
    for row in rows:
        event = json.loads(row.value)
        try:
            result = process_event(event)
        except ValueError as exc:
            LOG.warning("rejected_event tenant=%s event_id=%s reason=%s", event.get("tenant_id"), event.get("event_id"), exc)
            continue
        for alert in result["alerts"]:
            try:
                es.index(index="commerce-alerts-v1", id=f"{event['tenant_id']}:{alert['alert_id']}", document={**alert, "tenant_id": event["tenant_id"]})
            except Exception:
                LOG.exception("search_index_failed tenant=%s alert_id=%s", event["tenant_id"], alert["alert_id"])
        if event["event_type"] == "order":
            payload = event["payload"]
            at = parse_time(event["occurred_at"])
            ch.insert("commerce.order_facts", [[event["tenant_id"], payload["order_id"], event["market"], str(payload["amount"]), payload["currency"], at, int(at.timestamp() * 1000)]], column_names=["tenant_id", "order_id", "market", "amount", "currency", "order_at", "version"])


def process_batch(batch, batch_id: int) -> None:
    if batch.isEmpty():
        return
    batch.select("value", "tenant_id", "event_type", "event_time").write.mode("append").partitionBy("tenant_id", "event_type").parquet(f"{LAKE}/curated")
    batch.select("value").rdd.foreachPartition(process_partition)
    LOG.info("processed_batch id=%s count=%s", batch_id, batch.count())


def main() -> None:
    spark = SparkSession.builder.appName("commerce-risk-stream").config("spark.sql.shuffle.partitions", "3").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    source = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
        .option("subscribe", "commerce.events.v1")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true").load())
    decoded = source.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value", "timestamp AS kafka_timestamp")
    raw_query = decoded.writeStream.foreachBatch(archive_batch).option("checkpointLocation", f"{CHECKPOINTS}/raw").start()
    valid_udf = F.udf(validation_error, T.StringType())
    valid = (decoded.withColumn("validation_error", valid_udf("value"))
        .filter(F.col("validation_error").isNull())
        .withColumn("tenant_id", F.get_json_object("value", "$.tenant_id"))
        .withColumn("event_id", F.get_json_object("value", "$.event_id"))
        .withColumn("event_type", F.get_json_object("value", "$.event_type"))
        .withColumn("event_time", F.to_timestamp(F.get_json_object("value", "$.occurred_at")))
        .filter(F.col("event_time").isNotNull())
        .withWatermark("event_time", "48 hours")
        .dropDuplicates(["tenant_id", "event_id"]))
    curated_query = valid.writeStream.foreachBatch(process_batch).option("checkpointLocation", f"{CHECKPOINTS}/curated").start()
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

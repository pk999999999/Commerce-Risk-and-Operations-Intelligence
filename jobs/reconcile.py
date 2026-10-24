"""Replay raw events and replace queryable daily facts deterministically."""
from __future__ import annotations

import json
import logging
import os
import time

import clickhouse_connect
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession, Window, functions as F, types as T

from app.core import validate_event
from app.lake import LakePaths
from app.store import connect
from jobs.stream import process_partition, validation_error

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("commerce.reconcile")
LAKE = LakePaths.from_env()


def rebuild_search() -> int:
    es = Elasticsearch(os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200"))
    count = 0
    with connect() as db:
        for row in db.execute("SELECT tenant_id,alert_id,order_id,rule,severity,evidence FROM alerts"):
            es.index(index="commerce-alerts-v1", id=f"{row['tenant_id']}:{row['alert_id']}", document=row)
            count += 1
    return count


def main() -> None:
    spark = (SparkSession.builder.master("local[2]").appName("commerce-daily-reconciliation")
        .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = spark.read.parquet(LAKE.path("raw"))
    except Exception as exc:
        if "PATH_NOT_FOUND" in str(exc) or "does not exist" in str(exc):
            LOG.info("No raw events yet; nothing to reconcile")
            return
        raise
    valid_udf = F.udf(validation_error, T.StringType())
    events = (raw.filter(F.col("value").isNotNull())
        .withColumn("validation_error", valid_udf("value", "key"))
        .filter(F.col("validation_error").isNull())
        .withColumn("tenant_id", F.get_json_object("value", "$.tenant_id"))
        .withColumn("event_id", F.get_json_object("value", "$.event_id"))
        .withColumn("event_type", F.get_json_object("value", "$.event_type"))
        .withColumn("market", F.get_json_object("value", "$.market"))
        .withColumn("currency", F.get_json_object("value", "$.payload.currency"))
        .withColumn("order_id", F.get_json_object("value", "$.payload.order_id"))
        .withColumn("amount", F.get_json_object("value", "$.payload.amount").cast(T.DecimalType(18, 2)))
        .withColumn("event_time", F.to_timestamp(F.get_json_object("value", "$.occurred_at")))
        .dropDuplicates(["tenant_id", "event_id"]))
    with connect() as db:
        allowed = [(row["tenant_id"], row["code"]) for row in db.execute("SELECT tenant_id,code FROM markets")]
    if not allowed:
        LOG.info("No registered markets; nothing to reconcile")
        return
    allowed_df = spark.createDataFrame(allowed, ["tenant_id", "market"])
    events = events.join(F.broadcast(allowed_df), ["tenant_id", "market"], "inner")
    # PostgreSQL's event key makes replay safe; ClickHouse ReplacingMergeTree repairs
    # a stream microbatch that committed PostgreSQL before its analytics insert.
    events.select("value").rdd.foreachPartition(process_partition)
    orders = events.filter(F.col("event_type") == "order")
    latest = orders.withColumn("rank", F.row_number().over(Window.partitionBy("tenant_id", "order_id").orderBy(F.col("event_time").desc(), F.col("event_id").desc()))).filter("rank = 1")
    daily = latest.groupBy("tenant_id", F.to_date("event_time").alias("day"), "market", "currency").agg(F.count("order_id").alias("order_count"), F.sum("amount").alias("gross_amount"))
    rows = daily.collect()
    ch = clickhouse_connect.get_client(host=os.getenv("CLICKHOUSE_HOST", "clickhouse"), username="commerce", password=os.environ["CLICKHOUSE_PASSWORD"])
    version = int(time.time() * 1000)
    if rows:
        ch.insert("commerce.daily_metrics", [[r.tenant_id, r.day, r.market, r.currency, r.order_count, r.gross_amount, version] for r in rows], column_names=["tenant_id", "day", "market", "currency", "order_count", "gross_amount", "version"])
    indexed = rebuild_search()
    LOG.info("reconciliation_complete groups=%s alerts_indexed=%s", len(rows), indexed)
    spark.stop()


if __name__ == "__main__":
    main()

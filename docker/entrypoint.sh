#!/usr/bin/env bash
set -euo pipefail

# Submit the LakeStream streaming job. Delta / Kafka / S3A jars are resolved
# by --packages so they match the Spark 3.5.1 runtime exactly. All behaviour
# is driven by env vars (see reference.conf) so this entrypoint is generic.

SPARK_PACKAGES="io.delta:delta-spark_2.12:3.2.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.hadoop:hadoop-aws:3.3.4"

exec "${SPARK_HOME}/bin/spark-submit" \
  --master "${SPARK_MASTER:-local[*]}" \
  --class dev.xj16.lakestream.LakeStreamApp \
  --packages "${SPARK_PACKAGES}" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.driver.memory="${DRIVER_MEMORY:-1g}" \
  --conf spark.executor.memory="${EXECUTOR_MEMORY:-1g}" \
  /opt/lakestream/app.jar

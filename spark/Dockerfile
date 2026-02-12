# Spark image with Iceberg runtime + Kafka connector JARs pre-installed.
#
# Why bake JARs into the image instead of using --packages at runtime?
#   1. No Maven download on every spark-submit (faster, works offline)
#   2. Avoids Ivy/Coursier cache permission issues in non-root containers
#   3. Reproducible — the exact JAR versions are pinned here
#
# Versions:
#   Spark 3.5.6  (bitnami base)
#   Iceberg 1.6.1  (spark-runtime uber-jar)
#   spark-sql-kafka 3.5.3  +  transitive deps
#
# If bitnami/spark:3.5.6 fails (BSI paywall), swap to the free legacy image:
#   FROM bitnamilegacy/spark:3.5.6

FROM bitnamilegacy/spark:3.5.6

USER root

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# ── Iceberg runtime (single uber-jar — includes all Iceberg deps) ────
RUN curl -fSL -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"

# ── Spark Kafka connector + transitive deps ──────────────────────────
RUN curl -fSL -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar" && \
    curl -fSL -o /opt/bitnami/spark/jars/kafka-clients-3.5.2.jar \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.2/kafka-clients-3.5.2.jar" && \
    curl -fSL -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar" && \
    curl -fSL -o /opt/bitnami/spark/jars/commons-pool2-2.12.0.jar \
      "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar"

# Ensure JARs are world-readable
RUN chmod 644 /opt/bitnami/spark/jars/iceberg-*.jar \
              /opt/bitnami/spark/jars/spark-sql-kafka-*.jar \
              /opt/bitnami/spark/jars/kafka-clients-*.jar \
              /opt/bitnami/spark/jars/spark-token-provider-*.jar \
              /opt/bitnami/spark/jars/commons-pool2-*.jar

# Stay as root so volume mounts (/tmp/iceberg_warehouse, /tmp/checkpoints)
# are always writable.  In production, run as a dedicated non-root user.
# USER 1001

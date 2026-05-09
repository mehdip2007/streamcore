"""
SparkSession factory.

Why a factory function instead of creating SparkSession directly?
-----------------------------------------------------------------
SparkSession is expensive to create and must be created ONCE per
application. By centralizing creation here:
  - All jobs get identical configuration
  - Easy to change settings in one place
  - Testable — you can inject a test session

Local Mode vs Cluster Mode:
---------------------------
We use master("local[*]") which means:
  - Spark runs entirely inside your Python process
  - [*] means use ALL available CPU cores
  - No separate cluster needed — perfect for development
  - On your M1 with 8 cores, this is genuinely fast

In production you'd replace "local[*]" with the cluster URL:
  "spark://master:7077"  (standalone cluster)
  "yarn"                  (Hadoop YARN)
  "k8s://..."             (Kubernetes)

The rest of the code doesn't change — only this one line.

Kafka Integration Package:
--------------------------
PySpark doesn't include Kafka support by default. We need the
spark-sql-kafka connector JAR. We tell Spark to download it
automatically via spark.jars.packages. It downloads once and
caches locally. Requires internet on first run.
"""
from functools import lru_cache

from pyspark.sql import SparkSession

from producers.core.logging_setup import get_logger

log = get_logger(__name__)

# Kafka connector for Spark Structured Streaming.
# Format: groupId:artifactId:version (Maven coordinates)
# Must match your Spark + Scala version.
# Spark 3.5.x uses Scala 2.12
KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"


@lru_cache(maxsize=1)
def get_spark_session(app_name: str = "StreamCore") -> SparkSession:
    """
    Create or return the singleton SparkSession.

    lru_cache(maxsize=1) ensures only ONE SparkSession ever exists.
    Calling this function 100 times returns the same instance.
    Creating a second SparkSession in the same process raises an error.
    """
    log.info("spark_session_creating", app_name=app_name)

    spark = (
        SparkSession.builder
        .appName(app_name)

        # Local mode — all cores on your machine
        .master("local[*]")

        # Auto-download Kafka connector on first run
        .config("spark.jars.packages", KAFKA_CONNECTOR)

        # Reduce Spark's extremely verbose logging to warnings only.
        # Spark logs thousands of INFO lines — they drown out your app logs.
        .config("spark.sparkContext.logLevel", "WARN")

        # Smaller shuffle partitions for local dev.
        # Default is 200 — way too many for a single machine.
        # For local streaming, 4 is plenty.
        .config("spark.sql.shuffle.partitions", "4")

        # Checkpoint location for stateful streaming operations.
        # Spark uses this to recover state after a restart.
        .config(
            "spark.sql.streaming.checkpointLocation",
            "/tmp/streamcore_checkpoints",
        )

        .getOrCreate()
    )

    # Silence the Spark logger after session creation
    spark.sparkContext.setLogLevel("WARN")

    log.info(
        "spark_session_created",
        app_name=app_name,
        master=spark.sparkContext.master,
        version=spark.version,
    )

    return spark

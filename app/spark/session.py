"""
Shared SparkSession factory for KG V25.2.
PostgreSQL JDBC driver is auto-downloaded via Maven on first run.
"""
from app.core.logger import info


def get_spark(app_name: str = 'KG-V25-Spark',
              driver_memory: str = '8g',
              shuffle_partitions: int = 200) -> 'SparkSession':
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(app_name)
        # Auto-downloads postgresql JDBC driver from Maven on first run
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.7.3')
        .config('spark.driver.memory', driver_memory)
        .config('spark.sql.shuffle.partitions', str(shuffle_partitions))
        .config('spark.default.parallelism', str(shuffle_partitions))
        .config('spark.sql.adaptive.enabled', 'true')
        .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('WARN')
    info(f'SparkSession ready — version={spark.version}, '
         f'master={spark.sparkContext.master}')
    return spark


def jdbc_url(host: str, port: int, database: str) -> str:
    return f'jdbc:postgresql://{host}:{port}/{database}'


def jdbc_props(user: str, password: str) -> dict:
    return {
        'user': user,
        'password': password,
        'driver': 'org.postgresql.Driver',
        'fetchsize': '10000',
    }

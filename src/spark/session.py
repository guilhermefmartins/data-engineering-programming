"""Gerenciamento centralizado da SparkSession."""
from pyspark.sql import SparkSession

from src.config import AppConfig


class SparkSessionManager:
    """Encapsula a criação e o ciclo de vida da SparkSession."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._spark: SparkSession | None = None

    def get_or_create(self) -> SparkSession:
        if self._spark is None:
            self._spark = (
                SparkSession.builder.appName(self._config.app_name)
                .master(self._config.spark_master)
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel(self._config.spark_log_level)
        return self._spark

    def stop(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

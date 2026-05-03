"""Camada de escrita de dados."""
from pyspark.sql import DataFrame

from src.config import AppConfig


class ParquetWriter:
    """Persiste um DataFrame em Parquet conforme configuração."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config

    def write(self, df: DataFrame) -> None:
        (
            df.write.mode(self._config.output_mode)
            .format(self._config.output_format)
            .save(self._config.output_path)
        )

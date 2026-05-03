"""Orquestração do pipeline.

Coordena leitura → transformação → escrita usando exclusivamente
as dependências injetadas (não instancia nada por conta própria).
"""
import logging

from src.business import PedidosPagamentosTransformer
from src.config import AppConfig
from src.io import PagamentosReader, PedidosReader, ParquetWriter
from src.spark import SparkSessionManager


class PipelineOrchestrator:
    """Aggregation root da execução do pipeline."""

    def __init__(
        self,
        config: AppConfig,
        spark_manager: SparkSessionManager,
        pedidos_reader: PedidosReader,
        pagamentos_reader: PagamentosReader,
        transformer: PedidosPagamentosTransformer,
        writer: ParquetWriter,
    ) -> None:
        self._config = config
        self._spark_manager = spark_manager
        self._pedidos_reader = pedidos_reader
        self._pagamentos_reader = pagamentos_reader
        self._transformer = transformer
        self._writer = writer
        self._logger = logging.getLogger(self.__class__.__name__)

    def run(self) -> None:
        self._logger.info("Pipeline %s iniciado", self._config.app_name)
        try:
            df_pedidos = self._pedidos_reader.read()
            df_pagamentos = self._pagamentos_reader.read()
            df_resultado = self._transformer.transform(df_pedidos, df_pagamentos)
            self._writer.write(df_resultado)
            self._logger.info(
                "Pipeline %s finalizado com sucesso", self._config.app_name
            )
        finally:
            self._spark_manager.stop()
            self._pedidos_reader.cleanup()
            self._pagamentos_reader.cleanup()

"""Aggregation Root.

Único ponto onde dependências são instanciadas. Todas as classes são
construídas aqui e injetadas no orquestrador, que então é executado.
"""
import logging

from src.business import PedidosPagamentosTransformer
from src.config import AppConfig
from src.io import PagamentosReader, PedidosReader, ParquetWriter
from src.pipeline import PipelineOrchestrator
from src.spark import SparkSessionManager


def main() -> None:
    # 1. Configuração
    config = AppConfig()

    # 2. Logging global
    logging.basicConfig(level=config.log_level, format=config.log_format)

    # 3. SparkSession
    spark_manager = SparkSessionManager(config)
    spark = spark_manager.get_or_create()

    # 4. Camadas de I/O
    pedidos_reader = PedidosReader(spark, config)
    pagamentos_reader = PagamentosReader(spark, config)
    writer = ParquetWriter(config)

    # 5. Lógica de negócio
    transformer = PedidosPagamentosTransformer(config)

    # 6. Orquestrador (recebe TODAS as dependências via construtor)
    orchestrator = PipelineOrchestrator(
        config=config,
        spark_manager=spark_manager,
        pedidos_reader=pedidos_reader,
        pagamentos_reader=pagamentos_reader,
        transformer=transformer,
        writer=writer,
    )

    # 7. Execução
    orchestrator.run()


if __name__ == "__main__":
    main()

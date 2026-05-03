"""Schemas explicitos para todos os DataFrames do pipeline.

Nenhum schema deve ser inferido automaticamente; todas as fontes
declaram aqui o seu StructType.
"""
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    TimestampType,
)


class PedidosSchema:
    """Schema do dataset de pedidos (CSV mensal, separador ';')."""

    @staticmethod
    def schema() -> StructType:
        return StructType(
            [
                StructField("ID_PEDIDO", StringType(), nullable=False),
                StructField("PRODUTO", StringType(), nullable=True),
                StructField("VALOR_UNITARIO", DoubleType(), nullable=True),
                StructField("QUANTIDADE", IntegerType(), nullable=True),
                StructField("DATA_CRIACAO", TimestampType(), nullable=True),
                StructField("UF", StringType(), nullable=True),
                StructField("ID_CLIENTE", StringType(), nullable=True),
                StructField("MES", StringType(), nullable=True),
            ]
        )


class PagamentosSchema:
    """Schema do dataset de pagamentos (JSON lines, com STRUCT de fraude)."""

    @staticmethod
    def schema() -> StructType:
        avaliacao_fraude = StructType(
            [
                StructField("fraude", BooleanType(), nullable=True),
                StructField("score_fraude", DoubleType(), nullable=True),
            ]
        )
        return StructType(
            [
                StructField("id_pedido", StringType(), nullable=False),
                StructField("forma_pagamento", StringType(), nullable=True),
                StructField("valor_pagamento", DoubleType(), nullable=True),
                StructField("status", BooleanType(), nullable=True),
                StructField("data_processamento", TimestampType(), nullable=True),
                StructField("avaliacao_fraude", avaliacao_fraude, nullable=True),
                StructField("arquivo", StringType(), nullable=True),
                StructField("MES", StringType(), nullable=True),
            ]
        )

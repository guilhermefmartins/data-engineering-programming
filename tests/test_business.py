"""Testes unitários da lógica de negócio."""
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

from src.business import PedidosPagamentosTransformer
from src.config import AppConfig
from src.schemas import PagamentosSchema, PedidosSchema


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("tests-data-engineering-programming")
        .master("local[1]")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def config() -> AppConfig:
    return AppConfig()


def _build_pedidos(spark: SparkSession):
    schema = PedidosSchema.schema()
    rows = [
        ("P1", "ProdA", 10.0, 2, datetime(2025, 1, 1, 12, 0, 0), "SP", "C1", "01"),
        ("P2", "ProdB", 5.0, 4, datetime(2025, 1, 2, 12, 0, 0), "RJ", "C2", "01"),
        ("P3", "ProdC", 7.0, 1, datetime(2025, 1, 3, 12, 0, 0), "MG", "C3", "01"),
    ]
    return spark.createDataFrame(rows, schema=schema)


def _build_pagamentos(spark: SparkSession):
    schema = PagamentosSchema.schema()
    # P1 → status=False, fraude=False (deve passar)
    # P2 → status=True (filtrado)
    # P3 → status=False, fraude=True (filtrado)
    rows = [
        Row(
            id_pedido="P1",
            forma_pagamento="CARTAO_CREDITO",
            valor_pagamento=20.0,
            status=False,
            data_processamento=datetime(2025, 1, 1, 13, 0, 0),
            avaliacao_fraude=Row(fraude=False, score_fraude=0.1),
            arquivo="pagamentos-2025-01.json.gz",
            MES="01",
        ),
        Row(
            id_pedido="P2",
            forma_pagamento="PIX",
            valor_pagamento=20.0,
            status=True,
            data_processamento=datetime(2025, 1, 2, 13, 0, 0),
            avaliacao_fraude=Row(fraude=False, score_fraude=0.1),
            arquivo="pagamentos-2025-01.json.gz",
            MES="01",
        ),
        Row(
            id_pedido="P3",
            forma_pagamento="BOLETO",
            valor_pagamento=7.0,
            status=False,
            data_processamento=datetime(2025, 1, 3, 13, 0, 0),
            avaliacao_fraude=Row(fraude=True, score_fraude=0.9),
            arquivo="pagamentos-2025-01.json.gz",
            MES="01",
        ),
    ]
    return spark.createDataFrame(rows, schema=schema)


def test_transform_filtra_status_e_fraude_e_agrega_valor(spark, config):
    transformer = PedidosPagamentosTransformer(config)
    df_pedidos = _build_pedidos(spark)
    df_pagamentos = _build_pagamentos(spark)

    resultado = transformer.transform(df_pedidos, df_pagamentos).collect()

    assert len(resultado) == 1
    linha = resultado[0]
    assert linha["id_pedido"] == "P1"
    assert linha["uf"] == "SP"
    assert linha["forma_pagamento"] == "CARTAO_CREDITO"
    assert float(linha["valor_total_pedido"]) == pytest.approx(20.0)


def test_transform_retorna_colunas_esperadas(spark, config):
    transformer = PedidosPagamentosTransformer(config)
    df_pedidos = _build_pedidos(spark)
    df_pagamentos = _build_pagamentos(spark)

    resultado = transformer.transform(df_pedidos, df_pagamentos)

    assert resultado.columns == [
        "id_pedido",
        "uf",
        "forma_pagamento",
        "valor_total_pedido",
        "data_pedido",
    ]

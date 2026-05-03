"""Lógica de negócio do pipeline.

Reproduz a query final do notebook Databricks:

    SELECT
        p.ID_PEDIDO         AS id_pedido,
        p.UF                AS uf,
        pg.forma_pagamento,
        SUM(p.VALOR_UNITARIO * p.QUANTIDADE) AS valor_total_pedido,
        TO_TIMESTAMP(p.DATA_CRIACAO)         AS data_pedido
    FROM   vw_pedidos      p
    LEFT JOIN vw_pagamentos pg ON p.ID_PEDIDO = pg.id_pedido
    WHERE  pg.status = false
       AND pg.avaliacao_fraude.fraude = false
    GROUP BY p.ID_PEDIDO, p.UF, pg.forma_pagamento, p.DATA_CRIACAO
"""
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, to_timestamp

from src.config import AppConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class PedidosPagamentosTransformer:
    """Aplica join + agregação entre pedidos e pagamentos."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(self._config.log_level)

    def transform(
        self, df_pedidos: DataFrame, df_pagamentos: DataFrame
    ) -> DataFrame:
        try:
            self._logger.info(
                "Iniciando transformação pedidos x pagamentos "
                "(pedidos=%s, pagamentos=%s)",
                df_pedidos.count(),
                df_pagamentos.count(),
            )

            joined = df_pedidos.alias("p").join(
                df_pagamentos.alias("pg"),
                on=col("p.ID_PEDIDO") == col("pg.id_pedido"),
                how="left",
            )

            filtered = joined.where(
                (col("pg.status") == False)  # noqa: E712
                & (col("pg.avaliacao_fraude.fraude") == False)  # noqa: E712
            )

            result = (
                filtered.groupBy(
                    col("p.ID_PEDIDO").alias("id_pedido"),
                    col("p.UF").alias("uf"),
                    col("pg.forma_pagamento").alias("forma_pagamento"),
                    to_timestamp(col("p.DATA_CRIACAO")).alias("data_pedido"),
                ).agg(
                    spark_sum(col("p.VALOR_UNITARIO") * col("p.QUANTIDADE")).alias(
                        "valor_total_pedido"
                    )
                )
            ).select(
                "id_pedido",
                "uf",
                "forma_pagamento",
                "valor_total_pedido",
                "data_pedido",
            )

            self._logger.info(
                "Transformação concluída — linhas resultantes: %s", result.count()
            )
            return result

        except Exception as exc:  # noqa: BLE001
            self._logger.error(
                "Falha na transformação pedidos x pagamentos: %s", exc, exc_info=True
            )
            raise

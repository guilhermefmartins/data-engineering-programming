"""Configurações centralizadas da aplicação.

Toda configuração estática (URLs das fontes, paths de saída,
parâmetros do Spark, nível de log) vive aqui e é injetada via
construtor nas demais classes.
"""
from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class AppConfig:
    # Identificação
    app_name: str = "data_engineering_programming"

    # Fontes de dados
    pedidos_base_url: str = (
        "https://raw.githubusercontent.com/"
        "infobarbosa/datasets-csv-pedidos/main/data/pedidos/"
    )
    pedidos_file_pattern: str = "pedidos-2025-{mes}.csv.gz"
    pagamentos_api_url: str = (
        "https://api.github.com/repos/"
        "infobarbosa/dataset-json-pagamentos/contents/data/pagamentos"
    )
    pagamentos_file_prefix: str = "pagamentos-2025"
    pagamentos_file_suffix: str = ".json.gz"

    # Janela de processamento
    meses: List[str] = field(
        default_factory=lambda: [f"{m:02d}" for m in range(1, 13)]
    )

    # Saída
    output_path: str = "./output/pedidos_pagamentos"
    output_format: str = "parquet"
    output_mode: str = "overwrite"

    # Spark
    spark_master: str = "local[*]"
    spark_log_level: str = "WARN"

    # Logging
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"

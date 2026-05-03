"""Camada de leitura de dados.

Cada classe encapsula a lógica de aquisição de uma fonte específica
e devolve um Spark DataFrame com schema *explícito*.

Importante: o Spark é lazy. Os arquivos baixados precisam continuar
existindo até a action (count/write) executar. Por isso usamos
`tempfile.mkdtemp` (sem auto-remoção) e expomos `cleanup()` para o
orquestrador chamar no `finally` do pipeline.
"""
import os
import shutil
import tempfile
import urllib.request
from typing import List, Optional

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from src.config import AppConfig
from src.schemas import PedidosSchema, PagamentosSchema


class _BaseReader:
    """Base com utilitários comuns de download para leitura HTTP→local."""

    def __init__(self, spark: SparkSession, config: AppConfig) -> None:
        self._spark = spark
        self._config = config
        self._tmp_dir: Optional[str] = None

    def _ensure_tmp_dir(self, prefix: str) -> str:
        if self._tmp_dir is None:
            self._tmp_dir = tempfile.mkdtemp(prefix=prefix)
        return self._tmp_dir

    @staticmethod
    def _download(url: str, dest_dir: str, file_name: str) -> str:
        local_path = os.path.join(dest_dir, file_name)
        urllib.request.urlretrieve(url, local_path)
        return local_path

    def cleanup(self) -> None:
        if self._tmp_dir and os.path.isdir(self._tmp_dir):
            shutil.rmtree(self._tmp_dir, ignore_errors=True)
            self._tmp_dir = None


class PedidosReader(_BaseReader):
    """Lê os CSVs mensais de pedidos do repositório público."""

    def read(self) -> DataFrame:
        schema = PedidosSchema.schema()
        tmp = self._ensure_tmp_dir("pedidos_")
        paths: List[str] = []
        for mes in self._config.meses:
            file_name = self._config.pedidos_file_pattern.format(mes=mes)
            url = f"{self._config.pedidos_base_url}{file_name}"
            local_path = self._download(url, tmp, file_name)
            paths.append(local_path)

        df = (
            self._spark.read.option("header", "true")
            .option("sep", ";")
            .option("compression", "gzip")
            .schema(schema)
            .csv(paths)
        )
        return df


class PagamentosReader(_BaseReader):
    """Lista o diretório no GitHub e lê todos os JSON.gz de pagamentos."""

    def read(self) -> DataFrame:
        schema = PagamentosSchema.schema()
        response = requests.get(self._config.pagamentos_api_url, timeout=30)
        response.raise_for_status()
        files = response.json()

        tmp = self._ensure_tmp_dir("pagamentos_")
        df: Optional[DataFrame] = None
        for file in files:
            nome = file["name"]
            if not (
                nome.startswith(self._config.pagamentos_file_prefix)
                and nome.endswith(self._config.pagamentos_file_suffix)
            ):
                continue

            local_path = self._download(file["download_url"], tmp, nome)
            mes = nome.split("-")[2][:2]
            df_mes = (
                self._spark.read.option("compression", "gzip")
                .schema(schema)
                .json(local_path)
                .withColumn("arquivo", lit(nome))
                .withColumn("MES", lit(mes))
            )
            df = df_mes if df is None else df.unionByName(df_mes)

        if df is None:
            raise ValueError("Nenhum arquivo de pagamentos encontrado na fonte.")
        return df

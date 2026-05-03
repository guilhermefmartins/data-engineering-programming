# data-engineering-programming

Pipeline PySpark que reproduz, em código modular orientado a objetos, o
notebook Databricks da disciplina **Data Engineering Programming** (FIAP).

A partir das fontes públicas do professor Infobarbosa, o pipeline:

1. Lê 12 CSVs mensais de **pedidos** (2025).
2. Lê os JSONs mensais de **pagamentos** (2025) — incluindo o STRUCT
   `avaliacao_fraude`.
3. Faz `LEFT JOIN` por `id_pedido`, filtra `status = false` e
   `avaliacao_fraude.fraude = false`, e agrega o valor total por pedido.
4. Persiste o resultado em **Parquet**.

## Pré-requisitos (importante)

| Dependência | Versão | Obrigatório? |
|---|---|---|
| **Python** | 3.10 ou superior | sim |
| **Java (JDK)** | 17 (recomendado) — Java 11 funciona se usar PySpark 3.5 e Python ≤ 3.12 | sim |
| **Acesso à internet** | para baixar pedidos/pagamentos do GitHub | sim na 1ª execução |

> O `requirements.txt` instala apenas pacotes Python. **Java e Python precisam estar instalados na máquina antes** — pip não verifica isso.

### Instalando Java 17 no macOS

```bash
brew install openjdk@17
export JAVA_HOME="$(brew --prefix)/opt/openjdk@17"
export PATH="$JAVA_HOME/bin:$PATH"
java --version   # deve mostrar: openjdk 17.x.x
```

### Instalando Java 17 no Linux (Ubuntu/Debian)

```bash
sudo apt update && sudo apt install -y openjdk-17-jdk
java --version
```

## Quickstart

```bash
git clone https://github.com/guilhermefmartins/data-engineering-programming.git
cd data-engineering-programming
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest -v          # roda os testes unitários (critério 12)
python main.py     # executa o pipeline completo
```

## Estrutura

```
data-engineering-programming/
├── main.py                  # Aggregation Root (instancia + injeta tudo)
├── pyproject.toml
├── requirements.txt
├── MANIFEST.in
├── README.md
├── src/
│   ├── config/              # AppConfig (configs centralizadas)
│   ├── spark/               # SparkSessionManager
│   ├── schemas/             # StructTypes explícitos
│   ├── io/                  # PedidosReader, PagamentosReader, ParquetWriter
│   ├── business/            # PedidosPagamentosTransformer (logging + try/except)
│   └── pipeline/            # PipelineOrchestrator
└── tests/
    └── test_business.py     # pytest
```

## Mapeamento dos critérios

| # | Critério                          | Onde                                              |
|---|-----------------------------------|---------------------------------------------------|
| 1 | Schemas explícitos                | `src/schemas/schemas.py`                          |
| 2 | Orientação a objetos              | Todas as classes em `src/`                        |
| 3 | Injeção de dependências           | `main.py` instancia, `PipelineOrchestrator` recebe|
| 4 | Configurações centralizadas       | `src/config/config.py` (`AppConfig`)              |
| 5 | Sessão Spark                      | `src/spark/session.py` (`SparkSessionManager`)    |
| 6 | Leitura/escrita                   | `src/io/{reader.py,writer.py}`                    |
| 7 | Lógica de negócio                 | `src/business/transformer.py`                     |
| 8 | Orquestração                      | `src/pipeline/orchestrator.py`                    |
| 9 | Logging                           | `import logging` em `business/transformer.py`     |
| 10| Tratamento de erros (try/except)  | `transformer.py` + log do erro                    |
| 11| Empacotamento                     | `pyproject.toml`, `requirements.txt`, `MANIFEST.in`, `README.md` |
| 12| Testes unitários (pytest)         | `tests/test_business.py`                          |

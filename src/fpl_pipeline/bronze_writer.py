import json
from datetime import datetime, timezone
from typing import Any
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, NullType, StructType
from fpl_pipeline.logging_config import get_logger, log_step

logger = get_logger(__name__)


def _now_utc() -> datetime:
    """Timestamp d'ingestion, utilisé pour toutes les tables d'un même run."""
    return datetime.now(timezone.utc)


def _add_raw_payload(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ajoute le JSON brut comme champ de chaque record, avant la création du DataFrame."""
    enriched = []
    for r in records:
        enriched_record = dict(r)  # copie pour ne pas muter l'original
        enriched_record["raw_payload"] = json.dumps(r)
        enriched.append(enriched_record)
    return enriched


def _drop_null_type_columns(df: DataFrame) -> DataFrame:
    """Drop les colonnes contenant du NullType (Delta ne supporte pas NullType dans les types complexes)."""

    def _has_null_type(dt):
        if isinstance(dt, NullType):
            return True
        if isinstance(dt, ArrayType):
            return _has_null_type(dt.elementType)
        if isinstance(dt, StructType):
            return any(_has_null_type(f.dataType) for f in dt.fields)
        return False

    cols_to_drop = [f.name for f in df.schema.fields if _has_null_type(f.dataType)]
    if cols_to_drop:
        df = df.drop(*cols_to_drop)
    return df

@log_step("bronze_write", table="players_snapshot")
def write_bronze_players(spark, bootstrap_data, catalog, schema="bronze"):
    elements = bootstrap_data["elements"]
    logger.info("bronze_write_started", table="players_snapshot", row_count=len(elements))
    enriched = _add_raw_payload(elements)  # payload déjà dans chaque dict

    df = spark.createDataFrame(pd.DataFrame(enriched))
    df = df.withColumn("ingestion_ts", F.lit(_now_utc()))

    table_name = f"{catalog}.{schema}.players_snapshot"
    df = _drop_null_type_columns(df)
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)

@log_step("bronze_write", table="teams_snapshot")
def write_bronze_teams(
    spark: SparkSession,
    bootstrap_data: dict[str, Any],
    catalog: str,
    schema: str = "bronze",
) -> None:
    """Écrit bootstrap['teams'] dans bronze.teams_snapshot."""
    teams = bootstrap_data["teams"]
    ingestion_ts = _now_utc()

    df = spark.createDataFrame(pd.DataFrame(_add_raw_payload(teams)))

    df = df.withColumn("ingestion_ts", F.lit(ingestion_ts))
    df = _drop_null_type_columns(df)

    table_name = f"{catalog}.{schema}.teams_snapshot"
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)

@log_step("bronze_write", table="fixtures_snapshot")
def write_bronze_fixtures(
    spark: SparkSession,
    fixtures_data: list[dict[str, Any]],
    catalog: str,
    schema: str = "bronze",
) -> None:
    """Écrit fixtures/ dans bronze.fixtures_snapshot."""
    ingestion_ts = _now_utc()

    df = spark.createDataFrame(pd.DataFrame(_add_raw_payload(fixtures_data)))

    df = df.withColumn("ingestion_ts", F.lit(ingestion_ts))
    df = _drop_null_type_columns(df)

    table_name = f"{catalog}.{schema}.fixtures_snapshot"
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)

@log_step("bronze_write", table="players_history_snapshot")
def write_bronze_player_history_batch(
    spark: SparkSession,
    all_history_records: list[dict[str, Any]],
    catalog: str,
    schema: str = "bronze",
) -> None:
    """
    Écrit l'historique de TOUS les joueurs en une seule fois.
    all_history_records : liste de dicts, chacun étant une ligne d'historique
    (déjà enrichie avec player_id), toutes gameweeks/joueurs confondus.
    """
    if not all_history_records:
        return

    ingestion_ts = _now_utc()

    df = spark.createDataFrame(_add_raw_payload(all_history_records))
    df = df.withColumn("ingestion_ts", F.lit(ingestion_ts))

    table_name = f"{catalog}.{schema}.player_gameweek_history"
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)

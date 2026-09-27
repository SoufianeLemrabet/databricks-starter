import json
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _now_utc() -> datetime:
    """Timestamp d'ingestion, utilisé pour toutes les tables d'un même run."""
    return datetime.now(timezone.utc)


def _add_raw_payload(df: DataFrame, records: list[dict[str, Any]]) -> DataFrame:
    """Ajoute une colonne raw_payload contenant le JSON brut de chaque enregistrement."""
    raw_payloads = [json.dumps(r) for r in records]
    payload_df = df.sparkSession.createDataFrame(
        [(i, p) for i, p in enumerate(raw_payloads)], ["_row_id", "raw_payload"]
    )
    df_with_id = df.withColumn("_row_id", F.monotonically_increasing_id())
    return df_with_id.join(payload_df, on="_row_id", how="left").drop("_row_id")


def write_bronze_players(
    spark: SparkSession,
    bootstrap_data: dict[str, Any],
    catalog: str,
    schema: str = "bronze",
) -> None:
    """Écrit bootstrap['elements'] dans bronze.players_snapshot."""
    elements = bootstrap_data["elements"]
    ingestion_ts = _now_utc()

    df = spark.createDataFrame(elements)
    df = _add_raw_payload(df, elements)
    df = df.withColumn("ingestion_ts", F.lit(ingestion_ts))

    table_name = f"{catalog}.{schema}.players_snapshot"
    df.write.format("delta").mode("append").saveAsTable(table_name)


def write_bronze_teams(
    spark: SparkSession,
    bootstrap_data: dict[str, Any],
    catalog: str,
    schema: str = "bronze",
) -> None:
    """Écrit bootstrap['teams'] dans bronze.teams_snapshot."""
    teams = bootstrap_data["teams"]
    ingestion_ts = _now_utc()

    df = spark.createDataFrame(teams)
    df = _add_raw_payload(df, teams)
    df = df.withColumn("ingestion_ts", F.lit(ingestion_ts))

    table_name = f"{catalog}.{schema}.teams_snapshot"
    df.write.format("delta").mode("append").saveAsTable(table_name)


def write_bronze_fixtures(
    spark: SparkSession,
    fixtures_data: list[dict[str, Any]],
    catalog: str,
    schema: str = "bronze",
) -> None:
    """Écrit fixtures/ dans bronze.fixtures_snapshot."""
    ingestion_ts = _now_utc()

    df = spark.createDataFrame(fixtures_data)
    df = _add_raw_payload(df, fixtures_data)
    df = df.withColumn("ingestion_ts", F.lit(ingestion_ts))

    table_name = f"{catalog}.{schema}.fixtures_snapshot"
    df.write.format("delta").mode("append").saveAsTable(table_name)


def write_bronze_player_history(
    spark: SparkSession,
    player_id: int,
    history_data: dict[str, Any],
    catalog: str,
    schema: str = "bronze",
) -> None:
    """Écrit element-summary/{player_id}/['history'] dans bronze.player_gameweek_history."""
    history = history_data["history"]
    if not history:
        return  # joueur sans historique (ex: n'a pas encore joué cette saison)

    ingestion_ts = _now_utc()

    df = spark.createDataFrame(history)
    df = _add_raw_payload(df, history)
    df = df.withColumn("ingestion_ts", F.lit(ingestion_ts)).withColumn(
        "player_id", F.lit(player_id)
    )

    table_name = f"{catalog}.{schema}.player_gameweek_history"
    df.write.format("delta").mode("append").saveAsTable(table_name)

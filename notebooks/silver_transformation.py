# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Transformation — FPL Pipeline
# MAGIC Lit les tables Bronze, applique les transforms, écrit Silver en overwrite.
# MAGIC Dépend d'un run Bronze réussi (dépendance de tâche à configurer dans le Job).

# COMMAND ----------
import sys
sys.path.append("../src")

from fpl_pipeline.silver_transforms import (
    transform_silver_teams,
    transform_silver_players,
    transform_silver_fixtures,
    transform_silver_player_gameweek_stats,
)
from fpl_pipeline.logging_config import configure_logging, get_logger

configure_logging(json_logs=False)
logger = get_logger("silver_transformation")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
CATALOG = "fpl"  # TODO: même catalog que Bronze

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lecture Bronze

# COMMAND ----------
logger.info("silver_run_started")

bronze_teams = spark.table(f"{CATALOG}.bronze.teams_snapshot")
bronze_players = spark.table(f"{CATALOG}.bronze.players_snapshot")
bronze_fixtures = spark.table(f"{CATALOG}.bronze.fixtures_snapshot")
bronze_history = spark.table(f"{CATALOG}.bronze.player_gameweek_history")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Transformations
# MAGIC Ordre imposé par les dépendances : teams d'abord (utilisée par players et fixtures),
# MAGIC puis players et fixtures (utilisées par player_gameweek_stats).

# COMMAND ----------
silver_teams = transform_silver_teams(bronze_teams)
silver_players = transform_silver_players(bronze_players, silver_teams)
silver_fixtures = transform_silver_fixtures(bronze_fixtures, silver_teams)
silver_stats = transform_silver_player_gameweek_stats(bronze_history, silver_players, silver_fixtures)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Contrôles de qualité
# MAGIC TODO : à écrire — unicité sur (player_id, fixture_id) dans silver_stats,
# MAGIC pas de team_id orphelin, element_type dans [1,4], difficulty dans [1,5].

# COMMAND ----------
# TODO

# COMMAND ----------
# MAGIC %md
# MAGIC ## Écriture Silver

# COMMAND ----------
silver_teams.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.silver.teams")
silver_players.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.silver.players")
silver_fixtures.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.silver.fixtures")
silver_stats.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.silver.player_gameweek_stats")

logger.info("silver_run_completed")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Vérification post-écriture

# COMMAND ----------
for table in ["teams", "players", "fixtures", "player_gameweek_stats"]:
    count = spark.table(f"{CATALOG}.silver.{table}").count()
    print(f"{table}: {count} lignes")
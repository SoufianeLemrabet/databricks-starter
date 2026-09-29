# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Aggregation — FPL Pipeline
# MAGIC Lit les tables Silver, calcule les agrégats de recommandation, écrit Gold en overwrite.
# MAGIC Dépend d'un run Silver réussi (dépendance de tâche configurée dans le Job).



# COMMAND ----------
import sys
sys.path.append("../src")

from fpl_pipeline.gold_transforms import (
    transform_gold_player_form,
    transform_gold_player_value,
    transform_gold_upcoming_fixtures_difficulty,
    transform_gold_player_recommendation_scores,
)
from fpl_pipeline.data_quality import (
    validate_gold_player_recommendation_scores,
    DataQualityError,
)
from fpl_pipeline.logging_config import configure_logging, get_logger

configure_logging(json_logs=False)
logger = get_logger("gold_aggregation")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
CATALOG = "fpl"  # TODO: même catalog que Bronze/Silver

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lecture Silver

# COMMAND ----------
logger.info("gold_run_started")

silver_teams = spark.table(f"{CATALOG}.silver.teams")
silver_players = spark.table(f"{CATALOG}.silver.players")
silver_fixtures = spark.table(f"{CATALOG}.silver.fixtures")
silver_stats = spark.table(f"{CATALOG}.silver.player_gameweek_stats")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Transformations
# MAGIC Ordre imposé par les dépendances : form/value/difficulty d'abord (indépendantes entre elles),
# MAGIC puis le score composite qui les combine toutes.

# COMMAND ----------
gold_player_form = transform_gold_player_form(silver_stats)
gold_player_value = transform_gold_player_value(silver_players)
gold_upcoming_difficulty = transform_gold_upcoming_fixtures_difficulty(silver_fixtures)

# COMMAND ----------
gold_recommendation_scores = transform_gold_player_recommendation_scores(
    gold_player_form,
    gold_player_value,
    gold_upcoming_difficulty,
    silver_players,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Contrôles de qualité
# MAGIC Bloquant sur les 3 tables intermédiaires : pas encore écrit, uniquement fait sur le score
# MAGIC final pour l'instant. À toi de juger si form/value/difficulty méritent leurs propres
# MAGIC contrôles avant d'être combinées, ou si valider seulement le résultat final suffit.

# COMMAND ----------
try:
    validate_gold_player_recommendation_scores(gold_recommendation_scores)
except DataQualityError:
    logger.error("gold_run_aborted_data_quality")
    raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Écriture Gold

# COMMAND ----------
gold_player_form.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.player_form")
gold_player_value.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.gold.player_value")
gold_upcoming_difficulty.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.gold.upcoming_fixtures_difficulty"
)
gold_recommendation_scores.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.gold.player_recommendation_scores"
)

logger.info("gold_run_completed")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Vérification post-écriture

# COMMAND ----------
for table in ["player_form", "player_value", "upcoming_fixtures_difficulty", "player_recommendation_scores"]:
    count = spark.table(f"{CATALOG}.gold.{table}").count()
    print(f"{table}: {count} lignes")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Aperçu rapide du top 10

# COMMAND ----------
display(
    spark.table(f"{CATALOG}.gold.player_recommendation_scores")
    .orderBy("recommendation_score", ascending=False)
    .limit(10)
)
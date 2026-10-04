# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Team Optimization — FPL Pipeline
# MAGIC Lit gold.player_recommendation_scores, calcule l'équipe optimale (15 + formation
# MAGIC titulaire), écrit le résultat dans gold.optimal_team.
# MAGIC Dépend d'un run gold_aggregation réussi.

# COMMAND ----------
%pip install pulp
dbutils.library.restartPython()

# COMMAND ----------
import sys
sys.path.append("../src")

from fpl_pipeline.team_optimizer import PlayerCandidate, build_optimal_team
from fpl_pipeline.logging_config import configure_logging, get_logger

configure_logging(json_logs=False)
logger = get_logger("team_optimization")

# COMMAND ----------
dbutils.widgets.text("catalog", "")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lecture Gold

# COMMAND ----------
logger.info("optimization_run_started")

scores_df = spark.table(f"{CATALOG}.gold.player_recommendation_scores")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Construction des candidats
# MAGIC Le solveur travaille sur une liste Python, pas sur un DataFrame Spark — conversion
# MAGIC nécessaire ici. Table Gold de taille modeste, le collect() est sans risque.
# MAGIC photo et team_code inclus : nécessaires pour l'affichage côté app, pas pour le solveur.

# COMMAND ----------
rows = scores_df.select(
    "player_id", "web_name", "position", "team_id", "team_code",
    "cost_millions", "photo", "recommendation_score",
).collect()

candidates = [
    PlayerCandidate(
        player_id=r["player_id"],
        web_name=r["web_name"],
        position=r["position"],
        team_id=r["team_id"],
        team_code=r["team_code"],
        cost_millions=r["cost_millions"],
        photo=r["photo"],
        recommendation_score=r["recommendation_score"],
    )
    for r in rows
]

logger.info("candidates_loaded", count=len(candidates))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Optimisation

# COMMAND ----------
try:
    result = build_optimal_team(candidates)
except ValueError as e:
    logger.error("optimization_failed", error=str(e))
    raise

logger.info(
    "optimization_completed",
    total_cost=result["total_cost"],
    total_score=result["total_score"],
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mise en forme et écriture

# COMMAND ----------
from pyspark.sql import Row

def _to_rows(players, is_starter: bool):
    return [
        Row(
            player_id=p.player_id,
            web_name=p.web_name,
            position=p.position,
            team_id=p.team_id,
            team_code=p.team_code,
            cost_millions=p.cost_millions,
            photo=p.photo,
            recommendation_score=p.recommendation_score,
            is_starter=is_starter,
        )
        for p in players
    ]

all_rows = _to_rows(result["starting_xi"], is_starter=True) + _to_rows(result["bench"], is_starter=False)
optimal_team_df = spark.createDataFrame(all_rows)

optimal_team_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{CATALOG}.gold.optimal_team")

logger.info("optimization_run_completed")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Aperçu

# COMMAND ----------
print(f"Coût total : {result['total_cost']:.1f}M / 100.0M")
print(f"Score total (titulaires) : {result['total_score']:.3f}")
display(optimal_team_df.orderBy("is_starter", ascending=False))
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Ingestion — FPL Pipeline
# MAGIC Orchestration notebook : appelle le client API et écrit les résultats en Delta.
# MAGIC Tourne une fois par gameweek terminée. Aucune logique métier ici — tout vit dans `src/fpl_pipeline/`.

# COMMAND ----------

import sys

sys.path.append("../src")


from fpl_pipeline.api_client import FPLClient
from fpl_pipeline.bronze_writer import (
    write_bronze_players,
    write_bronze_teams,
    write_bronze_fixtures,
    write_bronze_player_history_batch,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration


# COMMAND ----------
dbutils.widgets.text("catalog", "")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialisation du client

# COMMAND ----------

client = FPLClient()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS fpl.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap : joueurs, équipes, gameweeks

# COMMAND ----------

bootstrap_data = client.get_bootstrap()

# COMMAND ----------



write_bronze_players(spark, bootstrap_data, catalog=CATALOG)
write_bronze_teams(spark, bootstrap_data, catalog=CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vérification : la dernière gameweek est-elle bien terminée ?
# MAGIC On ne veut ingérer l'historique que si la gameweek est `finished` (et idéalement `data_checked`).

# COMMAND ----------

events = bootstrap_data["events"]
finished_events = [e for e in events if e["finished"]]

if not finished_events:
    dbutils.notebook.exit("Aucune gameweek terminée pour le moment — arrêt du run.")

last_finished_gw = finished_events[-1]
print(
    f"Dernière gameweek terminée : {last_finished_gw['id']} — data_checked={last_finished_gw['data_checked']}"
)

# COMMAND ----------
current_gw = next((e for e in events if e["is_current"]), None) or next((e for e in events if e["is_next"]), None)
spark.createDataFrame([{
    "gameweek_id": current_gw["id"],
    "name": current_gw["name"],
    "deadline_time": current_gw["deadline_time"],
    "finished": current_gw["finished"],
}]).write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.bronze.current_gameweek")


# COMMAND ----------

fixtures_data = client.get_fixtures()
write_bronze_fixtures(spark, fixtures_data, catalog=CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Historique par joueur
# MAGIC ⚠️ Un appel API par joueur (700+). `write_bronze_player_history` écrit actuellement en append
# MAGIC à chaque appel — problème de small files sur autant d'écritures. À corriger avant un vrai run :
# MAGIC accumuler tous les résultats et faire un seul write groupé, comme évoqué précédemment.

# COMMAND ----------

# COMMAND ----------
player_ids = [e["id"] for e in bootstrap_data["elements"]]

all_history_records = []
for player_id in player_ids:
    history_data = client.get_player_history(player_id)
    for record in history_data["history"]:
        record["player_id"] = player_id  # enrichissement avant accumulation
        all_history_records.append(record)

# COMMAND ----------
write_bronze_player_history_batch(spark, all_history_records, catalog=CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vérification post-ingestion

# COMMAND ----------

for table in [
    "players_snapshot",
    "teams_snapshot",
    "fixtures_snapshot",
    "player_gameweek_history",
]:
    count = spark.table(f"{CATALOG}.bronze.{table}").count()
    print(f"{table}: {count} lignes")
# Databricks notebook source

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
    write_bronze_player_history,
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
CATALOG = "fpl"  # TODO: renseigner ton catalog Unity Catalog

# COMMAND ----------
# MAGIC %md
# MAGIC ## Initialisation du client

# COMMAND ----------
client = FPLClient()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bootstrap : joueurs, équipes, gameweeks

# COMMAND ----------
bootstrap_data = client.get_bootstrap()

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
# MAGIC %md
# MAGIC ## Fixtures

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
player_ids = [e["id"] for e in bootstrap_data["elements"]]

for player_id in player_ids:
    history_data = client.get_player_history(player_id)
    write_bronze_player_history(spark, player_id, history_data, catalog=CATALOG)

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

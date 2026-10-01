# app/app.py
import os

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config
from databricks.sql.exc import DatabaseError, OperationalError

st.set_page_config(page_title="FPL Player Rankings", layout="wide")


@st.cache_resource
def get_connection():
    cfg = Config()  # lit DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET automatiquement
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: cfg.authenticate,
    )


@st.cache_data(ttl=600)
def load_player_rankings(catalog: str) -> pd.DataFrame:
    conn = get_connection()
    query = f"""
        SELECT web_name, position, team_id, cost_millions, recommendation_score
        FROM {catalog}.gold.player_recommendation_scores
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()


# --- Sidebar ---
st.sidebar.header("Paramètres")
catalog = st.sidebar.text_input("Catalog", value="fpl_project")

if st.sidebar.button("Rafraîchir les données", type="primary"):
    st.cache_data.clear()

# --- Chargement ---
try:
    df = load_player_rankings(catalog)
except (DatabaseError, OperationalError) as e:
    st.error(f"Erreur de connexion aux données : {e}")
    st.stop()

if df.empty:
    st.warning("Aucune donnée disponible — vérifie que le pipeline Gold a bien tourné.")
    st.stop()

# --- Corps principal ---
st.title("⚽ FPL Player Rankings")
st.caption(
    "Classement par poste basé sur gold.player_recommendation_scores — "
    "forme récente, rapport points/prix et calendrier des prochains matchs."
)

POSITION_LABELS = {
    "GKP": "Gardiens",
    "DEF": "Défenseurs",
    "MID": "Milieux",
    "FWD": "Attaquants",
}

tabs = st.tabs(list(POSITION_LABELS.values()))

for tab, (position, label) in zip(tabs, POSITION_LABELS.items()):
    with tab:
        ranked = (
            df[df["position"] == position]
            .sort_values("recommendation_score", ascending=False)
            .reset_index(drop=True)
        )
        ranked.index += 1  # rang lisible à partir de 1, pas 0

        if ranked.empty:
            st.info(f"Aucun joueur disponible à ce poste ({label}).")
            continue

        display_df = ranked[
            ["web_name", "cost_millions", "recommendation_score"]
        ].rename(
            columns={
                "web_name": "Joueur",
                "cost_millions": "Prix (M)",
                "recommendation_score": "Score",
            }
        )
        display_df["Score"] = display_df["Score"].round(3)

        st.dataframe(display_df, use_container_width=True)

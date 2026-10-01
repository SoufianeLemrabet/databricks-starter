# app/app.py
import os

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

CATALOG = "fpl_project"  # catalog fixé, plus de choix utilisateur

st.set_page_config(page_title="FPL Player Rankings", page_icon="⚽", layout="wide")

# --- Styling : couleurs de marque Premier League (violet / magenta), sans logo officiel ---
st.markdown(
    """
    <style>
    .stApp {
        background: linear-gradient(135deg, #3D195B 0%, #1A0B2E 100%);
    }
    .main-header {
        text-align: center;
        padding: 1.5rem 0 0.5rem 0;
    }
    .main-header h1 {
        color: #FFFFFF;
        font-size: 2.5rem;
        font-weight: 800;
        margin-bottom: 0;
    }
    .main-header p {
        color: #E6D9F2;
        font-size: 1rem;
    }
    div[data-testid="stMetric"] {
        background-color: rgba(255, 255, 255, 0.08);
        border-radius: 10px;
        padding: 1rem;
        border: 1px solid #FF2882;
    }
    div[data-testid="stMetricLabel"] { color: #E6D9F2; }
    div[data-testid="stMetricValue"] { color: #FFFFFF; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        background-color: rgba(255, 255, 255, 0.08);
        border-radius: 8px 8px 0 0;
        color: #E6D9F2;
        padding: 10px 20px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #FF2882 !important;
        color: #FFFFFF !important;
    }
    div[data-testid="stDataFrame"] {
        background-color: rgba(255, 255, 255, 0.05);
        border-radius: 10px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def get_connection():
    cfg = Config()
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: cfg.authenticate,
    )


@st.cache_data(ttl=600)
def load_player_rankings() -> pd.DataFrame:
    conn = get_connection()
    query = f"""
        SELECT first_name, second_name, web_name, position, team_id,
               cost_millions, recommendation_score
        FROM {CATALOG}.gold.player_recommendation_scores
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()


# --- Sidebar ---
st.sidebar.header("⚙️ Paramètres")
if st.sidebar.button("🔄 Rafraîchir les données", type="primary"):
    st.cache_data.clear()

# --- Chargement ---
try:
    df = load_player_rankings()
except Exception as e:  # noqa: BLE001 — frontière UI, on veut toujours afficher un message plutôt que planter
    st.error(f"Erreur de connexion aux données : {e}")
    st.stop()

if df.empty:
    st.warning("Aucune donnée disponible — vérifie que le pipeline Gold a bien tourné.")
    st.stop()

df["full_name"] = df["first_name"] + " " + df["second_name"]

# --- Header ---
st.markdown(
    """
    <div class="main-header">
        <h1>⚽ FPL Player Rankings</h1>
        <p>Classement par poste — forme récente, rapport points/prix et calendrier des prochains matchs</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# --- Corps principal ---
POSITION_LABELS = {
    "GKP": "🧤 Gardiens",
    "DEF": "🛡️ Défenseurs",
    "MID": "⚡ Milieux",
    "FWD": "🎯 Attaquants",
}

tabs = st.tabs(list(POSITION_LABELS.values()))

for tab, (position, label) in zip(tabs, POSITION_LABELS.items()):
    with tab:
        ranked = (
            df[df["position"] == position]
            .sort_values("recommendation_score", ascending=False)
            .reset_index(drop=True)
        )
        ranked.index += 1

        if ranked.empty:
            st.info(f"Aucun joueur disponible à ce poste ({label}).")
            continue

        col1, col2, col3 = st.columns(3)
        col1.metric("Joueurs disponibles", len(ranked))
        col2.metric("Prix moyen", f"{ranked['cost_millions'].mean():.1f}M")
        col3.metric("Meilleur score", f"{ranked['recommendation_score'].max():.3f}")

        display_df = ranked[
            ["full_name", "cost_millions", "recommendation_score"]
        ].rename(
            columns={
                "full_name": "Joueur",
                "cost_millions": "Prix (M)",
                "recommendation_score": "Score",
            }
        )
        display_df["Score"] = display_df["Score"].round(3)

        st.dataframe(display_df, use_container_width=True)

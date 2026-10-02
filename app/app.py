# app/app.py
import os

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

CATALOG = "fpl_project"

st.set_page_config(page_title="FPL Intelligence", page_icon="⚽", layout="wide")

# --- Design chic : fond sombre profond, accents or/magenta, typographie large ---
st.markdown(
    """
    <style>
    .stApp {
        background: radial-gradient(circle at top, #1A0B2E 0%, #0A0510 70%);
    }
    .top-header {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 1.2rem;
        padding: 2rem 0 1rem 0;
        border-bottom: 1px solid rgba(255, 215, 0, 0.2);
        margin-bottom: 2rem;
    }
    .top-header img { height: 60px; }
    .top-header h1 {
        color: #FFFFFF;
        font-size: 2.2rem;
        font-weight: 700;
        letter-spacing: 0.5px;
        margin: 0;
        background: linear-gradient(90deg, #FFD700, #FF2882);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .gw-badge {
        display: inline-block;
        background: linear-gradient(90deg, #FF2882, #3D195B);
        color: white;
        padding: 0.4rem 1.2rem;
        border-radius: 30px;
        font-weight: 600;
        font-size: 0.95rem;
        margin-bottom: 1.5rem;
    }
    .player-card {
        background: linear-gradient(145deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02));
        border: 1px solid rgba(255, 215, 0, 0.25);
        border-radius: 14px;
        padding: 0.8rem;
        text-align: center;
        transition: transform 0.2s;
    }
    .player-card img {
        width: 64px;
        height: 64px;
        border-radius: 50%;
        object-fit: cover;
        border: 2px solid #FFD700;
    }
    .player-card .name { color: #FFFFFF; font-weight: 600; font-size: 0.85rem; margin-top: 0.4rem; }
    .player-card .pos { color: #FF2882; font-size: 0.75rem; font-weight: 500; }
    .player-card .price { color: #FFD700; font-size: 0.8rem; }
    div[data-testid="stMetric"] {
        background: rgba(255, 215, 0, 0.06);
        border-radius: 12px;
        padding: 1rem;
        border: 1px solid rgba(255, 215, 0, 0.3);
    }
    div[data-testid="stMetricValue"] { color: #FFD700; }
    div[data-testid="stMetricLabel"] { color: #E6D9F2; }
    section[data-testid="stSidebar"] {
        background: #0A0510;
        border-right: 1px solid rgba(255, 215, 0, 0.15);
    }
    .stTabs [data-baseweb="tab"] {
        background: rgba(255, 255, 255, 0.05);
        color: #E6D9F2;
        border-radius: 8px 8px 0 0;
        padding: 10px 24px;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(90deg, #FF2882, #3D195B) !important;
        color: white !important;
    }
    div[data-testid="stDataFrame"] { background: rgba(255,255,255,0.04); border-radius: 12px; }
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


def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()


@st.cache_data(ttl=600)
def load_player_rankings() -> pd.DataFrame:
    return run_query(f"""
        SELECT first_name, second_name, web_name, photo, position, team_id, team_code,
               cost_millions, avg_points_recent, points_per_million,
               avg_upcoming_difficulty, recommendation_score
        FROM {CATALOG}.gold.player_recommendation_scores
    """)


@st.cache_data(ttl=600)
def load_optimal_team() -> pd.DataFrame:
    # TODO: suppose que gold.optimal_team contient désormais photo et team_code
    # (non vrai tant que team_optimization.py n'a pas été mis à jour, voir message)
    return run_query(f"""
        SELECT web_name, photo, position, team_code, cost_millions,
               recommendation_score, is_starter
        FROM {CATALOG}.gold.optimal_team
    """)


@st.cache_data(ttl=600)
def load_current_gameweek() -> dict | None:
    # TODO: suppose que gold.current_gameweek existe (non vrai tant que le Bronze
    # et le Gold n'ont pas été étendus pour la persister, voir message)
    df = run_query(f"SELECT * FROM {CATALOG}.gold.current_gameweek")
    return df.iloc[0].to_dict() if not df.empty else None


def player_photo_url(photo: str) -> str:
    photo_id = str(photo).replace(".jpg", "")
    return f"https://resources.premierleague.com/premierleague/photos/players/110x140/p{photo_id}.png"


def team_badge_url(code: int) -> str:
    return f"https://resources.premierleague.com/premierleague/badges/50/t{code}.png"


PL_LOGO_URL = "assets/pl_logo.png"  # déposé localement, voir message précédent

# --- Header ---
st.markdown(
    f"""
    <div class="top-header">
        <img src="{PL_LOGO_URL}">
        <h1>FPL Intelligence</h1>
    </div>
    """,
    unsafe_allow_html=True,
)

# --- Navigation ---
page = st.sidebar.radio(
    "Navigation",
    ["🏠 Accueil", "📊 Classement des joueurs"],
    label_visibility="collapsed",
)

if st.sidebar.button("🔄 Rafraîchir les données"):
    st.cache_data.clear()

# --- Chargement commun ---
try:
    rankings_df = load_player_rankings()
except Exception as e:  # noqa: BLE001 — frontière UI, on veut toujours afficher un message plutôt que planter
    st.error(f"Erreur de connexion aux données : {e}")
    st.stop()

if rankings_df.empty:
    st.warning("Aucune donnée disponible — vérifie que le pipeline Gold a bien tourné.")
    st.stop()

rankings_df["full_name"] = rankings_df["first_name"] + " " + rankings_df["second_name"]

POSITION_LABELS = {
    "GKP": "🧤 Gardiens",
    "DEF": "🛡️ Défenseurs",
    "MID": "⚡ Milieux",
    "FWD": "🎯 Attaquants",
}

# =========================================================
# PAGE ACCUEIL
# =========================================================
if page == "🏠 Accueil":
    gw = load_current_gameweek()
    if gw:
        st.markdown(
            f'<div class="gw-badge">📅 {gw["name"]}</div>', unsafe_allow_html=True
        )
    else:
        st.markdown(
            '<div class="gw-badge">📅 Gameweek — données non disponibles</div>',
            unsafe_allow_html=True,
        )

    st.subheader("Équipe type de la semaine")

    try:
        optimal_df = load_optimal_team()
    except Exception as e:  # noqa: BLE001
        st.error(f"Impossible de charger l'équipe type : {e}")
        st.stop()

    starters = optimal_df[optimal_df["is_starter"]]

    for position in ["GKP", "DEF", "MID", "FWD"]:
        row_players = starters[starters["position"] == position]
        if row_players.empty:
            continue
        cols = st.columns(len(row_players))
        for col, (_, player) in zip(cols, row_players.iterrows()):
            with col:
                st.markdown(
                    f"""
                    <div class="player-card">
                        <img src="{player_photo_url(player["photo"])}">
                        <div class="name">{player["web_name"]}</div>
                        <div class="pos">{player["position"]}</div>
                        <div class="price">{player["cost_millions"]:.1f}M</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    col1.metric(
        "Coût total de l'équipe", f"{starters['cost_millions'].sum():.1f}M / 100.0M"
    )
    col2.metric(
        "Score total (titulaires)", f"{starters['recommendation_score'].sum():.3f}"
    )

# =========================================================
# PAGE CLASSEMENT
# =========================================================
else:
    st.subheader("Classement par poste")

    tabs = st.tabs(list(POSITION_LABELS.values()))

    for tab, (position, label) in zip(tabs, POSITION_LABELS.items()):
        with tab:
            ranked = (
                rankings_df[rankings_df["position"] == position]
                .sort_values("recommendation_score", ascending=False)
                .reset_index(drop=True)
            )
            if ranked.empty:
                st.info(f"Aucun joueur disponible à ce poste ({label}).")
                continue

            for _, player in ranked.head(20).iterrows():
                c1, c2, c3, c4, c5, c6 = st.columns([0.6, 2, 1, 1, 1, 1])
                with c1:
                    st.image(player_photo_url(player["photo"]), width=40)
                with c2:
                    st.write(f"**{player['full_name']}**")
                with c3:
                    st.write(f"{player['cost_millions']:.1f}M")
                with c4:
                    st.write(f"Forme: {player['avg_points_recent']:.1f}")
                with c5:
                    st.write(f"Pts/M: {player['points_per_million']:.2f}")
                with c6:
                    st.write(f"Calendrier: {player['avg_upcoming_difficulty']:.1f}/5")

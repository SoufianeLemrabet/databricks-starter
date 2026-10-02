# src/fpl_pipeline/silver_transforms.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# --- Référentiel des postes (statique, ne vient d'aucun endpoint dédié en Bronze) ---
ELEMENT_TYPE_MAP = {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}


def transform_silver_teams(bronze_teams: DataFrame) -> DataFrame:
    """Nettoie bronze_teams_snapshot. Inclut code : identifiant stable utilisé pour
    construire l'URL du badge officiel (resources.premierleague.com/.../t{code}.png)."""
    return bronze_teams.select(
        F.col("id").alias("team_id"),
        F.col("code").alias("team_code"),
        F.col("name").alias("team_name"),
        F.col("short_name"),
        F.col("strength").cast("int"),
        F.col("strength_overall_home").cast("int"),
        F.col("strength_overall_away").cast("int"),
        F.col("strength_attack_home").cast("int"),
        F.col("strength_attack_away").cast("int"),
        F.col("strength_defence_home").cast("int"),
        F.col("strength_defence_away").cast("int"),
    )


def transform_silver_players(
    bronze_players: DataFrame, silver_teams: DataFrame
) -> DataFrame:
    """Résout le poste et le nom d'équipe. Inclut photo : identifiant utilisé pour
    construire l'URL du portrait officiel du joueur."""
    element_type_mapping = F.create_map(
        *[F.lit(x) for pair in ELEMENT_TYPE_MAP.items() for x in pair]
    )

    players = bronze_players.select(
        F.col("id").alias("player_id"),
        F.col("web_name"),
        F.col("first_name"),
        F.col("second_name"),
        F.col("photo"),
        F.col("team").alias("team_id"),
        element_type_mapping[F.col("element_type")].alias("position"),
        (F.col("now_cost").cast(DoubleType()) / 10).alias("cost_millions"),
        F.col("total_points").cast("int"),
        F.col("form").cast(DoubleType()),
        (F.col("selected_by_percent").cast(DoubleType())).alias("selected_by_percent"),
        F.col("status"),
        F.col("chance_of_playing_next_round").cast("int"),
    )

    return players.join(
        silver_teams.select("team_id", "team_name", "team_code"),
        on="team_id",
        how="left",
    )


def transform_silver_fixtures(
    bronze_fixtures: DataFrame, silver_teams: DataFrame
) -> DataFrame:
    """Résout les noms d'équipes home/away, parse kickoff_time."""
    fixtures = bronze_fixtures.select(
        F.col("id").alias("fixture_id"),
        F.col("event").alias("gameweek_id"),
        F.col("team_h").alias("team_h_id"),
        F.col("team_a").alias("team_a_id"),
        F.col("team_h_difficulty").cast("int"),
        F.col("team_a_difficulty").cast("int"),
        F.col("team_h_score").cast("int"),
        F.col("team_a_score").cast("int"),
        F.col("finished").cast("boolean"),
        F.to_timestamp(F.col("kickoff_time")).alias("kickoff_time"),
    )

    teams_h = silver_teams.select(
        F.col("team_id").alias("team_h_id"), F.col("team_name").alias("team_h_name")
    )
    teams_a = silver_teams.select(
        F.col("team_id").alias("team_a_id"), F.col("team_name").alias("team_a_name")
    )

    return fixtures.join(teams_h, on="team_h_id", how="left").join(
        teams_a, on="team_a_id", how="left"
    )


def transform_silver_player_gameweek_stats(
    bronze_history: DataFrame,
    silver_players: DataFrame,
    silver_fixtures: DataFrame,
) -> DataFrame:
    """
    Table de faits : 1 ligne = 1 joueur x 1 fixture (pas 1 joueur x 1 gameweek,
    à cause des double gameweeks où un joueur a 2 lignes pour le même round).
    """
    history = bronze_history.select(
        F.col("player_id"),
        F.col("fixture").alias("fixture_id"),
        F.col("round").alias("gameweek_id"),
        F.col("opponent_team").alias("opponent_team_id"),
        F.col("was_home").cast("boolean"),
        F.col("total_points").cast("int"),
        F.col("minutes").cast("int"),
        F.col("goals_scored").cast("int"),
        F.col("assists").cast("int"),
        F.col("clean_sheets").cast("int"),
        F.col("goals_conceded").cast("int"),
        F.col("yellow_cards").cast("int"),
        F.col("red_cards").cast("int"),
        F.col("bonus").cast("int"),
        F.col("bps").cast("int"),
        F.col("expected_goals").cast(DoubleType()),
        F.col("expected_assists").cast(DoubleType()),
        (F.col("value").cast(DoubleType()) / 10).alias("cost_at_match_millions"),
    )

    players_dim = silver_players.select("player_id", "web_name", "position", "team_id")

    fixtures_dim = silver_fixtures.select(
        "fixture_id",
        F.col("team_h_difficulty"),
        F.col("team_a_difficulty"),
    )

    stats = history.join(players_dim, on="player_id", how="left")
    stats = stats.join(fixtures_dim, on="fixture_id", how="left")

    return stats.withColumn(
        "difficulty",
        F.when(F.col("was_home"), F.col("team_h_difficulty")).otherwise(
            F.col("team_a_difficulty")
        ),
    ).drop("team_h_difficulty", "team_a_difficulty")

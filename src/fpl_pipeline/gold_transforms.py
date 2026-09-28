# src/fpl_pipeline/gold_transforms.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- Pondération du score composite, fixe et documentée (pas configurable, hors scope V1) ---
# form   : 40% — indicateur le plus prédictif à court terme
# value  : 30% — points par million, favorise les bons rapports qualité/prix
# fixture: 30% — inverse de la difficulté des prochains matchs (calendrier favorable = bonus)
WEIGHT_FORM = 0.4
WEIGHT_VALUE = 0.3
WEIGHT_FIXTURE = 0.3
ROLLING_WINDOW_MATCHES = (
    3  # nombre de DERNIERS MATCHS (pas gameweeks, cf. double gameweeks)
)
UPCOMING_FIXTURES_WINDOW = (
    3  # nombre de prochains matchs pris en compte pour la difficulté
)


def transform_gold_player_form(silver_stats: DataFrame) -> DataFrame:
    """
    Rolling average sur les N DERNIERS MATCHS JOUÉS par joueur (pas les N dernières
    gameweeks) : un joueur avec une double gameweek récente a 2 matchs dans la fenêtre,
    ce qui reflète mieux sa forme réelle que si on comptait par gameweek.
    """
    window = (
        Window.partitionBy("player_id")
        .orderBy("gameweek_id")
        .rowsBetween(-(ROLLING_WINDOW_MATCHES - 1), 0)
    )

    return (
        silver_stats.withColumn("avg_points_recent", F.avg("total_points").over(window))
        .withColumn("avg_minutes_recent", F.avg("minutes").over(window))
        .withColumn("avg_expected_goals_recent", F.avg("expected_goals").over(window))
        .withColumn(
            "avg_expected_assists_recent", F.avg("expected_assists").over(window)
        )
        .withColumn("matches_in_window", F.count("total_points").over(window))
        .groupBy("player_id")
        .agg(
            F.last("avg_points_recent").alias("avg_points_recent"),
            F.last("avg_minutes_recent").alias("avg_minutes_recent"),
            F.last("avg_expected_goals_recent").alias("avg_expected_goals_recent"),
            F.last("avg_expected_assists_recent").alias("avg_expected_assists_recent"),
            F.last("matches_in_window").alias("matches_in_window"),
        )
    )


def transform_gold_player_value(silver_players: DataFrame) -> DataFrame:
    """Points par million et ownership, à l'état courant (silver_players n'a pas d'historique)."""
    return silver_players.select(
        "player_id",
        "web_name",
        "position",
        "team_name",
        "cost_millions",
        "total_points",
        "selected_by_percent",
        F.when(
            F.col("cost_millions") > 0, F.col("total_points") / F.col("cost_millions")
        )
        .otherwise(F.lit(0.0))
        .alias("points_per_million"),
    )


def transform_gold_upcoming_fixtures_difficulty(
    silver_fixtures: DataFrame,
) -> DataFrame:
    """
    Difficulté moyenne des N prochains matchs par équipe, séparé en 2 lignes home/away
    par fixture puis agrégé — un fixture a une difficulté différente pour chaque équipe.
    Ne garde que les matchs non joués (finished = False).
    """
    upcoming = silver_fixtures.filter(~F.col("finished"))

    home_side = upcoming.select(
        F.col("team_h_id").alias("team_id"),
        F.col("kickoff_time"),
        F.col("team_h_difficulty").alias("difficulty"),
    )
    away_side = upcoming.select(
        F.col("team_a_id").alias("team_id"),
        F.col("kickoff_time"),
        F.col("team_a_difficulty").alias("difficulty"),
    )
    all_upcoming = home_side.union(away_side)

    window = Window.partitionBy("team_id").orderBy("kickoff_time")
    ranked = all_upcoming.withColumn("match_rank", F.row_number().over(window))

    return (
        ranked.filter(F.col("match_rank") <= UPCOMING_FIXTURES_WINDOW)
        .groupBy("team_id")
        .agg(
            F.avg("difficulty").alias("avg_upcoming_difficulty"),
            F.count("difficulty").alias("upcoming_fixtures_count"),
        )
    )


def _min_max_normalize(df: DataFrame, columns: list[str]) -> DataFrame:
    """
    Normalise plusieurs colonnes en 0-1 (min-max) en UN SEUL passage Spark :
    un unique .collect() calcule tous les min/max d'un coup, plutôt qu'un
    .collect() par colonne. Ajoute une colonne "<col>_norm" par colonne fournie.
    Si min == max (pas assez de variance) ou valeurs nulles, la colonne
    normalisée vaut 0.5 pour ne pas fausser le score par une division par zéro.
    """
    agg_exprs = []
    for c in columns:
        agg_exprs.append(F.min(c).alias(f"{c}__min"))
        agg_exprs.append(F.max(c).alias(f"{c}__max"))

    stats = df.agg(*agg_exprs).collect()[0]

    result = df
    for c in columns:
        min_v = stats[f"{c}__min"]
        max_v = stats[f"{c}__max"]
        if min_v is None or max_v is None or min_v == max_v:
            result = result.withColumn(f"{c}_norm", F.lit(0.5))
        else:
            result = result.withColumn(
                f"{c}_norm", (F.col(c) - F.lit(min_v)) / (F.lit(max_v) - F.lit(min_v))
            )
    return result


def transform_gold_player_recommendation_scores(
    player_form: DataFrame,
    player_value: DataFrame,
    upcoming_difficulty: DataFrame,
    silver_players: DataFrame,
) -> DataFrame:
    """
    Score composite : forme (40%) + value (30%) + calendrier favorable (30%).
    avg_points_recent et points_per_million sont sur des échelles très différentes
    (points bruts vs points/million), donc les deux sont normalisés 0-1 (min-max)
    avant pondération, sinon la composante à plus grande échelle domine le score.
    La difficulté (1-5, plus bas = plus favorable) est inversée puis ramenée sur 0-1
    directement (pas besoin de min-max, l'échelle est déjà connue et bornée).
    Filtre les joueurs indisponibles (status != 'a') avant de produire le classement.
    """
    base = (
        silver_players.select("player_id", "web_name", "position", "team_id", "status")
        .join(player_form, on="player_id", how="left")
        .join(
            player_value.select("player_id", "points_per_million"),
            on="player_id",
            how="left",
        )
        .join(upcoming_difficulty, on="team_id", how="left")
    )

    base = base.withColumn(
        "fixture_favorability_norm",
        (F.lit(5) - F.coalesce(F.col("avg_upcoming_difficulty"), F.lit(3))) / F.lit(4),
        # difficulté 1 (facile) -> favorability 1.0 ; difficulté 5 (dur) -> favorability 0.0
    )

    scored = _min_max_normalize(base, ["avg_points_recent", "points_per_million"])

    return (
        scored.withColumn(
            "recommendation_score",
            F.coalesce(F.col("avg_points_recent_norm"), F.lit(0.0)) * WEIGHT_FORM
            + F.coalesce(F.col("points_per_million_norm"), F.lit(0.0)) * WEIGHT_VALUE
            + F.coalesce(F.col("fixture_favorability_norm"), F.lit(0.0))
            * WEIGHT_FIXTURE,
        )
        .filter(F.col("status") == "a")
        .select(
            "player_id",
            "web_name",
            "position",
            "team_id",
            "avg_points_recent",
            "points_per_million",
            "avg_upcoming_difficulty",
            "recommendation_score",
        )
        .orderBy(F.desc("recommendation_score"))
    )

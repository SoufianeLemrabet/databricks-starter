# tests/test_gold_transforms.py
import pytest

from fpl_pipeline.gold_transforms import (
    _min_max_normalize,
    transform_gold_player_form,
    transform_gold_player_recommendation_scores,
    transform_gold_player_value,
    transform_gold_upcoming_fixtures_difficulty,
)

# --- transform_gold_player_form ---


def test_gold_player_form_rolling_average_on_last_3_matches(spark):
    """5 matchs pour un joueur : la moyenne doit porter sur les 3 DERNIERS, pas tous."""
    silver_stats = spark.createDataFrame(
        [
            {
                "player_id": 1,
                "gameweek_id": 1,
                "total_points": 2,
                "minutes": 90,
                "expected_goals": 0.1,
                "expected_assists": 0.0,
            },
            {
                "player_id": 1,
                "gameweek_id": 2,
                "total_points": 2,
                "minutes": 90,
                "expected_goals": 0.1,
                "expected_assists": 0.0,
            },
            {
                "player_id": 1,
                "gameweek_id": 3,
                "total_points": 10,
                "minutes": 90,
                "expected_goals": 0.8,
                "expected_assists": 0.2,
            },
            {
                "player_id": 1,
                "gameweek_id": 4,
                "total_points": 8,
                "minutes": 90,
                "expected_goals": 0.5,
                "expected_assists": 0.1,
            },
            {
                "player_id": 1,
                "gameweek_id": 5,
                "total_points": 6,
                "minutes": 90,
                "expected_goals": 0.3,
                "expected_assists": 0.1,
            },
        ]
    )

    result = transform_gold_player_form(silver_stats).collect()

    assert len(result) == 1  # une ligne par joueur (groupBy)
    # moyenne des 3 derniers matchs (gw 3, 4, 5) : (10 + 8 + 6) / 3 = 8.0
    assert result[0]["avg_points_recent"] == 8.0
    assert result[0]["matches_in_window"] == 3


def test_gold_player_form_with_fewer_than_window_matches(spark):
    """Un joueur avec seulement 2 matchs (moins que la fenêtre de 3) : moyenne sur ce qu'il a."""
    silver_stats = spark.createDataFrame(
        [
            {
                "player_id": 1,
                "gameweek_id": 1,
                "total_points": 4,
                "minutes": 90,
                "expected_goals": 0.2,
                "expected_assists": 0.0,
            },
            {
                "player_id": 1,
                "gameweek_id": 2,
                "total_points": 6,
                "minutes": 90,
                "expected_goals": 0.4,
                "expected_assists": 0.1,
            },
        ]
    )

    result = transform_gold_player_form(silver_stats).collect()

    assert result[0]["matches_in_window"] == 2
    assert result[0]["avg_points_recent"] == 5.0


def test_gold_player_form_double_gameweek_counts_as_two_matches(spark):
    """2 lignes sur le même gameweek_id (double gameweek) : comptent comme 2 matchs distincts
    dans la fenêtre, pas comme 1 gameweek dédupliquée."""
    silver_stats = spark.createDataFrame(
        [
            {
                "player_id": 1,
                "gameweek_id": 1,
                "total_points": 2,
                "minutes": 90,
                "expected_goals": 0.1,
                "expected_assists": 0.0,
            },
            {
                "player_id": 1,
                "gameweek_id": 2,
                "total_points": 8,
                "minutes": 90,
                "expected_goals": 0.5,
                "expected_assists": 0.1,
            },
            {
                "player_id": 1,
                "gameweek_id": 2,
                "total_points": 4,
                "minutes": 60,
                "expected_goals": 0.2,
                "expected_assists": 0.0,
            },
        ]
    )

    result = transform_gold_player_form(silver_stats).collect()

    assert result[0]["matches_in_window"] == 3
    # moyenne des 3 matchs (2, 8, 4) puisque la fenêtre est de 3 et qu'il n'y en a que 3
    assert result[0]["avg_points_recent"] == pytest.approx(4.666666, abs=0.01)


def test_gold_player_form_separates_multiple_players(spark):
    silver_stats = spark.createDataFrame(
        [
            {
                "player_id": 1,
                "gameweek_id": 1,
                "total_points": 10,
                "minutes": 90,
                "expected_goals": 0.5,
                "expected_assists": 0.1,
            },
            {
                "player_id": 2,
                "gameweek_id": 1,
                "total_points": 2,
                "minutes": 90,
                "expected_goals": 0.1,
                "expected_assists": 0.0,
            },
        ]
    )

    result = transform_gold_player_form(silver_stats).collect()
    scores = {row["player_id"]: row["avg_points_recent"] for row in result}

    assert scores == {1: 10.0, 2: 2.0}


# --- transform_gold_player_value ---


def test_gold_player_value_computes_points_per_million(spark):
    silver_players = spark.createDataFrame(
        [
            {
                "player_id": 1,
                "web_name": "Salah",
                "position": "MID",
                "team_name": "Liverpool",
                "cost_millions": 12.5,
                "total_points": 100,
                "selected_by_percent": 45.0,
            },
        ]
    )

    result = transform_gold_player_value(silver_players).collect()

    assert result[0]["points_per_million"] == 8.0


def test_gold_player_value_zero_cost_does_not_divide_by_zero(spark):
    """Cas limite improbable mais protégé dans le code : cost_millions à 0."""
    silver_players = spark.createDataFrame(
        [
            {
                "player_id": 1,
                "web_name": "Test",
                "position": "GKP",
                "team_name": "Test FC",
                "cost_millions": 0.0,
                "total_points": 10,
                "selected_by_percent": 1.0,
            },
        ]
    )

    result = transform_gold_player_value(silver_players).collect()

    assert result[0]["points_per_million"] == 0.0  # pas de crash, valeur par défaut


# --- transform_gold_upcoming_fixtures_difficulty ---


def test_gold_upcoming_fixtures_difficulty_ignores_finished_matches(spark):
    silver_fixtures = spark.createDataFrame(
        [
            {
                "team_h_id": 1,
                "team_a_id": 2,
                "team_h_difficulty": 5,
                "team_a_difficulty": 1,
                "finished": True,
                "kickoff_time": "2026-01-01T14:00:00Z",
            },
            {
                "team_h_id": 1,
                "team_a_id": 3,
                "team_h_difficulty": 2,
                "team_a_difficulty": 4,
                "finished": False,
                "kickoff_time": "2026-02-01T14:00:00Z",
            },
        ]
    )

    result = transform_gold_upcoming_fixtures_difficulty(silver_fixtures).collect()
    team_1_row = next(r for r in result if r["team_id"] == 1)
    # seul le fixture non fini (difficulty 2 pour l'équipe 1 à domicile) doit compter
    assert team_1_row["avg_upcoming_difficulty"] == 2.0
    assert team_1_row["upcoming_fixtures_count"] == 1


def test_gold_upcoming_fixtures_difficulty_limits_to_window_size(spark):
    """4 prochains matchs pour une équipe, mais UPCOMING_FIXTURES_WINDOW=3 : seuls les 3 plus proches comptent."""
    silver_fixtures = spark.createDataFrame(
        [
            {
                "team_h_id": 1,
                "team_a_id": 9,
                "team_h_difficulty": 1,
                "team_a_difficulty": 3,
                "finished": False,
                "kickoff_time": "2026-01-01T14:00:00Z",
            },
            {
                "team_h_id": 1,
                "team_a_id": 9,
                "team_h_difficulty": 2,
                "team_a_difficulty": 3,
                "finished": False,
                "kickoff_time": "2026-01-08T14:00:00Z",
            },
            {
                "team_h_id": 1,
                "team_a_id": 9,
                "team_h_difficulty": 3,
                "team_a_difficulty": 3,
                "finished": False,
                "kickoff_time": "2026-01-15T14:00:00Z",
            },
            {
                "team_h_id": 1,
                "team_a_id": 9,
                "team_h_difficulty": 5,
                "team_a_difficulty": 3,
                "finished": False,
                "kickoff_time": "2026-01-22T14:00:00Z",
            },
        ]
    )

    result = transform_gold_upcoming_fixtures_difficulty(silver_fixtures).collect()
    team_1_row = next(r for r in result if r["team_id"] == 1)

    # moyenne des 3 premiers seulement (1, 2, 3), le 4e (difficulty=5) est exclu
    assert team_1_row["avg_upcoming_difficulty"] == 2.0
    assert team_1_row["upcoming_fixtures_count"] == 3


def test_gold_upcoming_fixtures_difficulty_home_and_away_are_separate(spark):
    """Un même fixture donne une difficulté différente pour l'équipe home et l'équipe away."""
    silver_fixtures = spark.createDataFrame(
        [
            {
                "team_h_id": 1,
                "team_a_id": 2,
                "team_h_difficulty": 5,
                "team_a_difficulty": 1,
                "finished": False,
                "kickoff_time": "2026-01-01T14:00:00Z",
            },
        ]
    )

    result = transform_gold_upcoming_fixtures_difficulty(silver_fixtures).collect()
    difficulties = {r["team_id"]: r["avg_upcoming_difficulty"] for r in result}

    assert difficulties[1] == 5.0  # équipe 1 à domicile, difficulté dure
    assert difficulties[2] == 1.0  # équipe 2 à l'extérieur, difficulté facile


# --- _min_max_normalize (helper interne, testé isolément) ---


def test_min_max_normalize_scales_to_0_1(spark):
    df = spark.createDataFrame(
        [{"id": 1, "score": 0}, {"id": 2, "score": 5}, {"id": 3, "score": 10}]
    )

    result = _min_max_normalize(df, ["score"]).collect()
    normalized = {r["id"]: r["score_norm"] for r in result}

    assert normalized[1] == 0.0
    assert normalized[2] == 0.5
    assert normalized[3] == 1.0


def test_min_max_normalize_handles_constant_column(spark):
    """Si min == max (aucune variance), la normalisation doit retourner 0.5 partout, pas planter."""
    df = spark.createDataFrame([{"id": 1, "score": 7}, {"id": 2, "score": 7}])

    result = _min_max_normalize(df, ["score"]).collect()

    assert all(r["score_norm"] == 0.5 for r in result)


def test_min_max_normalize_handles_multiple_columns_in_one_pass(spark):
    df = spark.createDataFrame(
        [{"id": 1, "a": 0, "b": 100}, {"id": 2, "a": 10, "b": 200}]
    )

    result = _min_max_normalize(df, ["a", "b"]).collect()
    row1 = next(r for r in result if r["id"] == 1)
    row2 = next(r for r in result if r["id"] == 2)

    assert row1["a_norm"] == 0.0
    assert row2["a_norm"] == 1.0
    assert row1["b_norm"] == 0.0
    assert row2["b_norm"] == 1.0


# --- transform_gold_player_recommendation_scores ---


def test_recommendation_scores_best_player_ranks_first(spark):
    player_form = spark.createDataFrame(
        [
            {"player_id": 1, "avg_points_recent": 8.0},
            {"player_id": 2, "avg_points_recent": 2.0},
        ]
    )
    player_value = spark.createDataFrame(
        [
            {"player_id": 1, "points_per_million": 8.0, "cost_millions": 10.0},
            {"player_id": 2, "points_per_million": 1.0, "cost_millions": 5.0},
        ]
    )
    upcoming_difficulty = spark.createDataFrame(
        [
            {"team_id": 1, "avg_upcoming_difficulty": 2.0},  # calendrier facile
            {"team_id": 2, "avg_upcoming_difficulty": 5.0},  # calendrier dur
        ]
    )
    silver_players = spark.createDataFrame(
        [
            {
                "player_id": 1,
                "web_name": "Best",
                "position": "MID",
                "team_id": 1,
                "status": "a",
                "first_name": "KSE",
                "second_name": "Bob",
            },
            {
                "player_id": 2,
                "web_name": "Worst",
                "position": "MID",
                "team_id": 2,
                "status": "a",
                "first_name": "KS",
                "second_name": "Olise",
            },
        ]
    )

    result = transform_gold_player_recommendation_scores(
        player_form, player_value, upcoming_difficulty, silver_players
    ).collect()

    assert result[0]["player_id"] == 1  # meilleur sur tout -> premier du classement
    assert result[0]["recommendation_score"] > result[1]["recommendation_score"]


def test_recommendation_scores_filters_unavailable_players(spark):
    player_form = spark.createDataFrame([{"player_id": 1, "avg_points_recent": 10.0}])
    player_value = spark.createDataFrame(
        [{"player_id": 1, "points_per_million": 10.0, "cost_millions": 10.0}]
    )
    upcoming_difficulty = spark.createDataFrame(
        [{"team_id": 1, "avg_upcoming_difficulty": 1.0}]
    )
    silver_players = spark.createDataFrame(
        [
            {
                "player_id": 1,
                "web_name": "Salah",
                "first_name": "Mohamed",
                "second_name": "Salah",
                "position": "MID",
                "team_id": 1,
                "status": "i",
            }
        ]
    )

    result = transform_gold_player_recommendation_scores(
        player_form, player_value, upcoming_difficulty, silver_players
    ).collect()

    assert len(result) == 0


def test_recommendation_scores_handles_missing_upcoming_difficulty(spark):
    player_form = spark.createDataFrame([{"player_id": 1, "avg_points_recent": 5.0}])
    player_value = spark.createDataFrame(
        [{"player_id": 1, "points_per_million": 5.0, "cost_millions": 7.0}]
    )
    upcoming_difficulty = spark.createDataFrame(
        [
            {"team_id": 999, "avg_upcoming_difficulty": 1.0}
        ]  # team_id différent, pas de match pour team 1
    )
    silver_players = spark.createDataFrame(
        [
            {
                "player_id": 1,
                "web_name": "Salah",
                "first_name": "Mohamed",
                "second_name": "Salah",
                "position": "MID",
                "team_id": 1,
                "status": "a",
            }
        ]
    )

    result = transform_gold_player_recommendation_scores(
        player_form, player_value, upcoming_difficulty, silver_players
    ).collect()

    assert len(result) == 1
    assert result[0]["recommendation_score"] is not None  # pas de null qui remonte

# tests/test_silver_transforms.py
from fpl_pipeline.silver_transforms import (
    transform_silver_fixtures,
    transform_silver_player_gameweek_stats,
    transform_silver_players,
    transform_silver_teams,
)

# --- transform_silver_teams ---


def test_transform_silver_teams_selects_and_renames_columns(spark):
    bronze_teams = spark.createDataFrame(
        [
            {
                "id": 1,
                "name": "Arsenal",
                "short_name": "ARS",
                "strength": 4,
                "strength_overall_home": 1300,
                "strength_overall_away": 1250,
                "strength_attack_home": 1300,
                "strength_attack_away": 1250,
                "strength_defence_home": 1300,
                "strength_defence_away": 1250,
                "played": 0,  # doit être ignoré
                "position": 0,  # doit être ignoré
            }
        ]
    )

    result = transform_silver_teams(bronze_teams).collect()

    assert len(result) == 1
    row = result[0]
    assert row["team_id"] == 1
    assert row["team_name"] == "Arsenal"
    assert row["short_name"] == "ARS"
    assert "played" not in result[0].asDict()
    assert "position" not in result[0].asDict()


# --- transform_silver_players ---


def test_transform_silver_players_resolves_position_and_team_name(spark):
    bronze_players = spark.createDataFrame(
        [
            {
                "id": 1,
                "web_name": "Salah",
                "first_name": "Mohamed",
                "second_name": "Salah",
                "team": 11,
                "element_type": 3,
                "now_cost": 125,
                "total_points": 80,
                "form": "5.5",
                "selected_by_percent": "45.2",
                "status": "a",
                "chance_of_playing_next_round": 100,
            }
        ]
    )
    silver_teams = spark.createDataFrame([{"team_id": 11, "team_name": "Liverpool"}])

    result = transform_silver_players(bronze_players, silver_teams).collect()

    assert len(result) == 1
    row = result[0]
    assert row["position"] == "MID"
    assert row["team_name"] == "Liverpool"
    assert row["cost_millions"] == 12.5


def test_transform_silver_players_handles_all_positions(spark):
    bronze_players = spark.createDataFrame(
        [
            {
                "id": i,
                "web_name": f"p{i}",
                "first_name": "x",
                "second_name": "y",
                "team": 1,
                "element_type": i,
                "now_cost": 50,
                "total_points": 0,
                "form": "0.0",
                "selected_by_percent": "0.0",
                "status": "a",
                "chance_of_playing_next_round": 100,
            }
            for i in range(1, 5)
        ]
    )
    silver_teams = spark.createDataFrame([{"team_id": 1, "team_name": "Test FC"}])

    result = transform_silver_players(bronze_players, silver_teams).collect()
    positions = {row["player_id"]: row["position"] for row in result}

    assert positions == {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}


def test_transform_silver_players_left_join_keeps_player_without_matching_team(spark):
    bronze_players = spark.createDataFrame(
        [
            {
                "id": 1,
                "web_name": "Orphan",
                "first_name": "x",
                "second_name": "y",
                "team": 999,
                "element_type": 1,
                "now_cost": 40,
                "total_points": 0,
                "form": "0.0",
                "selected_by_percent": "0.0",
                "status": "a",
                "chance_of_playing_next_round": 100,
            }
        ]
    )
    silver_teams = spark.createDataFrame([{"team_id": 1, "team_name": "Test FC"}])

    result = transform_silver_players(bronze_players, silver_teams).collect()

    assert len(result) == 1
    assert (
        result[0]["team_name"] is None
    )  # pas d'équipe correspondante -> left join, pas de perte de ligne


# --- transform_silver_fixtures ---


def test_transform_silver_fixtures_resolves_team_names(spark):
    bronze_fixtures = spark.createDataFrame(
        [
            {
                "id": 1,
                "event": 5,
                "team_h": 1,
                "team_a": 2,
                "team_h_difficulty": 3,
                "team_a_difficulty": 4,
                "team_h_score": 2,
                "team_a_score": 1,
                "finished": True,
                "kickoff_time": "2026-09-20T14:00:00Z",
            }
        ]
    )
    silver_teams = spark.createDataFrame(
        [{"team_id": 1, "team_name": "Arsenal"}, {"team_id": 2, "team_name": "Chelsea"}]
    )

    result = transform_silver_fixtures(bronze_fixtures, silver_teams).collect()

    assert len(result) == 1
    row = result[0]
    assert row["team_h_name"] == "Arsenal"
    assert row["team_a_name"] == "Chelsea"
    assert row["kickoff_time"] is not None


# --- transform_silver_player_gameweek_stats ---


def test_transform_silver_player_gameweek_stats_double_gameweek_keeps_both_rows(spark):
    """Un joueur avec 2 fixtures sur le même round (double gameweek) doit donner 2 lignes, pas 1."""
    bronze_history = spark.createDataFrame(
        [
            {
                "player_id": 1,
                "fixture": 101,
                "round": 18,
                "opponent_team": 2,
                "was_home": True,
                "total_points": 8,
                "minutes": 90,
                "goals_scored": 1,
                "assists": 0,
                "clean_sheets": 0,
                "goals_conceded": 1,
                "yellow_cards": 0,
                "red_cards": 0,
                "bonus": 2,
                "bps": 28,
                "expected_goals": "0.45",
                "expected_assists": "0.10",
                "value": 125,
            },
            {
                "player_id": 1,
                "fixture": 102,
                "round": 18,
                "opponent_team": 3,
                "was_home": False,
                "total_points": 2,
                "minutes": 60,
                "goals_scored": 0,
                "assists": 0,
                "clean_sheets": 0,
                "goals_conceded": 2,
                "yellow_cards": 1,
                "red_cards": 0,
                "bonus": 0,
                "bps": 10,
                "expected_goals": "0.05",
                "expected_assists": "0.02",
                "value": 125,
            },
        ]
    )
    silver_players = spark.createDataFrame(
        [{"player_id": 1, "web_name": "Salah", "position": "MID", "team_id": 11}]
    )
    silver_fixtures = spark.createDataFrame(
        [
            {"fixture_id": 101, "team_h_difficulty": 2, "team_a_difficulty": 3},
            {"fixture_id": 102, "team_h_difficulty": 4, "team_a_difficulty": 2},
        ]
    )

    result = transform_silver_player_gameweek_stats(
        bronze_history, silver_players, silver_fixtures
    ).collect()

    assert len(result) == 2  # pas déduplication sur round, les 2 fixtures sont gardées
    fixture_ids = {row["fixture_id"] for row in result}
    assert fixture_ids == {101, 102}


def test_transform_silver_player_gameweek_stats_difficulty_depends_on_was_home(spark):
    bronze_history = spark.createDataFrame(
        [
            {
                "player_id": 1,
                "fixture": 101,
                "round": 5,
                "opponent_team": 2,
                "was_home": True,
                "total_points": 5,
                "minutes": 90,
                "goals_scored": 0,
                "assists": 1,
                "clean_sheets": 1,
                "goals_conceded": 0,
                "yellow_cards": 0,
                "red_cards": 0,
                "bonus": 1,
                "bps": 20,
                "expected_goals": "0.1",
                "expected_assists": "0.3",
                "value": 60,
            },
        ]
    )
    silver_players = spark.createDataFrame(
        [{"player_id": 1, "web_name": "Test", "position": "DEF", "team_id": 1}]
    )
    silver_fixtures = spark.createDataFrame(
        [{"fixture_id": 101, "team_h_difficulty": 2, "team_a_difficulty": 4}]
    )

    result = transform_silver_player_gameweek_stats(
        bronze_history, silver_players, silver_fixtures
    ).collect()

    # was_home=True -> doit prendre team_h_difficulty (2), pas team_a_difficulty (4)
    assert result[0]["difficulty"] == 2


def test_transform_silver_player_gameweek_stats_expected_goals_cast_to_double(spark):
    bronze_history = spark.createDataFrame(
        [
            {
                "player_id": 1,
                "fixture": 101,
                "round": 5,
                "opponent_team": 2,
                "was_home": True,
                "total_points": 5,
                "minutes": 90,
                "goals_scored": 0,
                "assists": 0,
                "clean_sheets": 0,
                "goals_conceded": 0,
                "yellow_cards": 0,
                "red_cards": 0,
                "bonus": 0,
                "bps": 0,
                "expected_goals": "0.45",
                "expected_assists": "0.12",
                "value": 60,
            },
        ]
    )
    silver_players = spark.createDataFrame(
        [{"player_id": 1, "web_name": "Test", "position": "FWD", "team_id": 1}]
    )
    silver_fixtures = spark.createDataFrame(
        [{"fixture_id": 101, "team_h_difficulty": 2, "team_a_difficulty": 3}]
    )

    result = transform_silver_player_gameweek_stats(
        bronze_history, silver_players, silver_fixtures
    ).collect()

    assert result[0]["expected_goals"] == 0.45
    assert isinstance(result[0]["expected_goals"], float)

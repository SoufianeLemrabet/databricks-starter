# src/fpl_pipeline/team_optimizer.py
from dataclasses import dataclass
import pulp


DEFAULT_BUDGET = 100.0
SQUAD_SIZE = 15
POSITION_LIMITS = {"GKP": 2, "DEF": 5, "MID": 5, "FWD": 3}
MAX_PLAYERS_PER_TEAM = 3

STARTING_XI_SIZE = 11
STARTING_POSITION_BOUNDS = {
    "GKP": (1, 1),
    "DEF": (3, 5),
    "MID": (2, 5),
    "FWD": (1, 3),
}


@dataclass
class PlayerCandidate:
    player_id: int
    web_name: str
    position: str
    team_id: int
    cost_millions: float
    recommendation_score: float


def optimize_squad(
    candidates: list[PlayerCandidate],
    budget: float = DEFAULT_BUDGET,
) -> list[PlayerCandidate]:
    """
    Sélectionne 15 joueurs maximisant la somme des recommendation_score,
    sous contraintes de budget, de postes, et de max 3 par club.
    budget : surcharge le budget par défaut (100.0M), utile pour l'app Streamlit
    où l'utilisateur ajuste ce paramètre.
    """
    prob = pulp.LpProblem("fpl_squad_selection", pulp.LpMaximize)

    player_vars = {
        c.player_id: pulp.LpVariable(f"player_{c.player_id}", cat="Binary")
        for c in candidates
    }

    prob += pulp.lpSum(
        player_vars[c.player_id] * c.recommendation_score for c in candidates
    )

    prob += pulp.lpSum(player_vars.values()) == SQUAD_SIZE

    prob += (
        pulp.lpSum(player_vars[c.player_id] * c.cost_millions for c in candidates)
        <= budget
    )

    for position, limit in POSITION_LIMITS.items():
        prob += (
            pulp.lpSum(
                player_vars[c.player_id] for c in candidates if c.position == position
            )
            == limit
        )

    team_ids = {c.team_id for c in candidates}
    for team_id in team_ids:
        prob += (
            pulp.lpSum(
                player_vars[c.player_id] for c in candidates if c.team_id == team_id
            )
            <= MAX_PLAYERS_PER_TEAM
        )

    status = prob.solve(pulp.PULP_CBC_CMD(msg=False))

    if pulp.LpStatus[status] != "Optimal":
        raise ValueError(
            f"Pas de solution optimale trouvée (statut: {pulp.LpStatus[status]}) — "
            f"vérifie que le budget ({budget}M) et les contraintes sont satisfaisables avec ce pool de joueurs."
        )

    return [c for c in candidates if player_vars[c.player_id].value() == 1]


def select_starting_xi(
    squad: list[PlayerCandidate],
) -> tuple[list[PlayerCandidate], list[PlayerCandidate]]:
    """
    Choisit les 11 titulaires parmi les 15 joueurs du squad, maximisant la somme
    des recommendation_score, sous contrainte d'une formation FPL valide.
    Retourne (titulaires, remplaçants).
    """
    prob = pulp.LpProblem("fpl_starting_xi", pulp.LpMaximize)

    player_vars = {
        c.player_id: pulp.LpVariable(f"starter_{c.player_id}", cat="Binary")
        for c in squad
    }

    prob += pulp.lpSum(player_vars[c.player_id] * c.recommendation_score for c in squad)

    prob += pulp.lpSum(player_vars.values()) == STARTING_XI_SIZE

    for position, (min_count, max_count) in STARTING_POSITION_BOUNDS.items():
        position_sum = pulp.lpSum(
            player_vars[c.player_id] for c in squad if c.position == position
        )
        prob += position_sum >= min_count
        prob += position_sum <= max_count

    status = prob.solve(pulp.PULP_CBC_CMD(msg=False))

    if pulp.LpStatus[status] != "Optimal":
        raise ValueError(
            f"Pas de formation valide trouvée (statut: {pulp.LpStatus[status]}) — "
            "vérifie la répartition par poste du squad de 15."
        )

    starters = [c for c in squad if player_vars[c.player_id].value() == 1]
    bench = [c for c in squad if player_vars[c.player_id].value() == 0]

    return starters, bench


def build_optimal_team(
    candidates: list[PlayerCandidate],
    budget: float = DEFAULT_BUDGET,
) -> dict:
    squad = optimize_squad(candidates, budget=budget)
    starters, bench = select_starting_xi(squad)

    return {
        "squad": squad,
        "starting_xi": starters,
        "bench": bench,
        "total_cost": sum(c.cost_millions for c in squad),
        "total_score": sum(c.recommendation_score for c in starters),
    }

from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential


class FPLAPIError(Exception):
    """Erreur levée lors d'un appel à l'API FPL."""


class FPLClient:
    """Client pour l'API publique Fantasy Premier League."""

    BASE_URL = "https://fantasy.premierleague.com/api"

    def __init__(self, session: requests.Session | None = None, timeout: int = 10):
        """
        Args:
            session: session requests à réutiliser (facilite le mock en test)
            timeout: timeout en secondes pour chaque appel
        """
        self.timeout = timeout
        self.session = session or requests.Session()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _get(self, endpoint: str) -> dict[str, Any]:
        """
        Méthode privée commune à tous les appels GET.
        Gère la construction de l'URL, l'appel HTTP, la gestion des erreurs
        (status code, timeout, retry éventuel), et le parsing JSON.

        Args:
            endpoint: chemin relatif à BASE_URL (ex: "bootstrap-static/")
        """
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
        except requests.exceptions.RequestException:  # noqa: TRY203
            raise
        try:
            return response.json()
        except ValueError as e:
            raise FPLAPIError(f"Réponse non-JSON depuis {url}") from e

    def get_bootstrap(self) -> dict[str, Any]:
        """Récupère bootstrap-static/ : joueurs, équipes, gameweeks."""
        return self._get("bootstrap-static/")

    def get_fixtures(self) -> list[dict[str, Any]]:
        """Récupère fixtures/ : calendrier des matchs."""
        return self._get("fixtures/")

    def get_player_history(self, player_id: int) -> dict[str, Any]:
        """Récupère element-summary/{player_id}/ : historique d'un joueur."""
        return self._get(f"element-summary/{player_id}/")

    def get_gameweek_live(self, gameweek_id: int) -> dict[str, Any]:
        """Récupère event/{gameweek_id}/live/ : stats live d'une gameweek."""
        return self._get(f"event/{gameweek_id}/live/")

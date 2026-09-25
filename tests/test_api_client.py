import pytest
import requests
import requests_mock

from fpl_pipeline.api_client import FPLAPIError, FPLClient

BASE_URL = "https://fantasy.premierleague.com/api"


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Empêche tenacity de vraiment attendre entre les tentatives pendant les tests."""
    monkeypatch.setattr("time.sleep", lambda seconds: None)


@pytest.fixture
def client():
    return FPLClient()


# --- Cas nominal ---


def test_get_bootstrap_success(client):
    payload = {"elements": [{"id": 1, "web_name": "Salah"}]}
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/bootstrap-static/", json=payload, status_code=200)
        result = client.get_bootstrap()
    assert result == payload


def test_get_fixtures_success(client):
    payload = [{"id": 1, "event": 1, "team_h": 1, "team_a": 2}]
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/fixtures/", json=payload, status_code=200)
        result = client.get_fixtures()
    assert result == payload


# --- Construction des URLs ---


def test_get_player_history_calls_correct_url(client):
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/element-summary/123/", json={"history": []}, status_code=200)
        client.get_player_history(123)
    assert m.last_request.url == f"{BASE_URL}/element-summary/123/"


def test_get_gameweek_live_calls_correct_url(client):
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/event/5/live/", json={"elements": []}, status_code=200)
        client.get_gameweek_live(5)
    assert m.last_request.url == f"{BASE_URL}/event/5/live/"


# --- Gestion des erreurs HTTP ---


def test_get_bootstrap_raises_after_persistent_500(client):
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/bootstrap-static/", status_code=500)
        with pytest.raises(
            requests_mock.exceptions.RequestException if False else Exception
        ):
            client.get_bootstrap()
    # 3 tentatives attendues (stop_after_attempt(3))
    assert m.call_count == 3


def test_get_bootstrap_succeeds_after_transient_failures(client):
    with requests_mock.Mocker() as m:
        m.get(
            f"{BASE_URL}/bootstrap-static/",
            [
                {"status_code": 500},
                {"status_code": 500},
                {"json": {"elements": []}, "status_code": 200},
            ],
        )
        result = client.get_bootstrap()
    assert result == {"elements": []}
    assert m.call_count == 3


def test_get_bootstrap_raises_on_404(client):
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/bootstrap-static/", status_code=404)
        with pytest.raises(requests.exceptions.HTTPError):
            client.get_bootstrap()


# --- Erreur de parsing JSON ---


def test_get_bootstrap_raises_fplapierror_on_invalid_json(client):
    with requests_mock.Mocker() as m:
        m.get(
            f"{BASE_URL}/bootstrap-static/",
            text="<html>not json</html>",
            status_code=200,
        )
        with pytest.raises(FPLAPIError, match="Réponse non-JSON"):
            client.get_bootstrap()


# --- Timeout ---


def test_get_bootstrap_raises_on_timeout(client):
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/bootstrap-static/", exc=requests.exceptions.ConnectTimeout)
        with pytest.raises(requests.exceptions.ConnectTimeout):
            client.get_bootstrap()
    assert m.call_count == 3

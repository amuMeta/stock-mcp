from __future__ import annotations
import os
import httpx
import pytest


def _base_url() -> str:
    return os.getenv("STOCK_MCP_BASE_URL", "http://127.0.0.1:9898").rstrip("/")


def _auth_headers() -> dict[str, str]:
    token = os.getenv("STOCK_MCP_BEARER_TOKEN")
    return {"Authorization": f"Bearer {token}"} if token else {}


@pytest.mark.integration
def test_health_endpoint_live():
    with httpx.Client(timeout=10.0, headers=_auth_headers()) as client:
        r = client.get(f"{_base_url()}/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] in ("ok", "degraded")
    assert isinstance(data["components"]["redis"], bool)


@pytest.mark.integration
def test_openapi_contains_core_routes_live():
    with httpx.Client(timeout=15.0, headers=_auth_headers()) as client:
        r = client.get(f"{_base_url()}/openapi.json")
    assert r.status_code == 200
    data = r.json()
    paths = data.get("paths", {})
    assert "/api/v1/market/prices/batch" in paths
    assert "/api/v1/technical/indicators/calculate" in paths
    assert "/api/v1/news/search" in paths


@pytest.mark.integration
def test_market_historical_prices_live():
    # Use a US ticker that is usually supported by Yahoo/Finnhub.
    payload = {"symbol": "NASDAQ:AAPL", "period": "30d", "interval": "1d"}

    with httpx.Client(timeout=30.0, headers=_auth_headers()) as client:
        r = client.post(f"{_base_url()}/api/v1/market/prices/history", json=payload)

    assert r.status_code == 200
    data = r.json()
    assert data["symbol"] == payload["symbol"]
    assert data["period"] == payload["period"]
    assert data["interval"] == payload["interval"]
    assert isinstance(data.get("count"), int)
    assert isinstance(data.get("data"), list)

    # If the upstream provider returns rows, ensure they are dict-like.
    if data["data"]:
        assert isinstance(data["data"][0], dict)

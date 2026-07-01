from __future__ import annotations

import httpx
import pytest

from core.kis_market_handler import MarketHandler


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


def _build_handler() -> MarketHandler:
    handler = MarketHandler.__new__(MarketHandler)
    handler.base_url = "https://openapi.koreainvestment.com:9443"
    handler.app_key = "app-key"
    handler.app_secret = "app-secret"
    handler.credential_profile = "live"
    handler.is_simulation = False
    handler.account_number = "12345678"
    handler.account_product_code = "03"
    handler.acc_no_prefix = "12345678"
    handler.acc_no_postfix = "03"
    handler.get_valid_token = lambda: "token"
    handler.force_refresh_token = lambda: "token-refresh"
    return handler


def test_create_futureoption_buy_order_posts_expected_payload(monkeypatch):
    handler = _build_handler()
    captured = {}

    def fake_hashkey(payload):
        captured["hashkey_payload"] = payload
        return "hash-123"

    def fake_request(method, url, headers, params=None, json=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = headers
        captured["params"] = params
        captured["json"] = json
        captured["timeout"] = timeout
        return _FakeResponse({"rt_cd": "0", "msg1": "OK"})

    monkeypatch.setattr(handler, "_issue_hashkey", fake_hashkey)
    monkeypatch.setattr(httpx, "request", fake_request)

    result = handler.create_futureoption_buy_order("101W09", 2)

    assert result["rt_cd"] == "0"
    assert captured["method"] == "POST"
    assert captured["url"].endswith("/uapi/domestic-futureoption/v1/trading/order")
    assert captured["headers"]["tr_id"] == "TTTO1101U"
    assert captured["headers"]["hashkey"] == "hash-123"
    assert captured["json"]["CANO"] == "12345678"
    assert captured["json"]["ACNT_PRDT_CD"] == "03"
    assert captured["json"]["SLL_BUY_DVSN_CD"] == "02"
    assert captured["json"]["SHTN_PDNO"] == "101W09"
    assert captured["json"]["ORD_QTY"] == "2"
    assert captured["json"]["UNIT_PRICE"] == "0"
    assert captured["hashkey_payload"] == captured["json"]


def test_create_futureoption_order_rejects_non_future_account_code():
    handler = _build_handler()
    handler.account_product_code = "01"

    with pytest.raises(ValueError, match="ACNT_PRDT_CD=03"):
        handler.create_futureoption_sell_order("101W09", 1)

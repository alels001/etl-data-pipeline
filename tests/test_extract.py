import json

from off_pipeline.config import ApiConfig
from off_pipeline.extract import fetch_page, write_page


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class FakeSession:
    """Records the request instead of calling the real API."""

    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return FakeResponse(self.payload)


def test_fetch_page_sends_expected_params():
    session = FakeSession({"products": [{"code": "1"}]})
    products = fetch_page(session, ApiConfig(), page=3)

    assert products == [{"code": "1"}]
    params = session.calls[0]["params"]
    assert params["page"] == 3
    assert params["page_size"] == 100
    assert "code" in params["fields"].split(",")


def test_fetch_page_handles_missing_products_key():
    assert fetch_page(FakeSession({}), ApiConfig(), page=1) == []


def test_write_page_writes_json_lines(tmp_path):
    products = [{"code": "1", "product_name": "Käse"}, {"code": "2"}]
    path = write_page(products, tmp_path / "landing", run_id="run1", page=7)

    assert path.name == "run1_page_0007.jsonl"
    lines = path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == products


def test_user_agent_identifies_the_app():
    assert ApiConfig(contact="me@example.com").user_agent == "off-pipeline/0.2 (me@example.com)"

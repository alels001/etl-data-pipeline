"""Extract: download product pages from the Open Food Facts search API.

This step is plain Python on purpose. Calling a rate-limited API is I/O-bound
work on a single connection, so Spark would add nothing here. The raw pages are
written unchanged to the landing zone; all cleaning happens later in Spark.
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from off_pipeline.config import ApiConfig

log = logging.getLogger(__name__)


def build_session(cfg: ApiConfig) -> requests.Session:
    """HTTP session with automatic retries and exponential backoff."""
    retry = Retry(
        total=5,
        backoff_factor=2,  # waits 2, 4, 8, 16 ... seconds
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = cfg.user_agent
    return session


def fetch_page(session: requests.Session, cfg: ApiConfig, page: int) -> list[dict]:
    params = {
        "fields": ",".join(cfg.fields),
        "sort_by": "unique_scans_n",
        "page_size": cfg.page_size,
        "page": page,
    }
    response = session.get(cfg.base_url, params=params, timeout=cfg.timeout_seconds)
    response.raise_for_status()
    return response.json().get("products", [])


def write_page(products: list[dict], landing_dir: Path, run_id: str, page: int) -> Path:
    """Write one page as JSON Lines (one product per line), which Spark reads natively."""
    landing_dir.mkdir(parents=True, exist_ok=True)
    path = landing_dir / f"{run_id}_page_{page:04d}.jsonl"
    with path.open("w", encoding="utf-8") as f:
        for product in products:
            f.write(json.dumps(product, ensure_ascii=False) + "\n")
    return path


def run_extract(landing_path: str, max_pages: int, cfg: ApiConfig | None = None) -> list[Path]:
    """Download up to `max_pages` pages into landing/ingest_date=YYYY-MM-DD/."""
    cfg = cfg or ApiConfig()
    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%SZ")
    landing_dir = Path(landing_path) / f"ingest_date={now:%Y-%m-%d}"

    session = build_session(cfg)
    written = []
    for page in range(1, max_pages + 1):
        products = fetch_page(session, cfg, page)
        if not products:
            log.info("Page %d is empty, stopping.", page)
            break
        written.append(write_page(products, landing_dir, run_id, page))
        log.info("Page %d: %d products", page, len(products))
        if page < max_pages:
            time.sleep(cfg.seconds_between_requests)

    log.info("Extract finished: %d files in %s", len(written), landing_dir)
    return written

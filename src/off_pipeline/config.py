"""Central configuration.

Everything that changes between environments (paths, API contact) is read from
environment variables, so the same code runs locally and in CI without edits.
"""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    base: str

    @property
    def landing(self) -> str:
        # Raw API pages as JSON files, exactly as received
        return f"{self.base}/landing/products"

    @property
    def bronze_products(self) -> str:
        return f"{self.base}/bronze/products"

    @property
    def bronze_checkpoint(self) -> str:
        # Streaming state lives outside the table folder
        return f"{self.base}/_checkpoints/bronze_products"

    @property
    def silver_products(self) -> str:
        return f"{self.base}/silver/products"

    @property
    def silver_rejected(self) -> str:
        return f"{self.base}/silver/products_rejected"

    def gold(self, table: str) -> str:
        return f"{self.base}/gold/{table}"


@dataclass(frozen=True)
class ApiConfig:
    base_url: str = "https://world.openfoodfacts.org/api/v2/search"
    page_size: int = 100
    # The search endpoint allows max. 10 requests/minute per IP.
    # 6.5 s between requests keeps us safely below that limit.
    seconds_between_requests: float = 6.5
    timeout_seconds: int = 30
    fields: tuple = (
        "code",
        "product_name",
        "brands_tags",
        "categories_tags",
        "countries_tags",
        "nutriments",
        "last_modified_t",
    )
    contact: str = os.getenv("OFF_CONTACT_EMAIL", "your-email@example.com")

    @property
    def user_agent(self) -> str:
        # Open Food Facts asks every client to identify itself
        return f"off-pipeline/0.2 ({self.contact})"


def get_paths() -> Paths:
    default = Path(__file__).resolve().parents[2] / "data" / "lakehouse"
    return Paths(base=os.getenv("LAKEHOUSE_PATH", str(default)))

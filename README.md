# ETL Data Pipeline — Open Food Facts

![Python](https://img.shields.io/badge/Python-3.11-blue)
![SQLite](https://img.shields.io/badge/SQLite-database-green)
![API](https://img.shields.io/badge/REST_API-Open_Food_Facts-orange)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

An end-to-end **Extract → Transform → Load** pipeline that pulls 50,000 food-product records from the
[Open Food Facts API](https://world.openfoodfacts.org/), cleans and enriches the data with Python/pandas,
and loads it into a normalised SQLite database ready for analytics.

---

## Pipeline Overview

```
Open Food Facts API  ──►  JSON  ──►  CSV (filtered & cleaned)  ──►  SQLite database
     (Extract)                          (Transform)                      (Load)
```

| Stage | What happens |
|-------|-------------|
| **Extract** | Paginate through the REST API (v2), fetching 500 pages of products sorted by popularity; save raw JSON locally |
| **Transform** | Filter to products with complete nutrition data → remove nulls, duplicates & non-food categories → normalise text (lowercase, strip `en:` prefixes) → engineer nutrition-level features |
| **Load** | Create a normalised SQLite schema (5 tables with foreign keys) and insert all records |

## Database Schema

```
┌────────────┐         ┌────────────────────────────────────────┐         ┌──────────────┐
│   Brand    │         │              Product                   │         │   Category   │
├────────────┤    FK   ├────────────────────────────────────────┤   FK    ├──────────────┤
│ brand_id   │◄────────│ product_id  (PK)                       │────────►│ category_id  │
│ brand_name │         │ product_name                           │         │ category_name│
└────────────┘         │ energy_100g, sugar_100g, protein_100g  │         └──────────────┘
                       │ sugar_level, energy_level, protein_level│
                       │ is_healthy                              │
                       │ brand_id (FK), category_id (FK)         │
                       └──────────────────┬─────────────────────┘
                                          │
                                          │  M:N
                                          ▼
                              ┌─────────────────────┐
                              │  Product_Country     │
                              ├─────────────────────┤
                              │ product_id (FK)      │
                              │ country_id (FK)      │
                              └─────────┬───────────┘
                                        │
                                        ▼
                              ┌──────────────────┐
                              │     Country      │
                              ├──────────────────┤
                              │ country_id       │
                              │ country_name     │
                              └──────────────────┘
```

## Key Numbers

| Metric | Value |
|--------|------:|
| Raw products fetched | ~50,000 |
| Products after cleaning | ~41,400 |
| Unique brands | ~6,400 |
| Unique categories | ~590 |
| Countries represented | ~114 |
| Healthy-flag products | ~24 % |

## Engineered Features

| Feature | Logic |
|---------|-------|
| `sugar_level` | 0 (< 10 g), 1 (10–20 g), 2 (> 20 g per 100 g) |
| `energy_level` | 0 (< 100 kJ), 1 (100–400 kJ), 2 (> 400 kJ per 100 g) |
| `protein_level` | 0 (< 5 g), 1 (5–15 g), 2 (> 15 g per 100 g) |
| `is_healthy` | 1 if sugar < 10 g **and** protein > 5 g per 100 g |

## Repository Structure

```
├── notebooks/
│   └── ETL_Pipeline_OpenFoodFacts.ipynb   # full pipeline, documented step by step
├── data/
│   └── README.md                          # note: data files are generated at runtime
├── images/
│   └── schema.png                         # ER diagram (optional)
├── requirements.txt
├── .gitignore
└── LICENSE
```

## How to Run

**Option A — Google Colab (no setup):** open the notebook in Colab and run all cells.
The API calls take about 10–15 minutes; everything else finishes in seconds.

**Option B — locally:**

```bash
git clone https://github.com/alels001/etl-data-pipeline.git
cd etl-data-pipeline
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
jupyter notebook notebooks/ETL_Pipeline_OpenFoodFacts.ipynb
```

> **Note:** The Extract step fetches live data from the Open Food Facts API, so exact
> row counts will vary depending on when you run it. The Transform and Load steps are
> deterministic given the same input.

## Tech Stack

Python · pandas · requests · SQLite · JSON · CSV · Jupyter

## Data Source

[Open Food Facts](https://world.openfoodfacts.org/) — a free, open, collaborative database of food
products from around the world. Data is available under the
[Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/1-0/).

## Author

**Ali Elshafey** — Data Science student, Fachhochschule Dortmund
[LinkedIn](https://www.linkedin.com/in/ali-elshafey-profile) · [GitHub](https://github.com/alels001)

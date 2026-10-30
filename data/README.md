# Data

Nothing in this folder is committed. Running the pipeline creates the lakehouse here
(change the location with the `LAKEHOUSE_PATH` environment variable):

```
data/lakehouse/
├── landing/products/ingest_date=YYYY-MM-DD/*.jsonl   raw API pages, unchanged
├── _checkpoints/bronze_products/                     streaming state (which files are ingested)
├── bronze/products/                                  Delta, raw records + ingestion metadata
├── silver/products/                                  Delta, one clean row per barcode
├── silver/products_rejected/                         Delta, rows that failed data quality rules
└── gold/                                             Delta, star schema + aggregates
    ├── dim_brand/  dim_category/  dim_country/
    ├── fact_product/  bridge_product_country/
    └── agg_category_nutrition/  agg_country_summary/
```

The v1 prototype notebook writes its files (`openfoodfacts_data.json`, CSVs, `food_data.db`)
next to the notebook.

All data comes from [Open Food Facts](https://world.openfoodfacts.org/) and is licensed under the
[Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/1-0/).

# Design decisions

Why the pipeline is built the way it is, and what the alternatives were.

## 1. Medallion layers (Bronze / Silver / Gold)

- **Bronze** keeps the source data unchanged, plus ingestion metadata. If a cleaning rule is
  wrong, Silver and Gold can be rebuilt from Bronze without calling the API again.
- **Silver** is the cleaned, trusted version: one row per product, correct types, consistent
  text values. It is the single source of truth for everything downstream.
- **Gold** is shaped for a use case (here: a star schema and two aggregates for analytics).

Alternative: one script that goes straight from API to final tables (like v1). Simpler, but any
change in the cleaning logic means downloading everything again, and there is no raw copy to
debug against.

## 2. Delta Lake instead of plain Parquet

Delta adds a transaction log on top of Parquet files. What this project uses from it:

- `MERGE` (upsert) for Silver. With plain Parquet you would have to rewrite the whole table.
- ACID writes: a failed job does not leave half-written data that readers can see.
- Schema enforcement: writing a DataFrame with wrong column types fails instead of corrupting
  the table.
- Version history (`DESCRIBE HISTORY`, time travel) for debugging.

Alternative: Apache Iceberg offers similar features. Delta was chosen because it is the format
in the target environment and integrates directly with Spark through `delta-spark`.

## 3. Extract in plain Python, not Spark

The API returns 100 products per request and allows 10 requests per minute. This is slow,
sequential I/O. Spark is built for distributing computation over large data, which does not help
here. The extractor just writes raw JSON Lines files; Spark takes over from Bronze.

JSON Lines (one record per line) was chosen over one big JSON array because Spark can read it
line by line and split it across tasks.

## 4. Rate limiting and retries

Open Food Facts limits search requests to 10/minute per IP and can block IPs that exceed it.
The extractor waits 6.5 s between pages and uses `urllib3.Retry` with exponential backoff for
`429` and `5xx` responses. A timeout prevents a hanging request from blocking the job forever.

Found during the first real run (not in the API docs): anonymous search requests get
`401 Unauthorized` from page 11 on, i.e. only the first 1,000 results are available. v2.0 then
crashed and lost the run, although 10 pages were already saved. Now the extractor treats a `401`
on a later page as "end of available data": it logs a warning, stops, and the pipeline continues
with the pages it has. The default is 10 pages. Other HTTP errors still fail the job.

Because the results are sorted by popularity, each daily run fetches the current top 1,000
products; Silver's MERGE adds new barcodes and updates changed ones.

For loading the full catalogue (millions of products) the API is the wrong tool: Open Food Facts
asks to use the bulk export instead.

## 5. Incremental Bronze with Structured Streaming (`availableNow`)

Problem: when the job runs daily, Bronze must ingest only the new landing files, not everything
again.

Solution: a streaming read on the landing folder with `trigger(availableNow=True)`. Spark stores
in the checkpoint which files it has processed. Each run processes all *new* files and then
stops, like a batch job. Deleting the checkpoint means everything is reprocessed.

Alternatives: keep a list of processed files manually (anti-join on `_source_file`), or
Databricks Auto Loader (same idea, but Databricks-only).

## 6. Silver: dedupe, then MERGE by barcode

- The barcode (`code`) is the natural key of a product. v1 did not extract it.
- A product can appear several times in Bronze (different runs, edited in between). A window
  function keeps the newest version (`last_modified_at`, then `_ingested_at`).
- `MERGE` inserts new barcodes and updates existing ones only if the incoming row is newer.
  This makes the job idempotent: running it twice changes nothing.

Known limitation: Silver reads the whole Bronze table on every run. That is fine for thousands of
rows. For much larger data, Silver would read only new Bronze rows (e.g. streaming from the
Bronze Delta table with `foreachBatch` + `MERGE`, or Change Data Feed).

## 7. Data quality: quarantine instead of silent filtering

Each rule is a named condition. Rows that fail any rule go to `silver/products_rejected` with
the list of failed rules. This makes it possible to answer "why is product X missing?" and to
see if a source problem is growing (e.g. suddenly many rows fail `energy_in_range`).

A NULL value counts as a failed rule, so missing nutrients are caught by the range rules.

Alternatives: Great Expectations or Soda for larger rule sets and reporting. For nine rules,
plain PySpark is easier to read and has no extra dependency.

## 8. Gold: star schema, full rebuild, hash keys

- **Star schema**: one fact table with the measurements, dimension tables for descriptive
  attributes. This is the common shape for BI tools and SQL analytics.
- **Bridge table** for countries, because one product is sold in many countries (many-to-many).
- **Full rebuild (overwrite)** on every run: Gold is small and derived entirely from Silver,
  so rebuilding is simpler and cannot drift out of sync.
- **Surrogate keys from `xxhash64(name)`**: deterministic, so a brand keeps its key across
  rebuilds and no lookup table is needed. Trade-off: a 64-bit hash can theoretically collide;
  at a few thousand values the probability is negligible.

## 9. Code structure and testing

Every layer has pure functions (`DataFrame -> DataFrame`) and one `run_*` function that reads
and writes Delta tables. Unit tests call the pure functions on a small fixture file that
contains the edge cases on purpose (duplicate barcode, missing sugar value, cosmetics, empty
barcode, energy of 5,000 kJ, product without country). The integration test runs the full
pipeline twice on real Delta tables and then adds a file with one updated and one new product
to check the MERGE.

## 10. Orchestration with Airflow

The DAG only calls the CLI (`python -m off_pipeline --steps …`), so the logic stays testable in
the package and is not locked into Airflow. `max_active_runs=1` prevents two runs from merging
into Silver at the same time. In a production setup on Kubernetes, the `BashOperator` would
typically be replaced by a `SparkKubernetesOperator` or `KubernetesPodOperator`.

## Not done (yet), and why

- **AWS / S3**: the code uses path strings, so an `s3a://` path would work once the
  `hadoop-aws` connector and credentials are configured. Not done because it needs a paid
  account and adds setup that is not needed to show the pipeline logic.
- **Kubernetes**: would need a Docker image and a cluster (e.g. kind/minikube). A reasonable
  next step, but it is deployment work, not pipeline logic.
- **Kafka / streaming source**: the source is a REST API with daily changes, so a stream would
  not match the data. Structured Streaming is still used for incremental file ingestion.

.PHONY: install test lint run rebuild clean

install:
	pip install -e ".[dev]"

test:
	pytest -v

lint:
	ruff check .

# Full run: download 20 API pages (~2,000 products, about 2 minutes) and build all layers
run:
	python -m off_pipeline

# Rebuild Silver and Gold from Bronze without calling the API
rebuild:
	python -m off_pipeline --steps silver gold

clean:
	rm -rf data/lakehouse spark-warehouse metastore_db derby.log

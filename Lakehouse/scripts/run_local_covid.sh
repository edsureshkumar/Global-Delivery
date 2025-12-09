
#!/usr/bin/env bash
set -euo pipefail

BRONZE=./lakehouse/bronze
SILVER=./lakehouse/silver
GOLD=./lakehouse/gold

echo "Ingesting batch -> Bronze"
python src/ingestion/bronze_ingest_batch.py --input data --bronze ${BRONZE}

echo "Transform -> Silver (COVID)"
python src/transformation/silver_transform_covid.py --bronze ${BRONZE} --silver ${SILVER}

echo "Aggregate -> Gold (COVID)"
python src/business_logic/gold_aggregations_covid.py --silver ${SILVER} --gold ${GOLD}

echo "Done"

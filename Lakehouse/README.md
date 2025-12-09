
# Global Delivery Lakehouse (COVID dataset demo)

A simple Medallion Architecture (Bronze → Silver → Gold) pipeline using Apache Spark + Delta Lake.
This package includes a COVID dataset demo so you can upload the repo to GitHub and HR can review easily.

## How to run (batch)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Ingest to Bronze
python src/ingestion/bronze_ingest_batch.py --input data --bronze ./lakehouse/bronze

# Transform to Silver (COVID)
python src/transformation/silver_transform_covid.py --bronze ./lakehouse/bronze --silver ./lakehouse/silver

# Aggregate to Gold (COVID)
python src/business_logic/gold_aggregations_covid.py --silver ./lakehouse/silver --gold ./lakehouse/gold
```

## Repository structure
```
Global-Delivery-Lakehouse/
├── README.md
├── requirements.txt
├── .gitignore
├── LICENSE
├── scripts/run_local_covid.sh
├── src/
│   ├── configs/paths.yaml
│   ├── utils/delta_helpers.py
│   ├── ingestion/bronze_ingest_batch.py
│   ├── transformation/silver_transform_covid.py
│   └── business_logic/gold_aggregations_covid.py
├── notebooks/01_medallion_demo.py
├── docs/
│   └── medallion.md
└── data/
    └── Covid19_Project.csv```

## Notes
- No confidential data. COVID CSV included for demo.
- Works locally; Databricks-friendly notebook is provided.

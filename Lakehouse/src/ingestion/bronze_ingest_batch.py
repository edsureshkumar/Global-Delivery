
import argparse
from pyspark.sql import functions as F
from src.utils.delta_helpers import get_spark

parser = argparse.ArgumentParser(description='Bronze batch ingestion to Delta')
parser.add_argument('--input', required=True, help='Path to raw input folder (CSV files)')
parser.add_argument('--bronze', required=True, help='Path to Bronze Delta table')
args = parser.parse_args()

spark = get_spark('BronzeBatchIngest')

df = (
    spark.read.option('header', True).csv(args.input)
    .withColumn('ingest_ts', F.current_timestamp())
    .withColumn('source_file', F.input_file_name())
)

df.write.format('delta').mode('append').save(args.bronze)
print('Bronze batch ingested to: ' + args.bronze)

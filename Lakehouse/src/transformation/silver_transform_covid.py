
import argparse
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType
from src.utils.delta_helpers import get_spark

parser = argparse.ArgumentParser(description='Silver (COVID) transformation from Bronze')
parser.add_argument('--bronze', required=True)
parser.add_argument('--silver', required=True)
args = parser.parse_args()

spark = get_spark('SilverCOVID')

df = spark.read.format('delta').load(args.bronze)

# Expected columns: CaseID, Country, State, ReportDate, ConfirmedCases, Deaths, Recovered, Vaccinated
# Basic cleansing/casting
cleaned = (
    df
    .withColumn('ReportDate', F.to_date(F.col('ReportDate')))
    .withColumn('ConfirmedCases', F.col('ConfirmedCases').cast(IntegerType()))
    .withColumn('Deaths', F.col('Deaths').cast(IntegerType()))
    .withColumn('Recovered', F.col('Recovered').cast(IntegerType()))
    .withColumn('Vaccinated', F.col('Vaccinated').cast(IntegerType()))
    .withColumn('Country', F.initcap(F.trim(F.col('Country'))))
    .withColumn('State', F.initcap(F.trim(F.col('State'))))
    .dropna(subset=['Country','State','ReportDate'])
)

# Derived KPI: Active = Confirmed - Deaths - Recovered (lower bound at 0)
cleaned = cleaned.withColumn('Active', F.greatest(F.col('ConfirmedCases') - F.col('Deaths') - F.col('Recovered'), F.lit(0)))

cleaned.write.format('delta').mode('overwrite').save(args.silver)
print('Silver (COVID) written to: ' + args.silver)

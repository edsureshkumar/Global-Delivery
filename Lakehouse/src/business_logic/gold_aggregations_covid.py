
import argparse
from pyspark.sql import functions as F
from src.utils.delta_helpers import get_spark

parser = argparse.ArgumentParser(description='Gold (COVID) aggregations from Silver')
parser.add_argument('--silver', required=True)
parser.add_argument('--gold', required=True)
args = parser.parse_args()

spark = get_spark('GoldCOVID')

df = spark.read.format('delta').load(args.silver)

by_country = (
    df.groupBy('Country')
      .agg(
          F.sum('ConfirmedCases').alias('Confirmed_total'),
          F.sum('Deaths').alias('Deaths_total'),
          F.sum('Recovered').alias('Recovered_total'),
          F.sum('Vaccinated').alias('Vaccinated_total'),
          F.sum('Active').alias('Active_total')
      )
      .withColumn('CFR_percent', F.round(F.col('Deaths_total')/F.col('Confirmed_total')*100, 2))
      .withColumn('Recovery_percent', F.round(F.col('Recovered_total')/F.col('Confirmed_total')*100, 2))
)

by_country.write.format('delta').mode('overwrite').save(args.gold)
print('Gold (COVID) by_country written to: ' + args.gold)

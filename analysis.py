import configparser
import pandas as pd
import calendar
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col, isnan, when, count, pandas_udf, PandasUDFType
from pyspark.sql.types import IntegerType, FloatType
from pyspark.mllib.stat import Statistics
import seaborn as sns

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

output_data = config['IO']['OUTPUT_DATA']

spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .config("org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport().getOrCreate()

# read business travellers table
business_travellers_table = spark.read.parquet(output_data + "immigrants/business_travellers.parquet")

# calculate the diversity score for the origins of businness immigrants
@pandas_udf("float", PandasUDFType.GROUPED_AGG)
def diversity_index(n):
    return sum(i*i for i in n/n.sum())  # Herfindahl Index

diversity_origins = immigrants_origins.groupby('port').agg(diversity_index(immigrants_origins['count']).alias('origins_diversity'))

# calculate median age for each port
median_percentile = F.expr('percentile_approx(age, 0.5)')
immigrants_median_age = business_travellers_table.groupBy('port').agg(median_percentile.alias('immigrants_median_age'))

# calculate total number of immigrants for each port
total_immigrants = business_travellers_table.groupBy('port').count().withColumnRenamed('count', 'total_immigrants')

# join and create the business immigrant profile
diversity = diversity_origins.alias('diversity')
median = immigrants_median_age.alias('median')
total = total_immigrants.alias('total')
business_immigrants_profile = total.join(diversity, ['port']).join(median, ['port'])

# read city demographics table
cities_table = spark.read.parquet(output_data + "cities/demographics.parquet")

# calculate the diversity score of races for the cities
cities_table = cities_table.withColumn("count", cities_table["count"].cast(IntegerType()))
race_diversity = cities_table.groupby(['city', 'state']).agg(diversity_index(cities_table['count']).alias('race_diversity'))

# create city profile
median = cities_table.selectExpr('city', 'state', 'median_age AS residents_median_age').alias('median')
diversity = race_diversity.alias('diversity')
cities_profile = diversity.join(median, ['city', 'state'])

# read airport codes table
airports_table = spark.read.parquet(output_data + "airports/codes.parquet")

# create the final table for analysis
airports = airports_table.alias('airports')
immigrants = business_immigrants_profile.alias('immigrants')
cities = cities_profile.alias('cities')

cond = [airports.region == cities.state, airports.municipality == cities.city]
analysis_table = airports.join(cities, cond).join(immigrants, airports.iata_code == immigrants.port).  \
withColumnRenamed('iata_code', 'port')

# calculate the correlation matrix
df = analysis_table.drop('port', 'region', 'municipality', 'city', 'state')
df = df.select([col(c).cast(FloatType()) for c in df.columns])
col_names = df.columns
features = df.rdd.map(lambda row: row[0:])
corr_mat = Statistics.corr(features, method="pearson")
corr_df = pd.DataFrame(corr_mat)
corr_df.index, corr_df.columns = col_names, col_names

print(corr_df.to_string())

# plot heatmap
sns.heatmap(corr_df, annot=True)
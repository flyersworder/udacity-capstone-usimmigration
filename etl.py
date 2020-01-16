import configparser
import pandas as pd
import calendar
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, isnan, when, count

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Description: This function creates spark session to manipulate its analytical power.

    Returns:
        spark: the SparkSession object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .config("org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport().getOrCreate()
    return spark

def process_immigration_data(spark, input_data, output_data, year):
    """
    Description: This function process the immigrant data through a spark engine: read sas files into the engine and write parquets to the output folder.

    Arguments:
        spark: the spark session object. 
        input_data: the s3 bucket for the input data.
        output_data: the s3 bucket for the output data.
        year: define the year of the input data.

    Returns:
        None
    """
    # get and combine all monthly data and aggregate them into annual data
    for i in range(1, 13):
        file_name = 'i94_' + calendar.month_abbr[i].lower() + str(year)[-2:] + '_sub.sas7bdat'
        #input_data = '../../data/18-83510-I94-Data-2016/'
        file_path = input_data + file_name
        df_spark = spark.read.format('com.github.saurfang.sas.spark').load(file_path)
        df_business = df_spark.filter(df_spark["i94visa"]==1).selectExpr('cicid',  # select only business travellers
                                                                'i94yr as year',
                                                                'i94mon as month',
                                                                'i94port as port',
                                                                'i94cit as origin',
                                                                'i94bir as age',
                                                                'gender'
                                                                )
        if i == 1:
            df = df_business
        else:
            df = df.union(df_business) # aggregate monthly data

    # Performing cleaning tasks for business travellers table to check for invalid origins

    # List of invalid codes
    invalid_codes = [0,54,100,106,134,187,190,200,219,238,239,277,293,300,311,319, \
    365,394,395,400,403,404,407,471,485,501,503,505,506,574,583,589,592,700,705,710, \
    712,719,720,730,731,737,739,740,741,753,755,791,849,914,944,996,999]

    # Remove invalid codes and null ages from the data
    df_valid = df.filter(~df.origin.isin(*invalid_codes)).where(col("age").isNotNull())
    
    # write immigrants table to parquet files
    df_valid.write.mode('append').partitionBy('port', 'year').parquet(output_data + "immigrants/business_travellers.parquet")

def process_dims_data(spark, input_data, output_data):
    """
    Description: This function process both the cities and the airports data through a spark engine: read CSV files into the engine and write parquets to the output folder.

    Arguments:
        spark: the spark session object. 
        input_data: the s3 bucket for the input data.
        output_data: the s3 bucket for the output data.

    Returns:
        None
    """
    
    # get filepath to city demographics data file
    cities_data = input_data + "us-cities-demographics.csv"

    # read cities data file
    cities_table = spark.read.csv(cities_data, header=True, sep=';')
    
    # select relevant variables
    cities_table = cities_table.select(col('City').alias('city'),
                                        col('State Code').alias('state'),
                                        col('Median Age').alias('median_age'),
                                        col('Male Population').alias('male_population'), 
                                        col('Female Population').alias('female_population'),
                                        col('Total Population').alias('total_population'),
                                        col('Foreign-born').alias('foreign_born'),
                                        col('Race').alias('race'),
                                        col('Count').alias('count')
                                        )
    
    # drop null values
    cities_table = cities_table.na.drop()

    # write cities table to parquet files
    cities_table.write.mode('append').parquet(output_data + "cities/demographics.parquet")

    # get filepath to airport codes data file
    airports_data = input_data + "airport-codes_csv.csv"

    # read airports data file
    airports_table = spark.read.csv(airports_data, header=True, sep=',')

    # extract the two-letter region/states code
    extract_code = udf(lambda x: x.split("-")[1])
    airports_table = airports_table.withColumn('region', extract_code('iso_region')).select('iata_code',
                                                                                            'region', 
                                                                                            'municipality')

    # drop null values
    airports_table = airports_table.na.drop()

    # write airports table to parquet files
    airports_table.write.mode('append').parquet(output_data + "airports/codes.parquet")

def quality_checks_valid_records(spark, output_data):
    """
    Description: This function examines the number of valid records for each fields in each tables.

    Arguments:
        spark: the spark session object.
        output_data: the s3 bucket for the output data.

    Returns:
        None
    """ 

    # read all the tables
    immigrants_table = spark.read.parquet(output_data + "immigrants/business_travellers.parquet")
    cities_table = spark.read.parquet(output_data + "cities/demographics.parquet")
    airports_table = spark.read.parquet(output_data + "airports/codes.parquet")

    tables = [immigrants_table, cities_table, airports_table]
    table_names = ['immigrants', 'cities', 'airports']

    # show the number of valid records for each table
    for table, name in zip(tables, table_names):
        print(f"The total number of valid records for table {name} is {table.count()}")
        print(f"The number of valid records for each fields in table {name} is:")
        table.agg(*[count(c).alias(c) for c in table.columns]).show()  # vertical (column-wise) operations in SQL ignore NULLs

def quality_checks_unique_data(spark, output_data, folder_name, col_name):
    """
    Description: This function examines whether a column only contains unique values.

    Arguments:
        spark: the spark session object.
        output_data: the s3 bucket for the output data.
        folder_name: the folder (that corresponds to a certain table) to perform this check.
        col_name: the column to perform this check.

    Returns:
        None
    """ 
    
    # read the relevant table
    table = spark.read.parquet(output_data + folder_name)
    print(f"The number of unique values for column {col_name} is {table.select(col_name).dropDuplicates().count()}")
    
    n_duplicates = table.count() - table.select(col_name).dropDuplicates().count()
    # check whether a column only has unique values
    if n_duplicates > 0:
        print(f'Column {col_name} in table {folder_name} has {n_duplicates} duplicates')


def main():
    spark = create_spark_session()
    input_data = config['IO']['INPUT_DATA']
    output_data = config['IO']['OUTPUT_DATA']
    year = 2016 # currently we only have this year's data
    
    process_immigration_data(spark, input_data, output_data, year)   
    process_dims_data(spark, "", output_data)
    quality_checks_valid_records(spark, output_data)
    quality_checks_unique_data(spark, output_data, "immigrants", "port")
    quality_checks_unique_data(spark, output_data, "immigrants", "cicid")
    quality_checks_unique_data(spark, output_data, "airports/codes.parquet", "iata_code")
    quality_checks_unique_data(spark, output_data, "cities/demographics.parquet", "city")

if __name__ == "__main__":
    main()

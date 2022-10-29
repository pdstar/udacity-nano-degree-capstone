import configparser
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from pyspark.sql import types as T
from pyspark.sql.window import Window
import os
from pyspark.sql import functions as F
from quality_checks import *


from pyspark.sql.functions import monotonically_increasing_id,row_number

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Returns SparkSession that becomes the entry point to Spark to work with RDD, DataFrame, and Dataset     
    """
    spark = SparkSession.builder\
            .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
            .enableHiveSupport().getOrCreate()
    return spark


def set_df_columns_nullable(spark, df, column_list, nullable=True):
    """
    Set columns in Spark dataframe to Nullable or Not Nullable 
    Parameters: 
        spark: SparkSession Object
        df: Saprk Dataframe
        column_list: columns of interest in df
        nullable: True or False
    """
    for struct_field in df.schema:
        if struct_field.name in column_list:
            struct_field.nullable = nullable
    df_mod = spark.createDataFrame(df.rdd, df.schema)
    return df_mod

def convert_datetime(x):
    """
    Converts series to Datetime from SAS datetime
    Parameters: 
        x: SAS Datetime series
    """
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None
    

def process_extract_source_data(spark, input_data):
    """
    Extracts source/raw data from various sources for the Us Immigartion Data warehourse
    Parameters: 
        spark: SparkSession Object
        input_data: Input data path
    """
    I94_df=spark.read.parquet(input_data + "sas_data") 
    
    udf_datetime_from_sas = udf(lambda x: convert_datetime(x), T.DateType())
    I94_df = I94_df.withColumn("arr_date", udf_datetime_from_sas(I94_df.arrdate))
    I94_df = I94_df.withColumn("dep_date", udf_datetime_from_sas(I94_df.depdate))
    
    
    
    fname = input_data + "us-cities-demographics.csv"
    df_usdemog_t = pd.read_csv(fname, delimiter =";")
    float_to_int = ["Male Population", "Female Population", "Number of Veterans", "Foreign-born", "Average Household Size" ]
    df_usdemog_t[float_to_int] = df_usdemog_t[float_to_int].fillna(0).astype(int)
    rename_col={'Median Age': 'median_age', 'Male Population': 'male_pop', 'Female Population': 'female_pop', 
                'Total Population': 'tot_pop', 'Number of Veterans': 'veteran_pop', 'Foreign-born': 'foreign_born', 
                'Average Household Size': 'avg_hose_size', 'State Code': 'state_code'}
    df_usdemog = df_usdemog_t.rename(rename_col, axis='columns')
    
    
    fname = '../../data2/GlobalLandTemperaturesByCity.csv'
    df_temp = pd.read_csv(fname)
    df_temp['Country'] = df_temp['Country'].str.upper()
    df_temp['dt'] = pd.to_datetime(df_temp['dt'])
    
    return I94_df, df_usdemog, df_temp

    
def process_extract_mapping_data(input_data):
    """
    Extracts mapping data which will be required for creating US Immigration Schema
    Parameters: 
        input_data: Input data path
    """
    fname = input_data + 'airport-codes_csv.csv'
    df_apcds = pd.read_csv(fname)
    
    
    fname = input_data + 'country_mapping.csv'
    dim_countrymapping = pd.read_csv(fname)
    
    
    fname = input_data + 'state_code_dim.csv'
    dim_us_state = pd.read_csv(fname)
    
    fname = input_data + 'port_code.csv'
    dim_port = pd.read_csv(fname)
    
    return df_apcds, dim_countrymapping, dim_us_state, dim_port

def process_transform_data(spark, input_data):
    """
    Transforms source data into 4 dim tables and 1 Fact table which comprises of US immigration DB
    Parameters: 
        spark: SparkSession Object
        input_data: Input data path
    """
    
    I94_df, df_usdemog, df_temp = process_extract_source_data(spark, input_data)
    df_apcds, dim_countrymapping, dim_us_state, dim_port = process_extract_mapping_data(input_data)
    
    # Dimension table: US_Demographics
    dim_usdemog = df_usdemog.drop(columns=['State', 'Count'])
    dim_usdemog.insert(loc=0, column='demog_id', value=(dim_usdemog.index+1))
    dim_usdemog=spark.createDataFrame(dim_usdemog) 
    
    # extract columns to create time table
    dim_calendar = I94_df.selectExpr(
        "arr_date as arr_date",
        "dayofmonth(arr_date) as day",
        "weekofyear(arr_date) as week",
        "month(arr_date) as month",
        "year(arr_date) as year",
        "dayofweek(arr_date) as weekday"
    )
    dim_calendar= dim_calendar.dropDuplicates()
    dim_calendar = set_df_columns_nullable(spark,dim_calendar,['arr_date'], False)
    
    ## Dimension table: Weather
    dim_weather_t1 = df_temp.dropna(subset=['dt', 'AverageTemperature','City', 'Country'])
    dim_weather_t1 = dim_weather_t1.sort_values(['City', 'dt']).drop_duplicates('City', keep='last')
    
    # Including country code which connects with Fact table
    dim_weather = pd.merge(dim_countrymapping, dim_weather_t1, on='Country', suffixes=('_map','_wthr'),how='right')
    dim_weather[['country_code']] = dim_weather[['country_code']].fillna(0).astype(int)
    dim_weather.insert(loc=0, column='weather_id', value=(dim_weather.index+1))
    dim_weather.rename(columns={'dt': 'measure_date'}, inplace=True)
    dim_weather=spark.createDataFrame(dim_weather) 
    
                       

    ## Dimension table: Immigrants
    dim_immgrant_t1 = I94_df.selectExpr(
        "int(cicid) as cicid",
        "int(I94CIT) as country_origin",
        "int(BIRYEAR) as birth_year",
        "GENDER as gender",
        "INSNUM as insurance_num",
        "CASE WHEN int(I94VISA) == 1 THEN  'Business' WHEN int(I94VISA) == 2 THEN  'Pleasure' WHEN int(I94VISA) == 3 THEN 'Student' ELSE 'other' END AS visa_category", 
        "year(arr_date) as arr_year"
    )  
    dim_immigrant= dim_immgrant_t1.dropDuplicates(['cicid'])
    dim_immigrant = set_df_columns_nullable(spark,dim_immigrant,['visa_category'])
    dim_immigrant = set_df_columns_nullable(spark,dim_immigrant,['cicid'], False)
    dim_immigrant = dim_immigrant.filter(dim_immigrant.arr_year.isNotNull())
    dim_immigrant = set_df_columns_nullable(spark,dim_immigrant,['arr_year'], False)


    #Fact table: Immigration
    fact_immigration_t1 = I94_df.selectExpr(
        "int(cicid) as cicid",
        "int(I94CIT) as I94CIT",
        "I94PORT",
        "arr_date",
        "month(arr_date) as month",
        "year(arr_date) as arr_year",
        "int(I94MODE) as I94MODE",
        "I94ADDR",
        "dep_date",
        "CASE WHEN int(I94VISA) == 1 THEN  'Business' WHEN int(I94VISA) == 2 THEN  'Pleasure' WHEN int(I94VISA) == 3 THEN 'Student' ELSE 'other' END AS I94VISA",
        "MATFLAG",
        "AIRLINE",
        "ADMNUM",
        "FLTNO",
        "VISATYPE")  
    fact_immigration= fact_immigration_t1.dropDuplicates()
    fact_immigration = set_df_columns_nullable(spark,fact_immigration,['cicid'], False)
    fact_immigration = set_df_columns_nullable(spark,fact_immigration,['I94VISA'])
    fact_immigration = fact_immigration = fact_immigration.withColumn('immig_id', monotonically_increasing_id()+1)
    
    return fact_immigration, dim_immigrant, dim_weather, dim_calendar, dim_usdemog



def process_load_data(spark, input_data, output_data):
    """
    Loads Schema inform of parquet files into output location
    Parameters: 
        spark: SparkSession Object
        input_data: Input data path
        output_data: output data path
    """
    
    fact_immigration, dim_immigrant, dim_weather, dim_calendar, dim_usdemog = process_transform_data(spark, input_data)
    
    # quality checks
    tables = [fact_immigration, dim_immigrant, dim_weather, dim_calendar, dim_usdemog]
    tablename = ['fact_immigration', 'dim_immigrant', 'dim_weather', 'dim_calendar', 'dim_usdemog']
    primary_key = ['immig_id', 'cicid', 'weather_id', 'arr_date', 'demog_id']
    
    (emp_flag, emp_table) = empty_table(tables, tablename)
    (primary_null_flag, primary_null_table) = null_key(tables, tablename, primary_key)
    
    print(emp_flag, emp_table)
    print(primary_null_flag, primary_null_table)
    
    # write tables table to parquet files
    dim_calendar.write.mode('overwrite').partitionBy("year").parquet(output_data + "dim_calendar")
    dim_weather.write.mode('overwrite').parquet(output_data + "dim_weather")
    dim_usdemog.write.mode('overwrite').parquet(output_data + "dim_usdemog")
    fact_immigration.write.mode('overwrite').partitionBy("arr_year").parquet(output_data + "fact_immigration")
    dim_immigrant.write.mode('overwrite').partitionBy("arr_year").parquet(output_data + "dim_immigrant")

    #tables = [fact_immigration, dim_immigrant, dim_weather, dim_calendar, dim_usdemog]
    

def main():
    """
    Main function of script. Creates Sprak Session object
    Runs function that Loads data, processes that data using Spark, creates 
    the facts and dimension tables and writes them back to S3
    """
    spark = create_spark_session()
    output_data = "s3a://mycapstone-de/" 
    #output_data = "output_data/"
    
    input_data = "input_data/"

    process_load_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()

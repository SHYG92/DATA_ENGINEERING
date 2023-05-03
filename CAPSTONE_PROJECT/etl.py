import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month
from pyspark.sql.functions import dayofmonth, hour
from pyspark.sql.functions import weekofyear, date_format
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id, row_number


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

"""create global dataframes as we'll need to read them
couple of times to create the tables"""

# read crime locations data file
locations_df = spark.read.csv('Datasets/CrimeLocation.csv')

# 
districts_df

    
def create_spark_session():
    """create Spark session to start initiating the data lake"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_primary_type(spark, output_data):
    """ETL crimes desc data to create corresponding table
    primary_type dimension"""

    global locations_df
    
    # extract columns to create primary type table
    primary_type_table = locations_df.select(['primary_type'])
    primary_type_table = locations_df.withColumn('primary_type_code',/
                                       row_number().over(Window.orderBy(monotonically_increasing_id())))

    # write primary type table to parquet files partitioned by primary_type_code
    primary_type_table.write.parquet(os.path.join(output_data, 'primary_type'),
                              partitionBy=['primary_type_code'])


def process_districts(spark, output_data):
    """ETL crime locations and chicago districts data to create corresponding table
    districts dimension"""

    global locations_df
    
    # extract columns to create crime locations dataframe
    locations_df = df.select(['district', 'latitude', 'longitude'])
    
    # read chicago districts data file
    with open('Datasets/chicago_districts.json', 'r') as f:
    data = json.load(f)
    
    global districts_df
    
    districts_df = pd.DataFrame(data['data'],
                                columns = ['sid', 'id', 'position', 'created_at', 'created_meta', 'updated_at',
                                           'updated_meta', 'meta', 'district_name', 'designation_date'])
    schema = StructType([ \
    StructField("sid",StringType(),True), \
    StructField("id",StringType(),True), \
    StructField("position",StringType(),True), \
    StructField("created_at", StringType(), True), \
    StructField("created_meta", StringType(), True), \
    StructField("updated_at", StringType(), True), \
    StructField("updated_meta", StringType(), True), \
    StructField("meta", StringType(), True), \
    StructField("district_name", StringType(), True), \
    StructField("designation_date", IntegerType(), True) \
                        ])
    districts_df = spark.createDataFrame(districts_df, schema) 

    # extract columns to create chicago districts dataframe
    districts_df = districts_df.select(['district_name'])
    districts_df = districts_df.withColumn('district',/
                                       row_number().over(Window.orderBy(monotonically_increasing_id())))

    # join crime locations and chicago districts dataframes
    districts_df.join(locations_df, districts_df.district ==  locations_df.district, "inner") \
    .show(truncate=False)
    
    # write districts table to parquet files partitioned by district
    districts_df.write.parquet(os.path.join(output_data, 'districts'),
                              partitionBy=['district'])
    
    
def process_crimes(spark, output_data):
    """ETL crime locations to create corresponding table
    crimes fact table"""

    global locations_df
    
    # extract columns to create crime locations dataframe
    locations_df = df.select(['year', 'district', 'primary_type', 'crime_count'])
    
    # extract columns to create primary type dataframe
    primary_type_df = locations_df.select(['primary_type'])
    primary_type_df = locations_df.withColumn('primary_type_code',/
                                       row_number().over(Window.orderBy(monotonically_increasing_id())))

    # join crime locations and chicago districts dataframes
    locations_df.join(primary_type_df, locations_df.primary_type_code ==  primary_type_df.primary_type_code, "inner") \
    .show(truncate=False)
    
    # write primary type table to parquet files partitioned by primary_type_code
    locations_df.write.parquet(os.path.join(output_data, 'crimes'),
                              partitionBy=['year', 'district', 'primary_type_code'])


def main():
    """implement all methods to initiate the data lake"""
    spark = create_spark_session()
    output_data = "sas_data/"

    process_primary_type(spark, output_data)
    process_districts(spark, output_data)
    process_crimes(spark, output_data)


if __name__ == "__main__":
    main()

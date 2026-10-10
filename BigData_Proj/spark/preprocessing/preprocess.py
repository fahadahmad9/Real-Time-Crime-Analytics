import sys
sys.path.insert(0, '/app')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import yaml

from spark.schemas.schemas import (
    crimes_schema, arrests_schema, violence_schema,
    sex_offenders_schema, police_stations_schema
)

def create_spark_session(config):
    return SparkSession.builder \
        .appName(config['spark']['app_name']) \
        .master(config['spark']['master']) \
        .config("spark.jars.packages",
                "org.postgresql:postgresql:42.6.0,"
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()

def load_crimes(spark, path):
    df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .schema(crimes_schema) \
        .csv(path)

    df = df.withColumnRenamed("Case Number", "CASE_NUMBER") \
           .withColumnRenamed("Primary Type", "PRIMARY_TYPE") \
           .withColumnRenamed("Location Description", "LOCATION_DESC") \
           .withColumnRenamed("Community Area", "COMMUNITY_AREA") \
           .withColumnRenamed("FBI Code", "FBI_CODE") \
           .withColumnRenamed("X Coordinate", "X_COORD") \
           .withColumnRenamed("Y Coordinate", "Y_COORD") \
           .withColumnRenamed("Updated On", "UPDATED_ON")

    df = df.withColumn("DATE_PARSED",
                F.to_timestamp(F.col("Date"), "MM/dd/yyyy hh:mm:ss a")) \
           .withColumn("YEAR", F.year("DATE_PARSED")) \
           .withColumn("MONTH", F.month("DATE_PARSED")) \
           .withColumn("HOUR", F.hour("DATE_PARSED")) \
           .withColumn("DAY_OF_WEEK", F.dayofweek("DATE_PARSED")) \
           .withColumn("LATITUDE", F.col("Latitude").cast("double")) \
           .withColumn("LONGITUDE", F.col("Longitude").cast("double")) \
           .withColumn("ARREST_FLAG", F.when(
               F.upper(F.col("Arrest")) == "TRUE", 1).otherwise(0))

    df = df.filter(F.col("LATITUDE").isNotNull()) \
           .filter(F.col("LONGITUDE").isNotNull()) \
           .filter(F.col("District").isNotNull()) \
           .filter(F.col("PRIMARY_TYPE").isNotNull())

    return df

def load_arrests(spark, path):
    df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .schema(arrests_schema) \
        .csv(path)

    df = df.withColumn("ARREST_DATE_PARSED",
                F.to_timestamp(F.col("ARREST DATE"), "MM/dd/yyyy hh:mm:ss a")) \
           .filter(F.col("CASE NUMBER").isNotNull())

    return df

def load_violence(spark, path):
    df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .schema(violence_schema) \
        .csv(path)

    df = df.withColumn("DATE_PARSED",
                F.to_timestamp(F.col("DATE"), "MM/dd/yyyy hh:mm:ss a")) \
           .withColumn("IS_HOMICIDE",
                F.when(F.upper(F.col("VICTIMIZATION_PRIMARY")) == "HOMICIDE", 1).otherwise(0)) \
           .withColumn("IS_GUNSHOT",
                F.when(F.upper(F.col("GUNSHOT_INJURY_I")) == "YES", 1).otherwise(0)) \
           .filter(F.col("CASE_NUMBER").isNotNull())

    return df

def load_sex_offenders(spark, path):
    df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .schema(sex_offenders_schema) \
        .csv(path)

    df = df.withColumn("IS_VICTIM_MINOR",
                F.when(F.upper(F.col("VICTIM MINOR")) == "Y", 1).otherwise(0)) \
           .filter(F.col("BLOCK").isNotNull())

    return df

def load_police_stations(spark, path):
    df = spark.read \
        .option("header", "true") \
        .schema(police_stations_schema) \
        .csv(path)

    df = df.withColumn("LAT", F.col("LATITUDE").cast("double")) \
           .withColumn("LON", F.col("LONGITUDE").cast("double")) \
           .filter(F.col("DISTRICT").isNotNull())

    return df

def load_all(spark, config):
    data = config['data']
    print("Loading crimes...")
    crimes = load_crimes(spark, data['crime'])
    print(f"  Crimes loaded: {crimes.count()} rows")

    print("Loading arrests...")
    arrests = load_arrests(spark, data['arrests'])
    print(f"  Arrests loaded: {arrests.count()} rows")

    print("Loading violence...")
    violence = load_violence(spark, data['violence'])
    print(f"  Violence loaded: {violence.count()} rows")

    print("Loading sex offenders...")
    sex_offenders = load_sex_offenders(spark, data['sex_offenders'])
    print(f"  Sex offenders loaded: {sex_offenders.count()} rows")

    print("Loading police stations...")
    police_stations = load_police_stations(spark, data['police_stations'])
    print(f"  Police stations loaded: {police_stations.count()} rows")

    return crimes, arrests, violence, sex_offenders, police_stations

if __name__ == '__main__':
    with open('/app/config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")

    crimes, arrests, violence, sex_offenders, police_stations = load_all(spark, config)

    print("\nSchema - Crimes:")
    crimes.printSchema()
    print("\nSample - Crimes:")
    crimes.show(3, truncate=True)

    spark.stop()
    print("Preprocessing complete.")

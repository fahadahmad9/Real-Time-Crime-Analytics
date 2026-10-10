import sys
sys.path.insert(0, '/app')

from pyspark.sql import functions as F
import yaml

def get_pg_props(config):
    pg = config['postgresql']
    url = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['database']}"
    props = {
        "user": pg['user'],
        "password": pg['password'],
        "driver": "org.postgresql.Driver"
    }
    return url, props

def write_to_postgres(df, table, config):
    url, props = get_pg_props(config)
    df.write.jdbc(url=url, table=table, mode="append", properties=props)
    print(f"  Written to PostgreSQL table: {table}")

# --- 7.1 Crime Trend Analysis ---
def crime_trend_analysis(crimes, config):
    print("\nRunning Crime Trend Analysis...")

    trends = crimes.groupBy("YEAR", "MONTH", "DAY_OF_WEEK", "HOUR") \
        .agg(F.count("*").alias("crime_count")) \
        .filter(F.col("YEAR").isNotNull())

    write_to_postgres(trends, "crime_trends", config)
    print(f"  Trend rows written: {trends.count()}")
    return trends

# --- 7.2 Arrest Rate Analysis ---
def arrest_rate_analysis(crimes, arrests, config):
    print("\nRunning Arrest Rate Analysis...")

    # Rename arrests CASE NUMBER to avoid ambiguity
    arrests_renamed = arrests.withColumnRenamed("CASE NUMBER", "ARR_CASE_NUMBER")

    joined = crimes.join(
        arrests_renamed,
        crimes["CASE_NUMBER"] == arrests_renamed["ARR_CASE_NUMBER"],
        "left"
    ).withColumn("IS_ARRESTED",
        F.when(F.col("ARR_CASE_NUMBER").isNotNull(), 1).otherwise(0))

    # By primary type
    by_type = joined.groupBy("PRIMARY_TYPE") \
        .agg(
            F.count("*").alias("total_crimes"),
            F.sum("IS_ARRESTED").alias("total_arrests")
        ).withColumn("arrest_rate",
            F.col("total_arrests") / F.col("total_crimes")) \
        .withColumn("district", F.lit(None).cast("string")) \
        .withColumn("race", F.lit(None).cast("string")) \
        .withColumnRenamed("PRIMARY_TYPE", "primary_type")

    # By district
    by_district = joined.groupBy("District") \
        .agg(
            F.count("*").alias("total_crimes"),
            F.sum("IS_ARRESTED").alias("total_arrests")
        ).withColumn("arrest_rate",
            F.col("total_arrests") / F.col("total_crimes")) \
        .withColumn("primary_type", F.lit(None).cast("string")) \
        .withColumn("race", F.lit(None).cast("string")) \
        .withColumnRenamed("District", "district")

    write_to_postgres(by_type, "arrest_rates", config)
    write_to_postgres(by_district, "arrest_rates", config)

    # Top 10 crime types by arrest rate
    top10 = by_type.orderBy(F.col("arrest_rate").desc()).limit(10)
    print("  Top 10 crime types by arrest rate:")
    top10.select("primary_type", "arrest_rate").show(10, truncate=False)

    return by_type, by_district

# --- 7.3 Violence and Gunshot Analysis ---
def violence_analysis(violence, config):
    print("\nRunning Violence Analysis...")

    stats = violence.withColumn("month_int", F.col("MONTH").cast("integer")) \
        .withColumn("district_str", F.col("DISTRICT").cast("double").cast("integer").cast("string")) \
        .filter(F.col("month_int").isNotNull()) \
        .filter(F.col("district_str").isNotNull()) \
        .groupBy("month_int", "district_str") \
        .agg(
            F.sum("IS_HOMICIDE").alias("total_homicides"),
            F.sum(F.when(F.col("IS_HOMICIDE") == 0, 1).otherwise(0))
                .alias("non_fatal_shootings"),
            F.sum("IS_GUNSHOT").alias("gunshot_injuries")
        ).withColumn("community_area", F.lit(None).cast("string")) \
         .withColumnRenamed("month_int", "month") \
         .withColumnRenamed("district_str", "district")

    write_to_postgres(stats, "violence_stats", config)

    # Top community areas
    top_communities = violence.groupBy("COMMUNITY_AREA") \
        .agg(F.count("*").alias("incident_count")) \
        .orderBy(F.col("incident_count").desc()) \
        .limit(10)

    print("  Top 10 community areas by violence:")
    top_communities.show(10, truncate=False)

    return stats

# --- 7.4 Sex Offender Proximity Analysis ---
def sex_offender_analysis(sex_offenders, police_stations, config):
    print("\nRunning Sex Offender Analysis...")

    # Extract district from BLOCK (first 2 chars as proxy)
    so = sex_offenders.withColumn(
        "DISTRICT_EST",
        F.substring(F.col("BLOCK"), 1, 2)
    )

    stats = so.groupBy("DISTRICT_EST") \
        .agg(
            F.count("*").alias("offender_count"),
            F.sum("IS_VICTIM_MINOR").alias("victim_minor_count")
        ).withColumnRenamed("DISTRICT_EST", "district")

    write_to_postgres(stats, "sex_offender_stats", config)

    # Flag priority records
    priority = so.filter(F.col("IS_VICTIM_MINOR") == 1)
    print(f"  Priority records (victim minor): {priority.count()}")

    return stats

# --- 7.6 Cross-Dataset Correlation ---
def correlation_analysis(crimes, violence, sex_offenders, arrests, config):
    print("\nRunning Cross-Dataset Correlation Analysis...")

    # Violence rate vs arrest rate by district
    arrests_renamed = arrests.withColumnRenamed("CASE NUMBER", "ARR_CASE_NUMBER")
    crimes_joined = crimes.join(
        arrests_renamed,
        crimes["CASE_NUMBER"] == arrests_renamed["ARR_CASE_NUMBER"],
        "left"
    ).withColumn("IS_ARRESTED",
        F.when(F.col("ARR_CASE_NUMBER").isNotNull(), 1).otherwise(0))

    arrest_by_district = crimes_joined.groupBy("District") \
        .agg(
            F.count("*").alias("total_crimes"),
            F.sum("IS_ARRESTED").alias("total_arrests")
        ).withColumn("arrest_rate",
            F.col("total_arrests") / F.col("total_crimes")) \
        .withColumnRenamed("District", "district")

    violence_by_district = violence.groupBy("DISTRICT") \
        .agg(F.count("*").alias("violence_count")) \
        .withColumn("district", F.col("DISTRICT").cast("double").cast("integer").cast("string")) \
        .select("district", "violence_count")

    corr1 = arrest_by_district.join(violence_by_district, "district", "left") \
        .withColumn("violence_rate",
            F.col("violence_count") / F.col("total_crimes")) \
        .withColumn("correlation_type",
            F.lit("violence_rate_vs_arrest_rate")) \
        .withColumn("community_area", F.lit(None).cast("string")) \
        .withColumn("sex_offender_density", F.lit(None).cast("double")) \
        .withColumn("crime_rate", F.lit(None).cast("double")) \
        .select("correlation_type", "district", "community_area",
                "violence_rate", "arrest_rate",
                "sex_offender_density", "crime_rate")

    write_to_postgres(corr1, "correlations", config)

    # Sex offender density vs crime rate by community area
    crime_by_area = crimes.groupBy("COMMUNITY_AREA") \
        .agg(F.count("*").alias("crime_count")) \
        .withColumnRenamed("COMMUNITY_AREA", "community_area")

    so_by_area = sex_offenders.withColumn(
        "community_area", F.substring(F.col("BLOCK"), 1, 4)
    ).groupBy("community_area") \
        .agg(F.count("*").alias("so_count"))

    corr2 = crime_by_area.join(so_by_area, "community_area", "left") \
        .withColumn("sex_offender_density",
            F.col("so_count").cast("double")) \
        .withColumn("crime_rate", F.col("crime_count").cast("double")) \
        .withColumn("correlation_type",
            F.lit("sex_offender_density_vs_crime_rate")) \
        .withColumn("district", F.lit(None).cast("string")) \
        .withColumn("violence_rate", F.lit(None).cast("double")) \
        .withColumn("arrest_rate", F.lit(None).cast("double")) \
        .select("correlation_type", "district", "community_area",
                "violence_rate", "arrest_rate",
                "sex_offender_density", "crime_rate")

    write_to_postgres(corr2, "correlations", config)
    print("  Correlations written.")

    return corr1, corr2

if __name__ == '__main__':
    from pyspark.sql import SparkSession
    from spark.preprocessing.preprocess import create_spark_session, load_all

    with open('/app/config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")

    crimes, arrests, violence, sex_offenders, police_stations = load_all(spark, config)

    crime_trend_analysis(crimes, config)
    arrest_rate_analysis(crimes, arrests, config)
    violence_analysis(violence, config)
    sex_offender_analysis(sex_offenders, police_stations, config)
    correlation_analysis(crimes, violence, sex_offenders, arrests, config)

    spark.stop()
    print("\nAll analytics complete.")

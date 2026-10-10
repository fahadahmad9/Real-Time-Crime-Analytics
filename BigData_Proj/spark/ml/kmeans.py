import sys
sys.path.insert(0, '/app')

from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import yaml

def run_kmeans(crimes, config):
    print("\nRunning K-Means Clustering...")

    k = config['spark']['kmeans_k']

    # Keep only rows with valid coordinates
    geo_df = crimes.filter(
        F.col("LATITUDE").isNotNull() &
        F.col("LONGITUDE").isNotNull() &
        (F.col("LATITUDE") != 0.0) &
        (F.col("LONGITUDE") != 0.0)
    ).select("LATITUDE", "LONGITUDE", "PRIMARY_TYPE", "District")

    print(f"  Rows with valid coordinates: {geo_df.count()}")

    # Assemble features vector
    assembler = VectorAssembler(
        inputCols=["LATITUDE", "LONGITUDE"],
        outputCol="features"
    )
    geo_assembled = assembler.transform(geo_df)

    # Train K-Means
    kmeans = KMeans(k=k, seed=42, featuresCol="features", predictionCol="cluster")
    model = kmeans.fit(geo_assembled)

    # Evaluate silhouette score
    predictions = model.transform(geo_assembled)
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette"
    )
    silhouette = evaluator.evaluate(predictions)
    print(f"  Silhouette Score: {silhouette:.4f}")

    # Get cluster centroids
    centers = model.clusterCenters()
    print(f"  Cluster Centroids (k={k}):")
    for i, center in enumerate(centers):
        print(f"    Cluster {i}: lat={center[0]:.6f}, lon={center[1]:.6f}")

    # Count crimes per cluster
    cluster_counts = predictions.groupBy("cluster") \
        .agg(F.count("*").alias("crime_count")) \
        .orderBy("cluster")

    return predictions, centers, cluster_counts

def write_hotspots_to_postgres(centers, cluster_counts, config):
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    pg = config['postgresql']
    url = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['database']}"
    props = {
        "user": pg['user'],
        "password": pg['password'],
        "driver": "org.postgresql.Driver"
    }

    # Build hotspots dataframe from centroids + counts
    counts_collected = cluster_counts.collect()
    count_map = {row['cluster']: row['crime_count'] for row in counts_collected}

    rows = []
    for i, center in enumerate(centers):
        rows.append((
            int(i),
            float(center[0]),
            float(center[1]),
            int(count_map.get(i, 0))
        ))

    hotspots_df = spark.createDataFrame(
        rows,
        ["cluster_id", "centroid_lat", "centroid_lon", "crime_count"]
    )

    hotspots_df.write.jdbc(url=url, table="hotspots", mode="append", properties=props)
    print("  Hotspots written to PostgreSQL.")
    return hotspots_df

if __name__ == '__main__':
    from pyspark.sql import SparkSession
    from spark.preprocessing.preprocess import create_spark_session, load_all

    with open('/app/config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")

    crimes, arrests, violence, sex_offenders, police_stations = load_all(spark, config)

    predictions, centers, cluster_counts = run_kmeans(crimes, config)
    hotspots_df = write_hotspots_to_postgres(centers, cluster_counts, config)

    print("\nHotspot clusters:")
    hotspots_df.show(truncate=False)

    spark.stop()
    print("K-Means complete.")

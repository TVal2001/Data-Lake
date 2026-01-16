"""
Spark Curate Job - Agrège et prépare les données finales dans curated
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    countDistinct, when, round as spark_round, current_timestamp, lit,
    desc, asc, percent_rank, dense_rank
)
from pyspark.sql.window import Window
from datetime import datetime
from pathlib import Path

from spark_config import get_spark_session, stop_spark, STAGING_PATH, CURATED_PATH, to_file_uri


def load_staging_data(spark: SparkSession, name: str) -> DataFrame:
    """
    Charge les données staging (Parquet).
    
    Args:
        spark: Session Spark
        name: Nom du dataset (pattern)
    
    Returns:
        DataFrame chargé
    """
    staging_dirs = list(STAGING_PATH.glob(f"*_{name}"))
    
    if not staging_dirs:
        print(f"⚠ Aucune donnée staging pour: {name}")
        return None
    
    # Prendre le plus récent
    latest = sorted(staging_dirs)[-1]
    
    df = spark.read.parquet(to_file_uri(str(latest)))
    print(f"✓ Chargé depuis staging: {latest}")
    print(f"  - {df.count()} lignes")
    
    return df


def create_login_metrics(df: DataFrame) -> DataFrame:
    """
    Crée des métriques agrégées pour les logs de connexion.
    
    Args:
        df: DataFrame des logs transformés
    
    Returns:
        DataFrame avec métriques globales
    """
    metrics = df.agg(
        count("*").alias("total_attempts"),
        spark_sum(when(col("is_success") == True, 1).otherwise(0)).alias("successful_logins"),
        spark_sum(when(col("is_success") == False, 1).otherwise(0)).alias("failed_logins"),
        countDistinct("user_id").alias("unique_users"),
        countDistinct("ip_address").alias("unique_ips"),
        spark_min("timestamp").alias("first_attempt"),
        spark_max("timestamp").alias("last_attempt")
    )
    
    # Calculer le taux de succès
    metrics = metrics.withColumn(
        "success_rate",
        spark_round(col("successful_logins") / col("total_attempts") * 100, 2)
    )
    
    return metrics


def create_user_stats(df: DataFrame) -> DataFrame:
    """
    Crée des statistiques par utilisateur.
    
    Args:
        df: DataFrame des logs
    
    Returns:
        DataFrame avec stats par user_id
    """
    user_stats = df.groupBy("user_id").agg(
        count("*").alias("total_attempts"),
        spark_sum(when(col("is_success") == True, 1).otherwise(0)).alias("successes"),
        spark_sum(when(col("is_success") == False, 1).otherwise(0)).alias("failures"),
        countDistinct("ip_address").alias("unique_ips"),
        spark_min("timestamp").alias("first_seen"),
        spark_max("timestamp").alias("last_seen")
    )
    
    # Taux de succès par utilisateur
    user_stats = user_stats.withColumn(
        "success_rate",
        spark_round(col("successes") / col("total_attempts") * 100, 2)
    )
    
    # Rank des utilisateurs les plus actifs
    window = Window.orderBy(desc("total_attempts"))
    user_stats = user_stats.withColumn("activity_rank", dense_rank().over(window))
    
    return user_stats.orderBy("activity_rank")


def create_hourly_stats(df: DataFrame) -> DataFrame:
    """
    Crée des statistiques par heure.
    
    Args:
        df: DataFrame des logs
    
    Returns:
        DataFrame avec stats par heure
    """
    hourly = df.groupBy("hour").agg(
        count("*").alias("attempts"),
        spark_sum(when(col("is_success") == True, 1).otherwise(0)).alias("successes"),
        spark_sum(when(col("is_success") == False, 1).otherwise(0)).alias("failures"),
        countDistinct("user_id").alias("unique_users")
    )
    
    hourly = hourly.withColumn(
        "success_rate",
        spark_round(col("successes") / col("attempts") * 100, 2)
    )
    
    return hourly.orderBy("hour")


def create_suspicious_ips(df: DataFrame, threshold: int = 5) -> DataFrame:
    """
    Identifie les IPs suspectes (beaucoup d'échecs).
    
    Args:
        df: DataFrame des logs
        threshold: Seuil d'échecs pour être suspect
    
    Returns:
        DataFrame des IPs suspectes
    """
    ip_stats = df.groupBy("ip_address").agg(
        count("*").alias("total_attempts"),
        spark_sum(when(col("is_success") == False, 1).otherwise(0)).alias("failures"),
        countDistinct("user_id").alias("targeted_users")
    )
    
    # Filtrer les IPs avec beaucoup d'échecs
    suspicious = ip_stats.filter(col("failures") >= threshold)
    
    suspicious = suspicious.withColumn(
        "failure_rate",
        spark_round(col("failures") / col("total_attempts") * 100, 2)
    )
    
    return suspicious.orderBy(desc("failures"))


def save_to_curated(df: DataFrame, name: str, format: str = "parquet"):
    """
    Sauvegarde un DataFrame dans curated.
    
    Args:
        df: DataFrame à sauvegarder
        name: Nom du dataset
        format: Format de sortie (parquet, json, csv)
    """
    CURATED_PATH.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = to_file_uri(str(CURATED_PATH / f"{timestamp}_{name}"))
    
    if format == "parquet":
        df.write.mode("overwrite").parquet(output_path)
    elif format == "json":
        df.coalesce(1).write.mode("overwrite").json(output_path)
    elif format == "csv":
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    
    print(f"✓ Sauvegardé dans curated: {output_path}")


def run_curation(spark: SparkSession):
    """Exécute toutes les curations."""
    
    print("\n" + "="*50)
    print("📦 CURATION")
    print("="*50)
    
    # Charger les logs depuis staging
    df_logs = load_staging_data(spark, "logs")
    
    if df_logs:
        print("\n--- Métriques globales ---")
        metrics = create_login_metrics(df_logs)
        metrics.show(truncate=False)
        save_to_curated(metrics, "global_metrics", "json")
        
        print("\n--- Statistiques par utilisateur ---")
        user_stats = create_user_stats(df_logs)
        user_stats.show(10, truncate=False)
        save_to_curated(user_stats, "user_statistics")
        
        print("\n--- Statistiques par heure ---")
        hourly = create_hourly_stats(df_logs)
        hourly.show(24, truncate=False)
        save_to_curated(hourly, "hourly_statistics")
        
        print("\n--- IPs suspectes (≥5 échecs) ---")
        suspicious = create_suspicious_ips(df_logs, threshold=5)
        print(f"IPs suspectes trouvées: {suspicious.count()}")
        suspicious.show(20, truncate=False)
        save_to_curated(suspicious, "suspicious_ips")
    
    # Charger sales si disponible
    df_sales = load_staging_data(spark, "sales")
    if df_sales:
        print("\n--- Données Sales ---")
        df_sales.show(5, truncate=False)


if __name__ == "__main__":
    print("="*50)
    print("🚀 SPARK CURATE JOB")
    print("="*50)
    
    spark = get_spark_session("CurateJob")
    
    try:
        run_curation(spark)
    
    finally:
        stop_spark(spark)

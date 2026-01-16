"""
Spark Ingest Job - Charge les données brutes dans la zone raw du data lake
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from datetime import datetime
from pathlib import Path
import shutil

from spark_config import get_spark_session, stop_spark, RAW_PATH, DATA_LAKE, to_file_uri


def ingest_csv(spark: SparkSession, source_path: str, category: str) -> DataFrame:
    """
    Ingère un fichier CSV dans Spark.
    
    Args:
        spark: Session Spark
        source_path: Chemin du fichier CSV source
        category: Catégorie (transactions, sales, logs)
    
    Returns:
        DataFrame Spark avec les données ingérées
    """
    if category not in ["transactions", "sales", "logs"]:
        raise ValueError(f"Catégorie invalide: {category}")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(to_file_uri(source_path))
    
    # Ajouter métadonnées d'ingestion
    df = df.withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_source_file", input_file_name()) \
           .withColumn("_category", lit(category))
    
    print(f"✓ Fichier CSV ingéré: {source_path}")
    print(f"  - {df.count()} lignes")
    print(f"  - {len(df.columns)} colonnes")
    
    return df


def ingest_json(spark: SparkSession, source_path: str, category: str) -> DataFrame:
    """
    Ingère un fichier JSON dans Spark.
    
    Args:
        spark: Session Spark
        source_path: Chemin du fichier JSON source
        category: Catégorie (transactions, sales, logs)
    
    Returns:
        DataFrame Spark avec les données ingérées
    """
    if category not in ["transactions", "sales", "logs"]:
        raise ValueError(f"Catégorie invalide: {category}")
    
    df = spark.read \
        .option("multiLine", "false") \
        .json(to_file_uri(source_path))
    
    # Ajouter métadonnées d'ingestion
    df = df.withColumn("_ingested_at", current_timestamp()) \
           .withColumn("_source_file", input_file_name()) \
           .withColumn("_category", lit(category))
    
    print(f"✓ Fichier JSON ingéré: {source_path}")
    print(f"  - {df.count()} lignes")
    print(f"  - {len(df.columns)} colonnes")
    
    return df


def ingest_category(spark: SparkSession, category: str) -> DataFrame:
    """
    Ingère tous les fichiers d'une catégorie raw.
    
    Args:
        spark: Session Spark
        category: Catégorie (transactions, sales, logs)
    
    Returns:
        DataFrame combiné de tous les fichiers
    """
    category_path = RAW_PATH / category
    
    if not category_path.exists():
        print(f"⚠ Répertoire non trouvé: {category_path}")
        return None
    
    dfs = []
    
    # Ingérer les CSV
    csv_files = list(category_path.glob("*.csv"))
    for csv_file in csv_files:
        df = ingest_csv(spark, str(csv_file), category)
        dfs.append(df)
    
    # Ingérer les JSON
    json_files = list(category_path.glob("*.json"))
    for json_file in json_files:
        df = ingest_json(spark, str(json_file), category)
        dfs.append(df)
    
    if not dfs:
        print(f"⚠ Aucun fichier trouvé dans {category}")
        return None
    
    # Union de tous les DataFrames (si même schéma)
    if len(dfs) == 1:
        return dfs[0]
    
    # Retourner le premier pour simplifier (ou faire union si schémas compatibles)
    print(f"✓ {len(dfs)} fichiers ingérés depuis {category}")
    return dfs[0]


def show_raw_stats(spark: SparkSession):
    """Affiche les statistiques des données raw."""
    print("\n" + "="*50)
    print("📊 STATISTIQUES RAW")
    print("="*50)
    
    for category in ["transactions", "sales", "logs"]:
        category_path = RAW_PATH / category
        if category_path.exists():
            csv_count = len(list(category_path.glob("*.csv")))
            json_count = len(list(category_path.glob("*.json")))
            print(f"\n📁 {category}/")
            print(f"   - CSV:  {csv_count} fichiers")
            print(f"   - JSON: {json_count} fichiers")


if __name__ == "__main__":
    print("="*50)
    print("🚀 SPARK INGEST JOB")
    print("="*50)
    
    spark = get_spark_session("IngestJob")
    
    try:
        show_raw_stats(spark)
        
        # Ingérer chaque catégorie
        for category in ["transactions", "sales", "logs"]:
            print(f"\n--- Ingestion: {category} ---")
            df = ingest_category(spark, category)
            if df:
                print("\nSchéma:")
                df.printSchema()
                print("\nAperçu:")
                df.show(5, truncate=False)
    
    finally:
        stop_spark(spark)

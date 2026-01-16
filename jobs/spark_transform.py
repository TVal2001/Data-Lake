"""
Spark Transform Job - Nettoie et transforme les données raw vers staging
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lower, trim, regexp_replace, to_timestamp, to_date,
    current_timestamp, lit, when, hour, dayofweek, month, year
)
from pyspark.sql.types import TimestampType
from datetime import datetime
from pathlib import Path

from spark_config import get_spark_session, stop_spark, RAW_PATH, STAGING_PATH, to_file_uri


def clean_dataframe(df: DataFrame) -> DataFrame:
    """
    Nettoie un DataFrame Spark.
    
    Args:
        df: DataFrame à nettoyer
    
    Returns:
        DataFrame nettoyé
    """
    # Nettoyer les colonnes string
    for field in df.schema.fields:
        if str(field.dataType) == "StringType":
            col_name = field.name
            df = df.withColumn(col_name, trim(col(col_name)))
    
    return df


def transform_login_logs(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Transforme les logs de connexion.
    
    Args:
        spark: Session Spark
        df: DataFrame des logs bruts
    
    Returns:
        DataFrame transformé avec colonnes enrichies
    """
    # Nettoyer
    df = clean_dataframe(df)
    
    # Convertir timestamp
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
    
    # Enrichir avec des colonnes temporelles
    df = df.withColumn("date", to_date(col("timestamp"))) \
           .withColumn("hour", hour(col("timestamp"))) \
           .withColumn("day_of_week", dayofweek(col("timestamp"))) \
           .withColumn("month", month(col("timestamp"))) \
           .withColumn("year", year(col("timestamp")))
    
    # Ajouter flag de succès (boolean)
    df = df.withColumn("is_success", when(col("result") == "SUCCESS", True).otherwise(False))
    
    # Extraire le dernier octet de l'IP (pour analyse)
    df = df.withColumn("ip_last_octet", 
                       regexp_replace(col("ip_address"), r".*\.(\d+)$", "$1").cast("int"))
    
    # Métadonnées de transformation
    df = df.withColumn("_transformed_at", current_timestamp())
    
    return df


def transform_category(spark: SparkSession, category: str) -> DataFrame:
    """
    Transforme tous les fichiers d'une catégorie.
    
    Args:
        spark: Session Spark
        category: Catégorie à transformer
    
    Returns:
        DataFrame transformé
    """
    category_path = RAW_PATH / category
    
    if not category_path.exists():
        print(f"⚠ Catégorie non trouvée: {category}")
        return None
    
    # Lire tous les CSV
    csv_files = list(category_path.glob("*.csv"))
    json_files = list(category_path.glob("*.json"))
    
    df = None
    
    if csv_files:
        # Utiliser file:// URI pour forcer le filesystem local
        csv_paths = [to_file_uri(str(f)) for f in csv_files]
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(csv_paths)
    
    if json_files:
        # Utiliser file:// URI pour forcer le filesystem local
        json_paths = [to_file_uri(str(f)) for f in json_files]
        df_json = spark.read.json(json_paths)
        if df is not None:
            # Union si on a déjà des CSV
            df = df.unionByName(df_json, allowMissingColumns=True)
        else:
            df = df_json
    
    if df is None:
        print(f"⚠ Aucune donnée dans {category}")
        return None
    
    # Appliquer transformation selon le type de données
    if "result" in df.columns and "ip_address" in df.columns:
        df = transform_login_logs(spark, df)
    else:
        df = clean_dataframe(df)
    
    print(f"✓ Catégorie {category} transformée: {df.count()} lignes")
    return df


def save_to_staging(df: DataFrame, name: str):
    """
    Sauvegarde un DataFrame dans staging en format Parquet.
    
    Args:
        df: DataFrame à sauvegarder
        name: Nom du dataset
    """
    STAGING_PATH.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = to_file_uri(str(STAGING_PATH / f"{timestamp}_{name}"))
    
    df.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print(f"✓ Sauvegardé dans staging: {output_path}")


def run_transformations(spark: SparkSession):
    """Exécute toutes les transformations."""
    
    print("\n" + "="*50)
    print("🔄 TRANSFORMATIONS")
    print("="*50)
    
    for category in ["transactions", "sales", "logs"]:
        print(f"\n--- Transformation: {category} ---")
        df = transform_category(spark, category)
        
        if df:
            print("\n📋 Schéma transformé:")
            df.printSchema()
            
            print("\n📊 Aperçu:")
            df.show(5, truncate=False)
            
            # Sauvegarder en staging
            save_to_staging(df, category)


if __name__ == "__main__":
    print("="*50)
    print("🚀 SPARK TRANSFORM JOB")
    print("="*50)
    
    spark = get_spark_session("TransformJob")
    
    try:
        run_transformations(spark)
    
    finally:
        stop_spark(spark)

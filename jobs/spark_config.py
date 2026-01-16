"""
Configuration Spark pour le Mini Data Lake (Mode Local)
"""
from pyspark.sql import SparkSession
from pathlib import Path


# Chemins du Data Lake
BASE_PATH = Path(__file__).parent.parent
DATA_LAKE = BASE_PATH / "data_lake"
RAW_PATH = DATA_LAKE / "raw"
STAGING_PATH = DATA_LAKE / "staging"
CURATED_PATH = DATA_LAKE / "curated"


def get_spark_session(app_name: str = "MiniDataLake") -> SparkSession:
    """
    Crée et retourne une session Spark configurée pour le filesystem LOCAL.
    
    Args:
        app_name: Nom de l'application Spark
    
    Returns:
        SparkSession configurée
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", str(DATA_LAKE / "warehouse")) \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .getOrCreate()
    
    # Forcer le filesystem local dans le contexte Hadoop
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.defaultFS", "file:///")
    
    # Réduire les logs verbeux
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def to_file_uri(path: str) -> str:
    """
    Convertit un chemin en URI file:// pour Spark.
    
    path_str =str(path).replace("\\", "/")
    return f"file:///{path_srt}"
    
    Args:
        path: Chemin local
    
    Returns:
        URI file://
    """
    return f"file://{path}"


def stop_spark(spark: SparkSession):
    """Arrête proprement la session Spark."""
    if spark:
        spark.stop()
        print("✓ Session Spark arrêtée")

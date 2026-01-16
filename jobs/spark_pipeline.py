"""
Spark Pipeline Complet - Orchestre ingest → transform → curate
"""
from pyspark.sql import SparkSession
from datetime import datetime

from spark_config import get_spark_session, stop_spark, RAW_PATH, STAGING_PATH, CURATED_PATH
from spark_ingest import ingest_category, show_raw_stats
from spark_transform import transform_category, save_to_staging
from spark_curate import (
    load_staging_data, create_login_metrics, create_user_stats,
    create_hourly_stats, create_suspicious_ips, save_to_curated
)


def run_full_pipeline():
    """
    Exécute le pipeline complet du data lake.
    
    Étapes:
    1. Ingest: Lecture des données raw
    2. Transform: Nettoyage et enrichissement → staging
    3. Curate: Agrégation et métriques → curated
    """
    print("="*60)
    print("🚀 MINI DATA LAKE - PIPELINE SPARK COMPLET")
    print(f"   Démarré: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    spark = get_spark_session("FullPipeline")
    
    try:
        # ========================================
        # ÉTAPE 1: INGEST
        # ========================================
        print("\n" + "━"*60)
        print("📥 ÉTAPE 1: INGESTION")
        print("━"*60)
        
        show_raw_stats(spark)
        
        # ========================================
        # ÉTAPE 2: TRANSFORM
        # ========================================
        print("\n" + "━"*60)
        print("🔄 ÉTAPE 2: TRANSFORMATION")
        print("━"*60)
        
        for category in ["transactions", "sales", "logs"]:
            print(f"\n▶ Transformation: {category}")
            df = transform_category(spark, category)
            if df:
                save_to_staging(df, category)
                print(f"  ✓ {df.count()} lignes transformées")
        
        # ========================================
        # ÉTAPE 3: CURATE
        # ========================================
        print("\n" + "━"*60)
        print("📦 ÉTAPE 3: CURATION")
        print("━"*60)
        
        # Traitement des logs
        df_logs = load_staging_data(spark, "logs")
        
        if df_logs:
            # Métriques globales
            print("\n▶ Génération des métriques globales...")
            metrics = create_login_metrics(df_logs)
            save_to_curated(metrics, "global_metrics", "json")
            
            # Stats utilisateurs
            print("\n▶ Génération des stats utilisateurs...")
            user_stats = create_user_stats(df_logs)
            save_to_curated(user_stats, "user_statistics")
            
            # Stats horaires
            print("\n▶ Génération des stats horaires...")
            hourly = create_hourly_stats(df_logs)
            save_to_curated(hourly, "hourly_statistics")
            
            # IPs suspectes
            print("\n▶ Détection des IPs suspectes...")
            suspicious = create_suspicious_ips(df_logs, threshold=5)
            save_to_curated(suspicious, "suspicious_ips")
            
            # ========================================
            # RAPPORT FINAL
            # ========================================
            print("\n" + "━"*60)
            print("📊 RAPPORT FINAL")
            print("━"*60)
            
            print("\n🔹 Métriques globales:")
            metrics.show(truncate=False)
            
            print("\n🔹 Top 10 utilisateurs les plus actifs:")
            user_stats.select(
                "user_id", "total_attempts", "successes", "failures", "success_rate", "activity_rank"
            ).show(10, truncate=False)
            
            print("\n🔹 Activité par heure:")
            hourly.show(24, truncate=False)
            
            suspicious_count = suspicious.count()
            print(f"\n🔹 IPs suspectes détectées: {suspicious_count}")
            if suspicious_count > 0:
                suspicious.show(10, truncate=False)
        
        print("\n" + "="*60)
        print(f"✅ PIPELINE TERMINÉ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60)
        
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    run_full_pipeline()

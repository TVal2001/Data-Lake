# Mini Data Lake 🚀

Un data lake minimaliste avec **Apache Spark** pour gérer le cycle de vie des données : ingestion, transformation et curation.

## 📁 Structure

```
mini-data-lake/
├── data_lake/
│   ├── raw/                    # Données brutes (CSV, JSON)
│   │   ├── transactions/
│   │   ├── sales/
│   │   └── logs/
│   ├── staging/                # Données transformées (Parquet)
│   └── curated/                # Données agrégées (Parquet/JSON)
├── jobs/
│   ├── spark_config.py         # Configuration Spark
│   ├── spark_ingest.py         # Job d'ingestion
│   ├── spark_transform.py      # Job de transformation
│   ├── spark_curate.py         # Job de curation
│   ├── spark_pipeline.py       # Pipeline complet
│   ├── ingest.py               # (Python standard)
│   ├── transform.py            # (Python standard)
│   └── curate.py               # (Python standard)
├── requirements.txt
└── README.md
```

## ⚡ Installation

```bash
# Créer un environnement virtuel
python -m venv venv 
source venv/bin/activate  # Linux/Mac
# ou: venv\Scripts\activate  # Windows

# Installer PySpark
pip install -r requirements.txt
```

> **Note**: Spark nécessite Java 8/11/17. Vérifiez avec `java -version`.

## 🔄 Pipeline Spark

### Exécuter le pipeline complet

```bash
cd jobs
python spark_pipeline.py
```

Ce pipeline orchestre automatiquement :
1. **Ingest** → Lecture des fichiers raw (CSV/JSON)
2. **Transform** → Nettoyage + enrichissement → staging (Parquet)
3. **Curate** → Agrégations + métriques → curated

### Exécuter les jobs individuellement

```bash
# Ingestion uniquement
python spark_ingest.py

# Transformation uniquement
python spark_transform.py

# Curation uniquement
python spark_curate.py
```

## 📊 Fonctionnalités Spark

### Transformations

```python
from spark_transform import transform_category

# Transforme et enrichit les logs
df = transform_category(spark, "logs")
# Ajoute: date, hour, day_of_week, is_success, ip_last_octet...
```

### Métriques générées (Curation)

| Dataset | Description |
|---------|-------------|
| `global_metrics` | Statistiques globales (total, succès, échecs, taux) |
| `user_statistics` | Stats par utilisateur avec ranking |
| `hourly_statistics` | Distribution horaire des connexions |
| `suspicious_ips` | IPs avec taux d'échec élevé |

### Exemple de sortie

```
📊 MÉTRIQUES GLOBALES
+---------------+------------------+--------------+--------------+------------+
|total_attempts |successful_logins |failed_logins |unique_users  |success_rate|
+---------------+------------------+--------------+--------------+------------+
|400            |180               |220           |95            |45.0        |
+---------------+------------------+--------------+--------------+------------+
```

## 🔧 Configuration Spark

Le fichier `spark_config.py` configure :

```python
spark = SparkSession.builder \
    .appName("MiniDataLake") \
    .master("local[*]")           # Utilise tous les cores
    .config("spark.driver.memory", "2g")
    .getOrCreate()
```

## 📦 Formats de données

| Zone | Format | Raison |
|------|--------|--------|
| **Raw** | CSV, JSON | Formats source originaux |
| **Staging** | Parquet | Columnar, compressé, typé |
| **Curated** | Parquet/JSON | Optimisé pour l'analyse |

## 🛠️ Jobs Python Standard

Les jobs Python standard (sans Spark) restent disponibles pour les petits volumes :

```bash
python ingest.py
python transform.py
python curate.py
```

## 📈 Cas d'usage

Ce mini data lake est conçu pour :
- ✅ Apprentissage de Spark
- ✅ Prototypage de pipelines data
- ✅ Analyse de logs d'authentification
- ✅ Détection d'anomalies simples

## 📝 Licence

MIT

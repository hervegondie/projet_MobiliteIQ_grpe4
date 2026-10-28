from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Initialisation de Spark
spark = (
    SparkSession.builder
    .appName("IngestionTravauxQcCSV")
    .getOrCreate()
)

# Même fuseau pour les dates Python et Spark
spark.conf.set("spark.sql.session.timeZone", "UTC")

# 2. Source officielle des travaux routiers MTMD
url = "https://ws.mapserver.transports.gouv.qc.ca/swtq"

params = {
    "service": "wfs",
    "version": "2.0.0",
    "request": "getfeature",
    "typename": "ms:chantiers_mtmdet",
    "outfile": "TravauxRoutiers",
    "outputformat": "csv",
}

# Identifiant unique pour chaque collecte
now = datetime.now(timezone.utc)
run_id = now.strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:8]
ingestion_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

# Chaque exécution possède son propre dossier :
# on ne risque pas de renommer un ancien fichier.
output_path = Path("./ingestion/output_travaux") / f"collecte={run_id}"

df_history = None

try:
    # Le dossier temporaire est supprimé à la fin du traitement.
    with TemporaryDirectory(prefix="travaux_") as temp_dir:
        local_file = Path(temp_dir) / "travaux.csv"

        # 3. Téléchargement
        print("Téléchargement des travaux routiers...")

        response = requests.get(
            url,
            params=params,
            timeout=(15, 120),
        )
        response.raise_for_status()

        # Un service WFS peut renvoyer une erreur XML avec HTTP 200.
        beginning = response.content.lstrip()[:1024].lower()
        content_type = response.headers.get("Content-Type", "").lower()

        if not response.content.strip():
            raise ValueError("La réponse du serveur est vide.")

        if (
            beginning.startswith(b"<")
            or "xml" in content_type
            or "html" in content_type
        ):
            raise ValueError(
                "Le serveur a retourné du XML/HTML au lieu du CSV attendu."
            )

        local_file.write_bytes(response.content)

        # 4. Lecture du CSV
        df_incoming = (
            spark.read
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("multiLine", "true")
            .option("mode", "FAILFAST")
            # Conservation des valeurs sources en texte
            .option("inferSchema", "false")
            .csv(str(local_file))
        )

        # 5. Colonnes de traçabilité
        df_history = (
            df_incoming
            .withColumn(
                "ingestion_timestamp",
                F.to_timestamp(F.lit(ingestion_timestamp)),
            )
            .withColumn(
                "ingestion_date",
                F.to_date("ingestion_timestamp"),
            )
            .withColumn(
                "ingestion_time",
                F.date_format("ingestion_timestamp", "HH:mm:ss"),
            )
            .cache()
        )

        count = df_history.count()
        print(f"Nombre d'enregistrements reçus : {count}")

        # 6. Sauvegarde d'un nouvel instantané historique
        (
            df_history.coalesce(1)
            .write
            .mode("errorifexists")
            .option("header", "true")
            .partitionBy("ingestion_date")
            .csv(str(output_path))
        )

        # 7. Renommage du fichier de cette collecte uniquement
        part_files = list(
            output_path.glob("ingestion_date=*/part-*.csv")
        )

        for part_file in part_files:
            destination = part_file.with_name(
                f"travaux_{run_id}.csv"
            )
            part_file.rename(destination)

        print(f"Collecte enregistrée dans : {output_path}")

finally:
    if df_history is not None:
        df_history.unpersist()

    spark.stop()
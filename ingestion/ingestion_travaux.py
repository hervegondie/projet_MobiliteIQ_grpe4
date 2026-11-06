import os
from datetime import datetime, timezone
from getpass import getpass
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit, unquote, parse_qsl, urlencode
from uuid import uuid4

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# 1. Connexion à Neon
# ============================================================

# Utiliser DATABASE_URL si elle existe, sinon demander la connexion.
# La saisie du terminal est invisible.
connexion = os.environ.get("DATABASE_URL") or getpass(
    "Connexion PostgreSQL Neon : "
)

adresse = urlsplit(connexion.strip())

if adresse.scheme not in ("postgresql", "postgres") or not adresse.hostname:
    raise ValueError("La connexion PostgreSQL est invalide.")

if unquote(adresse.path.lstrip("/")) != "mobiliteIQIngestion":
    raise ValueError(
        "La connexion doit cibler la base mobiliteIQIngestion."
    )

if not adresse.username or not adresse.password:
    raise ValueError(
        "La connexion doit contenir un utilisateur et un mot de passe."
    )

options = dict(parse_qsl(adresse.query))
options.setdefault("sslmode", "require")

# Adapter le nom du paramètre au pilote JDBC PostgreSQL.
if "channel_binding" in options:
    options["channelBinding"] = options.pop("channel_binding")

jdbc_url = (
    f"jdbc:postgresql://{adresse.hostname}:{adresse.port or 5432}"
    f"{adresse.path}?{urlencode(options)}"
)


# ============================================================
# 2. Initialisation de Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("IngestionRtssQcCSV")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.8",
    )
    .getOrCreate()
)

df_history = None

try:
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # ========================================================
    # 3. Source officielle du réseau routier RTSS
    # ========================================================

    url = "https://ws.mapserver.transports.gouv.qc.ca/swtq"

    params = {
        "service": "wfs",
        "version": "2.0.0",
        "request": "getfeature",
        "typename": "ms:bgr_v_sous_route_res_sup_act",
        "outfile": "ReseauRoutier_RTSS",
        "outputformat": "csv",
    }

    now = datetime.now(timezone.utc)

    run_id = (
        now.strftime("%Y%m%d_%H%M%S")
        + "_"
        + uuid4().hex[:8]
    )

    ingestion_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

    # Dossier situé à côté de ce fichier Python.
    dossier_ingestion = Path(__file__).resolve().parent

    output_path = (
        dossier_ingestion
        / "output_route"
        / f"collecte={run_id}"
    )

    # ========================================================
    # 4. Téléchargement du CSV
    # ========================================================

    with TemporaryDirectory(prefix="route_") as temp_dir:
        local_file = Path(temp_dir) / "route.csv"

        print("Téléchargement du réseau routier RTSS...")

        response = requests.get(
            url,
            params=params,
            timeout=(15, 120),
        )
        response.raise_for_status()

        if not response.content.strip():
            raise ValueError("La réponse du serveur est vide.")

        beginning = response.content.lstrip()[:1024].lower()
        content_type = response.headers.get(
            "Content-Type", ""
        ).lower()

        if (
            beginning.startswith(b"<")
            or "xml" in content_type
            or "html" in content_type
        ):
            raise ValueError(
                "Le serveur a retourné du XML/HTML "
                "au lieu du CSV attendu."
            )

        local_file.write_bytes(response.content)

        # ====================================================
        # 5. Lecture du CSV
        # ====================================================

        # Conserver les champs sources en texte.
        df_incoming = (
            spark.read
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("multiLine", "true")
            .option("mode", "FAILFAST")
            .option("inferSchema", "false")
            .csv(str(local_file))
        )

        # Vérifier que le serveur a bien retourné le réseau RTSS.
        colonnes_attendues = {"ide_sous_r", "num_route"}
        colonnes_absentes = colonnes_attendues - set(df_incoming.columns)
        if colonnes_absentes:
            raise ValueError(
                "Le CSV ne correspond pas au réseau RTSS. "
                f"Colonnes absentes : {sorted(colonnes_absentes)}"
            )

        # ====================================================
        # 6. Ajout des colonnes de traçabilité
        # ====================================================

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
                F.date_format(
                    "ingestion_timestamp",
                    "HH:mm:ss",
                ),
            )
            .cache()
        )

        count = df_history.count()

        print(f"Nombre d'enregistrements reçus : {count}")

        if count == 0:
            print(
                "Aucun segment routier reçu : "
                "aucun fichier historique ni insertion dans Neon."
            )

        else:
            # ================================================
            # 7. Sauvegarde CSV locale
            # ================================================

            (
                df_history.coalesce(1)
                .write
                .mode("errorifexists")
                .option("header", "true")
                .partitionBy("ingestion_date")
                .csv(str(output_path))
            )

            # Renommer uniquement le CSV de cette collecte.
            for part_file in output_path.glob(
                "ingestion_date=*/part-*.csv"
            ):
                destination = part_file.with_name(
                    f"route_{run_id}.csv"
                )

                part_file.rename(destination)

            print(f"CSV enregistré dans : {output_path}")

            # ================================================
            # 8. Enregistrement dans Neon
            # ================================================

            # La base mobiliteIQIngestion doit déjà exister.
            # Spark crée la table si elle est absente et si
            # l'utilisateur possède les droits nécessaires.
            print("Enregistrement du réseau RTSS dans Neon...")

            (
                df_history.coalesce(1)
                # La table Bronze public.route existante stocke
                # toutes ses colonnes en TEXT, y compris les dates.
                .select([
                    F.col(column).cast("string").alias(column)
                    for column in df_history.columns
                ])
                .write
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", "public.route")
                .option(
                    "user",
                    unquote(adresse.username),
                )
                .option(
                    "password",
                    unquote(adresse.password),
                )
                .option(
                    "driver",
                    "org.postgresql.Driver",
                )
                .mode("append")
                .save()
            )

            print(
                f"SUCCÈS : {count} lignes enregistrées dans "
                "mobiliteIQIngestion.public.route."
            )

finally:
    # ========================================================
    # 9. Libération des ressources et arrêt de Spark
    # ========================================================

    try:
        if df_history is not None:
            df_history.unpersist()
    finally:
        spark.stop()

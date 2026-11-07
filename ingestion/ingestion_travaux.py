"""Collecte des travaux MTMD vers mobiliteIQIngestion.public.travaux_bruts.

Connexion via DATABASE_URL ou BRONZE_DATABASE_URL, sinon saisie masquée.
--csv-seulement : collecter et enregistrer le CSV sans connexion à Neon.
Les champs métier restent en texte; les métadonnées sont en timestamp/date/texte.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from getpass import getpass
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit
from uuid import uuid4


TABLE_CIBLE = "public.travaux_bruts"
BASE_CIBLE = "mobiliteIQIngestion"
URL_SOURCE = "https://ws.mapserver.transports.gouv.qc.ca/swtq"
PARAMETRES = {
    "service": "wfs",
    "version": "2.0.0",
    "request": "getfeature",
    "typename": "ms:chantiers_mtmdet",
    "outfile": "TravauxRoutiers",
    "outputformat": "csv",
}
COLONNES_SOURCE = [
    "identifiant", "identifiantChantier", "routeAutoroute", "entraveType",
    "debut", "fin", "miseAJour", "identificationDesTravaux", "urlFrancais",
    "texteUrlFrancais", "detoursEtItinerairesFacultatifs", "entrave",
    "entravesLieesAuxChargesEtDimensions", "localisation", "direction",
    "descriptionFrancais", "descriptionAnglais", "couleurLigne", "source",
]


def parametres_connexion():
    connexion = (os.environ.get("BRONZE_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if not connexion:
        if not sys.stdin.isatty():
            raise ValueError("Configurer le secret Actions DATABASE_URL pour Bronze")
        connexion = getpass("Connexion Neon mobiliteIQIngestion : ").strip()
    adresse = urlsplit(connexion)
    if adresse.scheme not in ("postgres", "postgresql") or not adresse.hostname:
        raise ValueError("Utiliser une connexion PostgreSQL Neon valide")
    if unquote(adresse.path.lstrip("/")) != BASE_CIBLE:
        raise ValueError("La connexion doit cibler mobiliteIQIngestion")
    if not adresse.username or not adresse.password:
        raise ValueError("La connexion doit contenir un utilisateur et un mot de passe")
    options = dict(parse_qsl(adresse.query))
    options.setdefault("sslmode", "require")
    if options["sslmode"] not in ("require", "verify-ca", "verify-full"):
        raise ValueError("La connexion Neon doit activer TLS")
    if "channel_binding" in options:
        options["channelBinding"] = options.pop("channel_binding")
    jdbc_url = (
        f"jdbc:postgresql://{adresse.hostname}:{adresse.port or 5432}"
        f"{adresse.path}?{urlencode(options)}"
    )
    return jdbc_url, unquote(adresse.username), unquote(adresse.password)


def normaliser_csv(source, destination):
    """Contrôler la source et écrire un CSV standard avec guillemets doublés."""
    with source.open(encoding="utf-8-sig", newline="") as entree:
        lecteur = csv.reader(entree, strict=True)
        colonnes = next(lecteur, [])
        if len(colonnes) != len(set(colonnes)) or set(colonnes) != set(COLONNES_SOURCE):
            raise ValueError(
                "Le CSV reçu ne correspond pas aux travaux attendus. "
                f"Colonnes absentes : {sorted(set(COLONNES_SOURCE)-set(colonnes))}; "
                f"colonnes nouvelles : {sorted(set(colonnes)-set(COLONNES_SOURCE))}"
            )
        positions = [colonnes.index(nom) for nom in COLONNES_SOURCE]
        count = 0
        with destination.open("w", encoding="utf-8", newline="") as sortie:
            ecrivain = csv.writer(sortie, doublequote=True)
            ecrivain.writerow(COLONNES_SOURCE)
            for number, row in enumerate(lecteur, start=2):
                if len(row) != len(colonnes):
                    raise ValueError(f"CSV invalide à l'enregistrement {number} : nombre de champs incorrect")
                ecrivain.writerow([row[i] for i in positions])
                count += 1
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv-seulement", action="store_true", help="Collecter sans écrire dans Neon")
    args = parser.parse_args()
    connexion = None if args.csv_seulement else parametres_connexion()
    import requests
    from pyspark.sql import SparkSession, functions as F

    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:8]
    output_path = Path(__file__).resolve().parent / "output_travaux" / f"collecte={run_id}"

    print("Téléchargement des travaux routiers MTMD...")
    response = requests.get(URL_SOURCE, params=PARAMETRES, timeout=(15, 120))
    response.raise_for_status()
    contenu = response.content
    content_type = response.headers.get("Content-Type", "").lower()
    if not contenu.strip():
        raise ValueError("Réponse MTMD vide")
    if contenu.lstrip(b"\xef\xbb\xbf \r\n\t").startswith(b"<") or "xml" in content_type or "html" in content_type:
        raise ValueError("Le serveur a retourné du XML/HTML au lieu du CSV")

    with TemporaryDirectory(prefix="travaux_") as temporary:
        source = Path(temporary) / "travaux_source.csv"
        fichier = Path(temporary) / "travaux.csv"
        source.write_bytes(contenu)
        expected_count = normaliser_csv(source, fichier)
        print(f"Travaux reçus : {expected_count}")
        if expected_count == 0:
            print("Aucun travail reçu : aucune insertion ni suppression dans Neon.")
            return

        spark = (
            SparkSession.builder.appName("IngestionTravauxQcCSV")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.8")
            .getOrCreate()
        )
        df_history = None
        try:
            spark.conf.set("spark.sql.session.timeZone", "UTC")
            spark.sparkContext.setLogLevel("WARN")
            df_incoming = (
                spark.read.option("header", "true").option("encoding", "UTF-8")
                .option("multiLine", "true").option("mode", "FAILFAST")
                .option("inferSchema", "false")
                .option("quote", '"').option("escape", '"')
                .csv(str(fichier)).select(*COLONNES_SOURCE)
            )
            df_history = (
                df_incoming
                .withColumn("ingestion_timestamp", F.to_timestamp(F.lit(now.isoformat())))
                .withColumn("ingestion_date", F.to_date("ingestion_timestamp"))
                .withColumn("ingestion_time", F.date_format("ingestion_timestamp", "HH:mm:ss"))
                .cache()
            )
            count = df_history.count()
            if count != expected_count:
                raise ValueError("Le nombre de lignes Spark ne correspond pas au CSV contrôlé")
            (
                df_history.coalesce(1).write.mode("errorifexists")
                .option("header", "true").option("encoding", "UTF-8")
                .option("quote", '"').option("escape", '"')
                .partitionBy("ingestion_date").csv(str(output_path))
            )
            for part_file in output_path.glob("ingestion_date=*/part-*.csv"):
                part_file.rename(part_file.with_name(f"travaux_{run_id}.csv"))
            print(f"CSV historique enregistré dans : {output_path}")

            if args.csv_seulement:
                print("Vérification réussie. Aucune écriture dans Neon.")
                return

            jdbc_url, utilisateur, mot_de_passe = connexion
            (
                df_history.coalesce(1).write.format("jdbc")
                .option("url", jdbc_url).option("dbtable", TABLE_CIBLE)
                .option("user", utilisateur).option("password", mot_de_passe)
                .option("driver", "org.postgresql.Driver")
                .mode("append").save()
            )
            print(f"SUCCÈS : {count} lignes ajoutées dans {BASE_CIBLE}.{TABLE_CIBLE}.")
        finally:
            try:
                if df_history is not None:
                    df_history.unpersist()
            finally:
                spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        if isinstance(exc, (ValueError, FileNotFoundError, ModuleNotFoundError)):
            print(f"ERREUR : {exc}", file=sys.stderr)
        else:
            print(f"Échec de l'ingestion travaux ({type(exc).__name__}). "
                  "Vérifier la source, la connexion Neon et le schéma; aucun secret n'est affiché.", file=sys.stderr)
        sys.exit(1)

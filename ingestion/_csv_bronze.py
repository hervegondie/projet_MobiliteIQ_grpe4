"""Collecte CSV MTMD et ajout transactionnel dans Bronze, sans Spark ni secret intégré."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from getpass import getpass
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import parse_qs, unquote, urlsplit
from uuid import uuid4

if __package__:
    from ._contrats_csv import COLONNES, SOURCES
else:
    from _contrats_csv import COLONNES, SOURCES

METADONNEES = ["ingestion_timestamp", "ingestion_date", "ingestion_time"]


def connexion_bronze():
    try:
        from dotenv import load_dotenv
    except ImportError:
        pass
    else:
        load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
    value = (os.environ.get("BRONZE_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if not value:
        if not sys.stdin.isatty():
            raise ValueError("Configurer le secret Actions DATABASE_URL pour mobiliteIQIngestion")
        value = getpass("Connexion Neon mobiliteIQIngestion : ").strip()
    address = urlsplit(value)
    if address.scheme not in ("postgres", "postgresql") or not address.hostname:
        raise ValueError("Utiliser la chaîne de connexion PostgreSQL fournie par Neon")
    if unquote(address.path.lstrip("/")) != "mobiliteIQIngestion":
        raise ValueError("La connexion doit cibler mobiliteIQIngestion")
    if parse_qs(address.query).get("sslmode", [""])[0] not in ("require", "verify-ca", "verify-full"):
        raise ValueError("La connexion doit activer TLS avec sslmode=require ou une vérification plus stricte")
    return value


def telecharger_csv(layer, outfile, destination):
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    with requests.Session() as session:
        session.headers.update({"User-Agent": "Mozilla/5.0 MobiliteIQ/Polaris"})
        session.mount("https://", HTTPAdapter(max_retries=Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"], respect_retry_after_header=True)))
        with session.get(
            "https://ws.mapserver.transports.gouv.qc.ca/swtq",
            params={"service": "wfs", "version": "2.0.0", "request": "getfeature",
                    "typename": layer, "outfile": outfile, "outputformat": "csv"},
            timeout=(15, 180), stream=True,
        ) as response:
            response.raise_for_status()
            kind = response.headers.get("Content-Type", "").lower()
            if "xml" in kind or "html" in kind:
                raise ValueError("Le serveur MTMD a retourné du XML/HTML au lieu du CSV")
            with destination.open("wb") as output:
                for block in response.iter_content(chunk_size=1024 * 1024):
                    output.write(block)
    if destination.stat().st_size == 0:
        raise ValueError("CSV MTMD vide")


def normaliser_csv(source, destination, columns, collected_at):
    """Ne perdre ni identifiants, ni guillemets, ni lignes avec texte multiligne."""
    if collected_at.utcoffset() is None:
        raise ValueError("L'horodatage de collecte doit préciser un fuseau horaire")
    stamp = collected_at.astimezone(timezone.utc)
    metadata = [stamp.isoformat(), stamp.date().isoformat(), stamp.time().replace(tzinfo=None).isoformat()]
    with source.open(encoding="utf-8-sig", newline="") as incoming:
        reader = csv.reader(incoming, strict=True)
        header = next(reader, [])
        if len(header) != len(set(header)) or set(header) != set(columns):
            raise ValueError(
                "Schéma source incompatible. "
                f"Absentes={sorted(set(columns)-set(header))}; nouvelles={sorted(set(header)-set(columns))}"
            )
        positions = [header.index(c) for c in columns]
        with destination.open("w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output, doublequote=True)
            writer.writerow(columns + METADONNEES)
            count = 0
            for number, row in enumerate(reader, start=2):
                if len(row) != len(header):
                    raise ValueError(f"Nombre de champs incorrect à l'enregistrement {number}")
                writer.writerow([row[i] for i in positions] + metadata)
                count += 1
    if count == 0:
        raise ValueError("Source de référence sans données : aucun chargement Bronze")
    return count


def importer_csv(path, table, columns, connection, expected_count):
    import psycopg
    from psycopg import sql

    if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", table):
        raise ValueError("Nom de table PostgreSQL invalide")
    names = columns + METADONNEES
    with psycopg.connect(connection, connect_timeout=30) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("mobiliteiq-bronze:" + table,))
        conn.execute(sql.SQL("CREATE TABLE IF NOT EXISTS public.{} ({})").format(
            sql.Identifier(table), sql.SQL(", ").join(
                sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in names)))
        actual = dict(conn.execute(
            "SELECT column_name,data_type FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s", (table,)).fetchall())
        allowed = {c: {"text"} for c in names}
        allowed["ingestion_timestamp"].update({"timestamp with time zone", "timestamp without time zone"})
        allowed["ingestion_date"].add("date")
        if set(actual) != set(names) or any(actual[c] not in allowed[c] for c in names):
            raise ValueError(f"public.{table}: schéma incompatible; aucune table remplacée")
        statement = sql.SQL("COPY public.{} ({}) FROM STDIN").format(
            sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, names)))
        copied = 0
        with path.open(encoding="utf-8-sig", newline="") as incoming, conn.cursor() as cursor:
            reader = csv.reader(incoming, strict=True)
            if next(reader, []) != names:
                raise ValueError("En-tête du CSV préparé incorrect")
            with cursor.copy(statement) as copy:
                for row in reader:
                    if len(row) != len(names):
                        raise ValueError("Le CSV préparé contient une ligne invalide")
                    copy.write_row(tuple(None if cell == "" else cell for cell in row))
                    copied += 1
        if copied != expected_count:
            raise ValueError("Nombre de lignes copié incorrect : transaction annulée")
    return copied


def executer(source):
    cfg = SOURCES[source]
    parser = argparse.ArgumentParser(description=f"Ingestion {source} vers Bronze public.{cfg['table']}")
    parser.add_argument("--csv-seulement", action="store_true", help="Préparer les fichiers sans écrire dans Neon")
    parser.add_argument("--csv", type=Path, help="Utiliser un CSV MTMD source local sans téléchargement")
    parser.add_argument("--output-dir", type=Path,
                        default=Path(__file__).resolve().parent / cfg["output"])
    args = parser.parse_args()
    connection = None if args.csv_seulement else connexion_bronze()
    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:8]
    folder = args.output_dir / f"collecte={run_id}"
    folder.mkdir(parents=True, exist_ok=False)
    raw = folder / "source.csv"
    normalized = folder / f"{source}_{run_id}.csv"
    if args.csv:
        import shutil
        shutil.copyfile(args.csv, raw)
    else:
        print(f"Téléchargement {source} : {cfg['layer']}...")
        telecharger_csv(cfg["layer"], cfg["outfile"], raw)
    count = normaliser_csv(raw, normalized, COLONNES[source], now)
    manifest = {"source": source, "layer": cfg["layer"], "collected_at_utc": now.isoformat(),
                "table_bronze": "public." + cfg["table"], "rows": count,
                "raw_file": raw.name, "csv_file": normalized.name,
                "geometry_included": False, "format": "MTMD CSV attributaire"}
    (folder / "collecte.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.csv_seulement:
        print(f"Vérification réussie : {count} lignes, aucune écriture Neon. CSV : {normalized}")
    else:
        copied = importer_csv(normalized, cfg["table"], COLONNES[source], connection, count)
        print(f"SUCCÈS : {copied} lignes ajoutées dans mobiliteIQIngestion.public.{cfg['table']}.")
    return count


def lancer(source):
    try:
        executer(source)
    except Exception as exc:
        if isinstance(exc, (ValueError, FileNotFoundError, ModuleNotFoundError)):
            print(f"ERREUR : {exc}", file=sys.stderr)
        else:
            print(f"Échec ingestion {source} ({type(exc).__name__}); "
                  "aucun secret n'est affiché. Vérifier source, connexion et droits Neon.", file=sys.stderr)
        sys.exit(1)

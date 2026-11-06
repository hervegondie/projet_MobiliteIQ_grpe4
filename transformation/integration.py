"""Connexions, contrats et lecture des sources Bronze. Collecte des débits optionnelle."""
from __future__ import annotations

from contextlib import contextmanager
import csv
from datetime import datetime, timezone
from getpass import getpass
import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from urllib.parse import unquote, urlsplit, parse_qs
from urllib.request import Request, urlopen

if __package__:
    from .nettoyage import COMMON_TYPES
else:
    from nettoyage import COMMON_TYPES

SOURCES = {"meteo": "meteo", "route": "routes", "travaux_bruts": "travaux",
           "circulation": "circulation"}


CONTRACT = {
    "circulation": [
        "ide_sectn_trafc",
        "num_sectn_trafc",
        "des_debut_sous_route",
        "des_fin_sous_route",
        "rtss_debut_chaing",
        "rtss_fin_chaing",
        "annee_en_cours",
        "annee2",
        "annee3",
        "annee4",
        "annee5",
        "annee6",
        "annee7",
        "annee8",
        "annee9",
        "annee10",
        "dat_debut_sectn_trafc",
        "rtss_debut",
        "val_chang_debut",
        "rtss_fin",
        "val_chang_fin",
        "djma_annee_1",
        "val_djma_annee_1",
        "djma_annee_2",
        "val_djma_annee_2",
        "djma_annee_3",
        "val_djma_annee_3",
        "djma_annee_4",
        "val_djma_annee_4",
        "djma_annee_5",
        "val_djma_annee_5",
        "djma_annee_6",
        "val_djma_annee_6",
        "djma_annee_7",
        "val_djma_annee_7",
        "djma_annee_8",
        "val_djma_annee_8",
        "djma_annee_9",
        "val_djma_annee_9",
        "djma_annee_10",
        "val_djma_annee_10",
        "djme_annee_1",
        "val_djme_annee_1",
        "djme_annee_2",
        "val_djme_annee_2",
        "djme_annee_3",
        "val_djme_annee_3",
        "djme_annee_4",
        "val_djme_annee_4",
        "djme_annee_5",
        "val_djme_annee_5",
        "djme_annee_6",
        "val_djme_annee_6",
        "djme_annee_7",
        "val_djme_annee_7",
        "djme_annee_8",
        "val_djme_annee_8",
        "djme_annee_9",
        "val_djme_annee_9",
        "djme_annee_10",
        "val_djme_annee_10",
        "djmh_annee_1",
        "val_djmh_annee_1",
        "djmh_annee_2",
        "val_djmh_annee_2",
        "djmh_annee_3",
        "val_djmh_annee_3",
        "djmh_annee_4",
        "val_djmh_annee_4",
        "djmh_annee_5",
        "val_djmh_annee_5",
        "djmh_annee_6",
        "val_djmh_annee_6",
        "djmh_annee_7",
        "val_djmh_annee_7",
        "djmh_annee_8",
        "val_djmh_annee_8",
        "djmh_annee_9",
        "val_djmh_annee_9",
        "djmh_annee_10",
        "val_djmh_annee_10",
        "cam_annee_1",
        "val_cam_annee_1",
        "cam_annee_2",
        "val_cam_annee_2",
        "cam_annee_3",
        "val_cam_annee_3",
        "cam_annee_4",
        "val_cam_annee_4",
        "cam_annee_5",
        "val_cam_annee_5",
        "cam_annee_6",
        "val_cam_annee_6",
        "cam_annee_7",
        "val_cam_annee_7",
        "cam_annee_8",
        "val_cam_annee_8",
        "cam_annee_9",
        "val_cam_annee_9",
        "cam_annee_10",
        "val_cam_annee_10",
        "val_30e_heure",
        "index_agreg",
        "index_sectn",
        "index_donnees",
        "objectid",
        "url_index_agregees",
        "url_index_section",
        "url_index_donnees"
    ],
    "meteo": [
        "date",
        "temperature_max",
        "temperature_min",
        "precipitation_totale",
        "pluie",
        "neige",
        "vitesse_vent_max",
        "rafales_vent_max",
        "visibilite_moyenne",
        "ingestion_timestamp"
    ],
    "route": [
        "ide_infrm_",
        "ide_sous_r",
        "num_rts",
        "num_route",
        "num_tronc",
        "num_sectn",
        "cod_sous_r",
        "seq_sous_r",
        "cod_sous_c",
        "cod_cote_c",
        "chang_debu",
        "val_longr_",
        "des_sourc_",
        "sta_sous_r",
        "cod_clasf_",
        "des_clasf_",
        "nom_sous_r",
        "ide_orgns_",
        "nom_orgns_",
        "ide_orgn00",
        "nom_orgn00",
        "cod_unite_",
        "val_locls_",
        "des_unite_",
        "des_type_r",
        "val_repr_d",
        "val_locl00",
        "des_type00",
        "val_repr00",
        "dat_debut_",
        "dah_cretn",
        "objectid",
        "ingestion_timestamp",
        "ingestion_date",
        "ingestion_time"
    ],
    "travaux_bruts": [
        "identifiant",
        "identifiantChantier",
        "routeAutoroute",
        "entraveType",
        "debut",
        "fin",
        "miseAJour",
        "identificationDesTravaux",
        "urlFrancais",
        "texteUrlFrancais",
        "detoursEtItinerairesFacultatifs",
        "entrave",
        "entravesLieesAuxChargesEtDimensions",
        "localisation",
        "direction",
        "descriptionFrancais",
        "descriptionAnglais",
        "couleurLigne",
        "source",
        "ingestion_timestamp",
        "ingestion_date",
        "ingestion_time"
    ]
}


def connection_url(name, database):
    value = os.environ.get(name, "")
    if not value and name == "BRONZE_DATABASE_URL":
        value = os.environ.get("DATABASE_URL", "")
    if not value:
        value = getpass(f"Connexion Neon {database} : ")
    value = value.strip()
    address = urlsplit(value)
    if address.scheme not in ("postgres", "postgresql") or not address.hostname:
        raise ValueError(f"Variable {name} absente ou invalide")
    if unquote(address.path.lstrip("/")) != database:
        raise ValueError(f"{name} doit cibler la base {database}")
    mode = parse_qs(address.query).get("sslmode", [""])[0]
    if mode not in ("require", "verify-ca", "verify-full"):
        raise ValueError(f"{name}: sslmode=require (ou vérification plus stricte) obligatoire")
    return value


def load_contract():
    return {table: list(columns) for table, columns in CONTRACT.items()}


def inspect_source(conn, source, expected, table=None):
    table = table or source
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position", (table,)
    ).fetchall()
    columns = [r[0] for r in rows]
    # Les scripts Bronze historiques ne sauvegardent pas tous les champs de collecte.
    optional = set(COMMON_TYPES) if source == "circulation" else set()
    if source == "circulation" and "temperature_max" in columns:
        raise ValueError(f"public.{table} contient encore des colonnes météo. "
                         "Réingérer les débits MTMD dans Bronze avant la transformation. "
                         "Aucune écriture dans Silver n'a été effectuée.")
    actual_business = set(columns) - optional
    if actual_business != set(expected):
        raise ValueError(f"Schéma Bronze public.{table} différent du contrat : "
                         f"absentes={sorted(set(expected)-actual_business)}, "
                         f"nouvelles={sorted(actual_business-set(expected))}")
    return columns


def ingest_circulation(bronze_url, table, psycopg, sql):
    """Collecte optionnelle, dans une transaction Bronze indépendante de Silver."""
    url = (
        "https://ws.mapserver.transports.gouv.qc.ca/swtq?service=wfs&version=2.0.0"
        "&request=getfeature&typename=ms:circulation_routier"
        "&outfile=DebitCirculation&outputformat=csv"
    )
    print(f"Téléchargement des vrais débits MTMD vers mobiliteIQIngestion.public.{table}...")
    with TemporaryDirectory(prefix="mobiliteiq_debits_") as temporary:
        path = Path(temporary) / "debits.csv"
        request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(request, timeout=120) as response, path.open("wb") as output:
            content_type = response.headers.get("Content-Type", "").lower()
            if "html" in content_type or "xml" in content_type:
                raise ValueError("Le serveur MTMD a retourné du HTML/XML au lieu du CSV")
            shutil.copyfileobj(response, output)
        # Valider intégralement le fichier avant toute création de table Bronze.
        with path.open(encoding="utf-8-sig", newline="") as source:
            reader = csv.reader(source, strict=True)
            headers = next(reader, [])
            if len(set(headers)) != len(headers) or set(headers) != set(CONTRACT["circulation"]):
                raise ValueError("Le CSV MTMD ne correspond pas aux 109 champs de débits attendus")
            count = 0
            for number, row in enumerate(reader, start=2):
                if len(row) != len(headers):
                    raise ValueError(f"CSV MTMD invalide : nombre de champs à l'enregistrement {number}")
                count += 1
            if not count:
                raise ValueError("CSV MTMD sans données; aucune insertion Bronze")
        names = headers + list(COMMON_TYPES)
        now = datetime.now(timezone.utc)
        metadata = (now.isoformat(), now.date().isoformat(), now.time().replace(tzinfo=None).isoformat())
        with psycopg.connect(bronze_url, connect_timeout=20) as conn:
            conn.execute("SELECT pg_advisory_xact_lock(740221106)")
            conn.execute(sql.SQL("CREATE TABLE IF NOT EXISTS public.{} ({})").format(
                sql.Identifier(table), sql.SQL(", ").join(
                    sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in names)))
            actual = dict(conn.execute(
                "SELECT column_name,data_type FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name=%s", (table,)).fetchall())
            if actual != {c: "text" for c in names}:
                raise ValueError(f"public.{table}: schéma existant incompatible, aucune table remplacée")
            statement = sql.SQL("COPY public.{} ({}) FROM STDIN").format(
                sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, names)))
            with path.open(encoding="utf-8-sig", newline="") as source, conn.cursor() as cursor:
                reader = csv.reader(source, strict=True)
                next(reader)
                with cursor.copy(statement) as copy:
                    for row in reader:
                        # Tous les champs source restent en texte dans Bronze.
                        copy.write_row(tuple(row) + metadata)
        print(f"SUCCÈS Bronze : {count} lignes ajoutées dans public.{table}. "
              "Les anciennes données circulation sont conservées.")
        print("La collecte Bronze est validée indépendamment de la transformation Silver.")


@contextmanager
def lire_source(conn, source, table, columns):
    """Ouvrir un curseur serveur; sa fermeture est garantie même en cas d'erreur."""
    from psycopg import sql
    from psycopg.rows import dict_row
    with conn.cursor(name="lecture_" + source, row_factory=dict_row) as reader:
        reader.itersize = 1000
        reader.execute(sql.SQL("SELECT {} FROM public.{}").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)), sql.Identifier(table)))
        yield reader

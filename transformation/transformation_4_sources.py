"""Transformation unique des 4 sources MobiliteIQ Bronze -> Silver_test (Python 3.11+).

Installer : python -m pip install 'psycopg[binary]>=3.2,<4' tzdata
Vérifier  : python transformation_4_sources.py
Charger  : python transformation_4_sources.py --apply
Réparer l'ingestion des débits puis vérifier :
python transformation_4_sources.py --ingest-circulation
Connexions : BRONZE_DATABASE_URL (mobiliteIQIngestion, ou DATABASE_URL),
SILVER_DATABASE_URL (Silver_test sur la branche Neon souhaitée).
Si absentes, elles sont demandées avec une saisie masquée.
Sans --apply : aucune écriture Silver. --ingest-circulation écrit explicitement dans Bronze.
Aucun secret n'est intégré au code. La table Bronze circulation d'origine est conservée.
Les tables cibles conservent les observations distinctes, pas uniquement l'état courant.
Fuseaux par défaut des valeurs SANS décalage : métier America/Toronto, collecte UTC.
Les décalages déjà présents sont toujours respectés. Options pour adapter ces fuseaux.
Version modulaire : garder les quatre fichiers Python dans le même dossier."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import re
import sys
from uuid import uuid4
from zoneinfo import ZoneInfo

if __package__:
    from .nettoyage import clean, transform
    from .integration import SOURCES, connection_url, load_contract, inspect_source, ingest_circulation, lire_source
    from .historisation import serialized, fingerprint, prepare_table, initialiser_suivi, enregistrer_lot, enregistrer_bilan
else:
    from nettoyage import clean, transform
    from integration import SOURCES, connection_url, load_contract, inspect_source, ingest_circulation, lire_source
    from historisation import serialized, fingerprint, prepare_table, initialiser_suivi, enregistrer_lot, enregistrer_bilan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", nargs="+", default=list(SOURCES))
    parser.add_argument("--circulation-table", default="circulation_debits_bruts",
                        help="Table de débits dans public Bronze (défaut : circulation_debits_bruts)")
    parser.add_argument("--ingest-circulation", action="store_true",
                        help="Télécharger les débits et les ajouter dans Bronze avant de transformer")
    parser.add_argument("--source-timezone", default="America/Toronto",
                        help="Fuseau des dates métier sans décalage, à confirmer avec la source")
    parser.add_argument("--ingestion-timezone", default="UTC",
                        help="Fuseau des ingestion_timestamp sans décalage")
    parser.add_argument("--apply", action="store_true", help="Autoriser les écritures dans Silver_test")
    args = parser.parse_args()
    if not args.sources or set(args.sources) - SOURCES.keys() or len(set(args.sources)) != len(args.sources):
        parser.error("Sources autorisées, sans répétition : meteo route travaux_bruts circulation")
    if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", args.circulation_table):
        parser.error("--circulation-table doit être un nom de table PostgreSQL simple, sans schéma")
    if args.ingest_circulation and "circulation" not in args.sources:
        parser.error("--ingest-circulation nécessite circulation parmi les sources sélectionnées")
    source_tz, ingestion_tz = ZoneInfo(args.source_timezone), ZoneInfo(args.ingestion_timezone)
    print(f"Sources sélectionnées : {', '.join(args.sources)}. "
          f"Fuseaux des dates sans décalage : métier={args.source_timezone}, "
          f"collecte={args.ingestion_timezone}.", file=sys.stderr)
    bronze_url = connection_url("BRONZE_DATABASE_URL", "mobiliteIQIngestion")
    silver_url = connection_url("SILVER_DATABASE_URL", "Silver_test") if args.apply else None
    import psycopg
    from psycopg import sql
    from psycopg.types.json import Jsonb

    if args.ingest_circulation:
        ingest_circulation(bronze_url, args.circulation_table, psycopg, sql)
    source_tables = {s: args.circulation_table if s == "circulation" else s for s in args.sources}
    contract, summary, run_id = load_contract(), {}, str(uuid4())
    with psycopg.connect(bronze_url, connect_timeout=20) as bronze:
        bronze.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        # Vérifier toutes les sources avant de créer quoi que ce soit.
        columns_by_source = {s: inspect_source(bronze, s, contract[s], source_tables[s]) for s in args.sources}
        destination = psycopg.connect(silver_url, connect_timeout=20) if args.apply else None
        try:
            if destination:
                initialiser_suivi(destination)
            for source, columns in columns_by_source.items():
                counts = Counter(lues=0, valides=0, rejetees=0, inserees=0, deja_presentes=0,
                                 dates_ingestion_completees=0)
                reasons = Counter()
                names, insert = prepare_table(destination, source, columns, sql) if destination else (None, None)
                with lire_source(bronze, source, source_tables[source], columns) as reader:
                    while batch := reader.fetchmany(1000):
                        accepted, rejected = [], []
                        for raw in batch:
                            counts["lues"] += 1
                            values, errors = transform(source, raw, source_tz, ingestion_tz)
                            if not errors and "ingestion_date" in raw and clean(raw["ingestion_date"]) is None and values.get("ingestion_date") is not None:
                                counts["dates_ingestion_completees"] += 1
                            row_hash = fingerprint(source, raw)
                            now = datetime.now(timezone.utc)
                            if errors:
                                counts["rejetees"] += 1
                                reasons.update(errors)
                                if destination:
                                    rejected.append((source, row_hash, Jsonb(json.loads(serialized(raw))),
                                                     Jsonb(errors), run_id, now))
                            else:
                                counts["valides"] += 1
                                if destination:
                                    values.update(_silver_hash=row_hash,
                                                  _silver_raw=Jsonb(json.loads(serialized(raw))),
                                                  _silver_run=run_id, _silver_processed_at=now)
                                    accepted.append(tuple(values[n] for n in names))
                        if destination:
                            inserted, existing = enregistrer_lot(
                                destination, source, names, insert, accepted, rejected)
                            counts["inserees"] += inserted
                            counts["deja_presentes"] += existing
                summary[source] = {**dict(counts), "table_bronze": "public." + source_tables[source],
                                   "motifs": dict(reasons)}
                if counts["lues"] == 0 or counts["valides"] == 0:
                    raise ValueError(f"{source}: source vide ou aucune ligne valide; chargement annulé")
            if destination:
                enregistrer_bilan(destination, run_id, summary)
                destination.commit()
        except BaseException:
            if destination:
                destination.rollback()
            raise
        finally:
            if destination:
                destination.close()
    print(json.dumps({"mode": "chargement" if args.apply else "verification_sans_ecriture",
                      "version": "4-sources-modulaire-1",
                      "base_cible": "Silver_test",
                      "run_id": run_id, "resultats": summary}, ensure_ascii=False, indent=2))
    if any(s["rejetees"] for s in summary.values()):
        print("ATTENTION : des lignes sont rejetées; consulter les motifs et la table de rejets.", file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # Ne jamais afficher les messages bruts du pilote : ils peuvent contenir des données sensibles.
        if isinstance(exc, (ValueError, FileNotFoundError, ModuleNotFoundError)):
            print(f"ERREUR : {exc}", file=sys.stderr)
        else:
            print(f"Échec ({type(exc).__name__}). Aucune transaction Silver partielle validée. "
                  "Vérifier connexion, droits et schéma; aucun secret n'est affiché.", file=sys.stderr)
        sys.exit(1)

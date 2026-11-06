"""Empreintes, tables Silver, chargement des observations, rejets et suivi des exécutions."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json

if __package__:
    from .integration import SOURCES
    from .nettoyage import target_types
else:
    from integration import SOURCES
    from nettoyage import target_types

META = {"_silver_hash": "text", "_silver_raw": "jsonb",
        "_silver_run": "text", "_silver_processed_at": "timestamp with time zone"}


def serialized(raw):
    return json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False)


def fingerprint(source, raw):
    # Inclut les métadonnées Bronze : deux collectes restent deux observations.
    return hashlib.sha256((source + "\n" + serialized(raw)).encode("utf-8")).hexdigest()


def prepare_table(conn, source, columns, sql):
    table = SOURCES[source]
    types = {**target_types(source, columns), **META}
    definitions = [sql.SQL("{} {}{}").format(
        sql.Identifier(c), sql.SQL(t),
        sql.SQL(" PRIMARY KEY" if c == "_silver_hash" else "")) for c, t in types.items()]
    conn.execute(sql.SQL("CREATE TABLE IF NOT EXISTS public.{} ({})").format(
        sql.Identifier(table), sql.SQL(", ").join(definitions)))
    actual = dict(conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s", (table,)).fetchall())
    if actual != types:
        raise ValueError(f"Schéma Silver {table} incompatible : migration explicite nécessaire")
    keys = conn.execute(
        "SELECT a.attname FROM pg_constraint c "
        "JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace "
        "JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=ANY(c.conkey) "
        "WHERE n.nspname='public' AND t.relname=%s AND c.contype='p'", (table,)
    ).fetchall()
    if keys != [("_silver_hash",)]:
        raise ValueError(f"Clé primaire incompatible dans Silver {table}")
    names = list(types)
    statement = sql.SQL("INSERT INTO public.{} ({}) VALUES ({}) "
                        "ON CONFLICT (_silver_hash) DO NOTHING").format(
        sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, names)),
        sql.SQL(", ").join(sql.Placeholder() for _ in names))
    return names, statement


def initialiser_suivi(destination):
    destination.execute("SELECT pg_advisory_xact_lock(740221105)")
    destination.execute("CREATE TABLE IF NOT EXISTS public.pipeline_silver_runs "
                        "(run_id text PRIMARY KEY, finished_at timestamptz NOT NULL, summary jsonb NOT NULL)")
    destination.execute("CREATE TABLE IF NOT EXISTS public.pipeline_silver_rejets "
                        "(source_table text NOT NULL, row_hash text NOT NULL, raw_payload jsonb NOT NULL, "
                        "raisons jsonb NOT NULL, run_id text NOT NULL, processed_at timestamptz NOT NULL, "
                        "PRIMARY KEY(source_table, row_hash))")


def enregistrer_lot(destination, source, names, insert, accepted, rejected):
    """Écrire un lot sans commit : le point d'entrée contrôle la transaction globale."""
    inserted, existing = 0, 0
    with destination.cursor() as writer:
        if accepted:
            writer.executemany(insert, accepted)
            inserted = writer.rowcount
            existing = len(accepted) - inserted
        if rejected:
            writer.executemany(
                "INSERT INTO public.pipeline_silver_rejets VALUES (%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (source_table,row_hash) DO UPDATE SET "
                "raisons=EXCLUDED.raisons, run_id=EXCLUDED.run_id, "
                "processed_at=EXCLUDED.processed_at", rejected)
    if accepted:
        destination.execute(
            "DELETE FROM public.pipeline_silver_rejets WHERE source_table=%s AND row_hash=ANY(%s)",
            (source, [row[names.index("_silver_hash")] for row in accepted]))
    return inserted, existing


def enregistrer_bilan(destination, run_id, summary):
    from psycopg.types.json import Jsonb
    destination.execute("INSERT INTO public.pipeline_silver_runs VALUES (%s,%s,%s)",
                        (run_id, datetime.now(timezone.utc), Jsonb(summary)))

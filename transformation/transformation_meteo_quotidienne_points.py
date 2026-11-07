"""Chargement atomique du lot quotidien géolocalisé et contrôle après chargement."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from uuid import UUID

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ingestion.points_meteo import verifier_lot, lire_manifest, verifier_manifest_enveloppes
from transformation.integration_meteo_horaire import BASE, REJETS, connexion, inserer, table
from transformation.nettoyage import METEO_NUMBERS
from transformation.nettoyage_meteo_quotidienne_points import transformer
from transformation.transformation_meteo_horaire import empreinte

COLONNES = {**{k: v for k, v in BASE.items() if k != 'heure_meteo'},
            'date': 'date', **{k: 'numeric' for k in METEO_NUMBERS}}
RUNS = dict(collecte_id='uuid', lieu_id='text', collecte_at='timestamp with time zone',
            processed_at='timestamp with time zone', payload_hash='text', nb_jours='integer')


def initialiser(conn):
    conn.execute('SELECT pg_advisory_xact_lock(740221108)')
    table(conn, 'meteo_quotidienne_points', COLONNES, ('collecte_id', 'date'))
    table(conn, 'meteo_quotidienne_points_runs', RUNS, ('collecte_id',))
    table(conn, 'meteo_quotidienne_points_rejets', REJETS, ('collecte_id',))
    conn.execute('CREATE INDEX IF NOT EXISTS meteo_quotidienne_points_lieu_date_idx '
                 'ON public.meteo_quotidienne_points (lieu_id,date,collecte_at DESC)')
    conn.execute('CREATE OR REPLACE VIEW public.v_meteo_quotidienne_points_dernieres AS '
                 'SELECT DISTINCT ON (lieu_id,date) * FROM public.meteo_quotidienne_points '
                 'ORDER BY lieu_id,date,collecte_at DESC,collecte_id DESC')


def lire_bronze(identifiant):
    with connexion('bronze', readonly=True) as conn:
        row = conn.execute('SELECT lieu_id,ingestion_timestamp,payload FROM public.meteo_quotidienne_points_bruts '
                           'WHERE collecte_id=%s', (UUID(str(identifiant)),)).fetchone()
    if row is None:
        raise ValueError('Collecte quotidienne absente de Bronze')
    payload = row[2]
    if (payload.get('collecte_id') != str(identifiant) or payload.get('lieu_id') != row[0]
            or datetime.fromisoformat(payload['ingestion_timestamp']) != row[1]):
        raise ValueError('Metadonnees quotidiennes Bronze incoherentes')
    return payload


def verifier_sql(conn, identifiant, expected):
    n, distinct = conn.execute('SELECT count(*),count(DISTINCT date) FROM public.meteo_quotidienne_points '
                               'WHERE collecte_id=%s', (identifiant,)).fetchone()
    if n != expected or distinct != expected:
        raise ValueError('Collecte quotidienne Silver incomplete ou dates dupliquees')


def charger_lot(envelopes):
    from psycopg.types.json import Jsonb
    verifier_lot(envelopes)
    prepared, errors = [], []
    for envelope in envelopes:
        try:
            prepared.append((envelope, transformer(envelope)))
        except ValueError as exc:
            errors.append((UUID(envelope['collecte_id']), str(exc)))
    if errors:
        with connexion('silver') as conn:
            conn.execute('SELECT pg_advisory_xact_lock(740221108)')
            table(conn, 'meteo_quotidienne_points_rejets', REJETS, ('collecte_id',))
            for identifiant, reason in errors:
                conn.execute('INSERT INTO public.meteo_quotidienne_points_rejets VALUES (%s,%s,%s) '
                             'ON CONFLICT(collecte_id) DO UPDATE SET raisons=EXCLUDED.raisons,processed_at=EXCLUDED.processed_at',
                             (identifiant, Jsonb([reason]), datetime.now(timezone.utc)))
        raise ValueError('Lot quotidien refuse : ' + '; '.join(reason for _, reason in errors))
    results = []
    with connexion('silver') as conn:
        initialiser(conn)
        for envelope, rows in prepared:
            identifiant, digest = UUID(envelope['collecte_id']), empreinte(envelope)
            old = conn.execute('SELECT payload_hash,nb_jours FROM public.meteo_quotidienne_points_runs '
                               'WHERE collecte_id=%s', (identifiant,)).fetchone()
            if old:
                if old != (digest, len(rows)):
                    raise ValueError('Une collecte quotidienne deja traitee a change de contenu')
                inserted = 0
            else:
                inserted = inserer(conn, 'meteo_quotidienne_points', COLONNES, rows)
                if inserted != len(rows):
                    raise ValueError('Insertion quotidienne partielle : transaction annulee')
                inserer(conn, 'meteo_quotidienne_points_runs', RUNS, [dict(collecte_id=identifiant,
                        lieu_id=envelope['lieu_id'], collecte_at=rows[0]['collecte_at'],
                        processed_at=datetime.now(timezone.utc), payload_hash=digest, nb_jours=len(rows))])
            verifier_sql(conn, identifiant, len(rows))
            conn.execute('DELETE FROM public.meteo_quotidienne_points_rejets WHERE collecte_id=%s', (identifiant,))
            results.append(dict(collecte_id=str(identifiant), lieu_id=envelope['lieu_id'],
                                jours=len(rows), inserees=inserted, deja_traitee=bool(old)))
    return results


def verifier(envelope):
    rows = transformer(envelope)
    identifiant = UUID(envelope['collecte_id'])
    age = (datetime.now(timezone.utc) - rows[0]['collecte_at']).total_seconds()
    if not -300 <= age <= 36 * 3600:
        raise ValueError('Collecte quotidienne datant de plus de 36 heures ou dans le futur')
    with connexion('silver', readonly=True) as conn:
        audit = conn.execute('SELECT payload_hash,nb_jours FROM public.meteo_quotidienne_points_runs '
                             'WHERE collecte_id=%s', (identifiant,)).fetchone()
        if audit != (empreinte(envelope), len(rows)):
            raise ValueError('Audit quotidien absent ou incompatible')
        verifier_sql(conn, identifiant, len(rows))
        if conn.execute('SELECT count(*) FROM public.meteo_quotidienne_points_rejets WHERE collecte_id=%s',
                        (identifiant,)).fetchone()[0]:
            raise ValueError('Rejet quotidien present pour cette collecte')
    return dict(statut='OK', collecte_id=str(identifiant), lieu_id=envelope['lieu_id'], jours=len(rows))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--manifest', type=Path)
    source.add_argument('--collecte-id', type=UUID)
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--apply', action='store_true')
    action.add_argument('--verifier', action='store_true')
    parser.add_argument('--rapport', type=Path, default=Path('reports/transformation_meteo_quotidienne_points.json'))
    args = parser.parse_args()
    pairs = lire_manifest(args.manifest, 'quotidienne') if args.manifest else [(None, args.collecte_id)]
    envelopes = [lire_bronze(identifiant) for _, identifiant in pairs]
    verifier_manifest_enveloppes(pairs, envelopes)
    if args.apply:
        results = charger_lot(envelopes)
        report = dict(mode='chargement_silver', points=len(envelopes), collectes=results,
                      jours=sum(r['jours'] for r in results), inserees=sum(r['inserees'] for r in results))
    elif args.verifier:
        results = [verifier(e) for e in envelopes]
        report = dict(mode='controle_sans_ecriture', statut='OK', points=len(envelopes), collectes=results,
                      jours=sum(r['jours'] for r in results))
    else:
        report = dict(mode='verification_sans_ecriture_silver', points=len(envelopes),
                      jours=sum(len(transformer(e)) for e in envelopes))
    args.rapport.parent.mkdir(parents=True, exist_ok=True)
    args.rapport.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        message = str(exc) if isinstance(exc, ValueError) else f'Echec meteo quotidienne multi-points ({type(exc).__name__})'
        print(f'::error::{message}', file=sys.stderr)
        sys.exit(1)

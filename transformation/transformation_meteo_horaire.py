"""Traiter une collecte précise : Bronze -> actuelle + prévisions Silver, atomiquement."""
from datetime import datetime, timezone
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from uuid import UUID

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from transformation.nettoyage_meteo_horaire import transformer
from ingestion.points_meteo import verifier_lot, lire_manifest, verifier_manifest_enveloppes
from transformation.integration_meteo_horaire import (ACTUELLE, PREVISIONS, RUNS, REJETS,
    connexion, initialiser_silver, inserer, table)


def empreinte(envelope):
    return hashlib.sha256(json.dumps(envelope, sort_keys=True, ensure_ascii=False,
                                    allow_nan=False).encode('utf-8')).hexdigest()


def lire_bronze(identifiant):
    with connexion('bronze', readonly=True) as conn:
        row = conn.execute('SELECT lieu_id,ingestion_timestamp,payload FROM public.meteo_horaire_bruts '
                           'WHERE collecte_id=%s', (UUID(str(identifiant)),)).fetchone()
    if row is None:
        raise ValueError('Collecte absente de public.meteo_horaire_bruts')
    payload = row[2]
    if (payload.get('collecte_id') != str(identifiant) or payload.get('lieu_id') != row[0]
            or datetime.fromisoformat(payload['ingestion_timestamp']) != row[1]):
        raise ValueError('Metadonnees Bronze incoherentes avec le JSON brut')
    return payload


def verifier_sql(conn, identifiant, expected):
    actuelle = conn.execute('SELECT count(*) FROM public.meteo_actuelle WHERE collecte_id=%s', (identifiant,)).fetchone()[0]
    count, distinct = conn.execute('SELECT count(*),count(DISTINCT heure_meteo) FROM public.meteo_previsions_horaires '
                                   'WHERE collecte_id=%s', (identifiant,)).fetchone()
    if actuelle != 1 or count != expected or distinct != expected:
        raise ValueError('Collection Silver incomplete ou heures dupliquees')
    return actuelle, count


def _charger_connexion(conn, envelope, actuelle, forecasts):
    identifiant = UUID(envelope['collecte_id'])
    digest = empreinte(envelope)
    old = conn.execute('SELECT payload_hash,nb_actuelles,nb_previsions FROM public.meteo_horaire_runs WHERE collecte_id=%s',
                       (identifiant,)).fetchone()
    if old:
        if old != (digest, 1, len(forecasts)):
            raise ValueError('Une collecte deja traitee a change de contenu')
        verifier_sql(conn, identifiant, len(forecasts))
        conn.execute('DELETE FROM public.meteo_horaire_rejets WHERE collecte_id=%s', (identifiant,))
        return dict(collecte_id=str(identifiant), lieu_id=envelope['lieu_id'], actuelles_inserees=0, previsions_inserees=0,
                    deja_traitee=True, actuelles=1, previsions=len(forecasts))
    inserted_current = inserer(conn, 'meteo_actuelle', ACTUELLE, [actuelle])
    inserted_forecasts = inserer(conn, 'meteo_previsions_horaires', PREVISIONS, forecasts)
    if inserted_current != 1 or inserted_forecasts != len(forecasts):
        raise ValueError('Insertion Silver partielle : transaction annulee')
    verifier_sql(conn, identifiant, len(forecasts))
    inserer(conn, 'meteo_horaire_runs', RUNS, [dict(collecte_id=identifiant, lieu_id=actuelle['lieu_id'],
             collecte_at=actuelle['collecte_at'], processed_at=datetime.now(timezone.utc),
             payload_hash=digest, nb_actuelles=1, nb_previsions=len(forecasts))])
    conn.execute('DELETE FROM public.meteo_horaire_rejets WHERE collecte_id=%s', (identifiant,))
    return dict(collecte_id=str(identifiant), lieu_id=envelope['lieu_id'], actuelles_inserees=inserted_current,
            previsions_inserees=inserted_forecasts, deja_traitee=False, actuelles=1, previsions=len(forecasts))



def charger_lot(envelopes):
    from psycopg.types.json import Jsonb
    verifier_lot(envelopes)
    prepared, errors = [], []
    for envelope in envelopes:
        try:
            actuelle, forecasts = transformer(envelope)
            prepared.append((envelope, actuelle, forecasts))
        except ValueError as exc:
            errors.append((UUID(envelope['collecte_id']), str(exc)))
    if errors:
        with connexion('silver') as conn:
            conn.execute('SELECT pg_advisory_xact_lock(740221106)')
            table(conn, 'meteo_horaire_rejets', REJETS, ('collecte_id',))
            for identifiant, reason in errors:
                conn.execute('INSERT INTO public.meteo_horaire_rejets VALUES (%s,%s,%s) '
                             'ON CONFLICT(collecte_id) DO UPDATE SET raisons=EXCLUDED.raisons,processed_at=EXCLUDED.processed_at',
                             (identifiant, Jsonb([reason]), datetime.now(timezone.utc)))
        raise ValueError('Lot horaire refuse : ' + '; '.join(reason for _, reason in errors))
    with connexion('silver') as conn:
        initialiser_silver(conn)
        return [_charger_connexion(conn, envelope, actuelle, forecasts)
                for envelope, actuelle, forecasts in prepared]


def charger(envelope):
    return charger_lot([envelope])[0]


def verifier(envelope):
    actuelle, forecasts = transformer(envelope)
    age = (datetime.now(timezone.utc) - actuelle['collecte_at']).total_seconds()
    if not -300 <= age <= 7200:
        raise ValueError('Collecte datant de plus de deux heures ou dans le futur')
    with connexion('silver', readonly=True) as conn:
        audit = conn.execute('SELECT payload_hash,nb_actuelles,nb_previsions FROM public.meteo_horaire_runs '
                             'WHERE collecte_id=%s', (actuelle['collecte_id'],)).fetchone()
        if audit != (empreinte(envelope), 1, len(forecasts)):
            raise ValueError('Audit Silver absent ou incompatible')
        current_count, forecast_count = verifier_sql(conn, actuelle['collecte_id'], len(forecasts))
        if conn.execute('SELECT count(*) FROM public.meteo_horaire_rejets WHERE collecte_id=%s',
                        (actuelle['collecte_id'],)).fetchone()[0]:
            raise ValueError('Rejet present pour cette collecte')
    return dict(statut='OK', collecte_id=envelope['collecte_id'], lieu_id=envelope['lieu_id'], actuelle=current_count,
                previsions=forecast_count, rejets=0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--collecte-id', type=UUID)
    source.add_argument('--manifest', type=Path)
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--apply', action='store_true')
    action.add_argument('--verifier', action='store_true', help='Controle Neon en lecture seule')
    parser.add_argument('--rapport', type=Path, default=Path('reports/transformation_meteo_horaire.json'))
    args = parser.parse_args()
    pairs = lire_manifest(args.manifest, 'horaire') if args.manifest else [(None, args.collecte_id)]
    envelopes = [lire_bronze(identifiant) for _, identifiant in pairs]
    verifier_manifest_enveloppes(pairs, envelopes)
    if args.apply:
        results = charger_lot(envelopes)
        report = dict(mode='chargement_silver', points=len(envelopes), collectes=results,
                      actuelles_inserees=sum(r['actuelles_inserees'] for r in results),
                      previsions_inserees=sum(r['previsions_inserees'] for r in results))
    elif args.verifier:
        results = [verifier(e) for e in envelopes]
        report = dict(mode='controle_sans_ecriture', statut='OK', points=len(envelopes), collectes=results,
                      actuelles=sum(r['actuelle'] for r in results), previsions=sum(r['previsions'] for r in results))
    else:
        counts = [len(transformer(e)[1]) for e in envelopes]
        report = dict(mode='verification_sans_ecriture_silver', points=len(envelopes), previsions=sum(counts))
    args.rapport.parent.mkdir(parents=True, exist_ok=True)
    args.rapport.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a', encoding='utf-8') as f:
            f.write('\n### Meteo horaire\n\n' + json.dumps(report, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        message = str(exc) if isinstance(exc, ValueError) else f'Echec meteo horaire ({type(exc).__name__})'
        print(f'::error::{message}', file=sys.stderr)
        sys.exit(1)

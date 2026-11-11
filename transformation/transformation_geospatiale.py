"""Publier les geometries Silver et debits de reference ; pas de jointure spatiale forcee."""
from pathlib import Path
from uuid import UUID
import argparse
import json
import sys

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ingestion.contrats_geospatiaux import (
    empreinte, identifiant, references_debits, valider_geometrie, SOURCES_GEO)
from transformation.integration_meteo_horaire import connexion

MIGRATION = Path(__file__).resolve().parents[1] / 'sql/gold/001_prerequis.sql'


def preparer(snapshot):
    source = snapshot['source']
    features = snapshot.get('features')
    if source not in SOURCES_GEO or snapshot.get('srid') != 4326:
        raise ValueError('Source ou SRID invalide')
    if not isinstance(features, list) or snapshot.get('attendu') != len(features):
        raise ValueError('Collecte tronquee : total et nombre de features differents')
    accepted, rejected, seen = [], [], set()
    for feature in features:
        key = identifiant(source, feature)
        if key in seen:
            raise ValueError('Identifiants dupliques dans le lot')
        seen.add(key)
        try:
            valider_geometrie(feature.get('geometry'))
            refs = references_debits(feature['properties']) if source == 'debits' else []
        except ValueError as exc:
            rejected.append((key, str(exc), feature))
        else:
            accepted.append((key, empreinte(feature), feature, refs))
    if not accepted and features:
        raise ValueError('Aucune geometrie valide : la population courante reste inchangee')
    return accepted, rejected


def lire(manifest):
    if manifest.get('version') != 1 or manifest.get('bronze_ecrit') is not True:
        raise ValueError('Manifest Bronze complet obligatoire (pas de --json-seulement)')
    cid = UUID(manifest['collecte_id'])
    specifications = manifest.get('sources', [])
    names = [s['source'] for s in specifications]
    if not names or len(set(names)) != len(names) or not set(names) <= set(SOURCES_GEO):
        raise ValueError('Liste des sources invalide')
    lots = []
    with connexion('bronze', readonly=True) as conn:
        for spec in specifications:
            row = conn.execute('SELECT payload,payload_hash FROM public.geo_collectes_brutes '
                               'WHERE collecte_id=%s AND source=%s', (cid, spec['source'])).fetchone()
            if not row or row[1] != spec['payload_hash'] or empreinte(row[0]) != row[1]:
                raise ValueError('Contenu Bronze absent ou empreinte differente du manifest')
            if row[0].get('attendu') != spec['attendu'] or row[0].get('source') != spec['source']:
                raise ValueError('Manifest et lot Bronze incompatibles')
            lots.append(row[0])
    return cid, lots


def publier(cid, lots, conn):
    from psycopg.types.json import Jsonb
    conn.execute('SELECT pg_advisory_xact_lock(740221202)')
    if not conn.execute("SELECT to_regclass('public.geo_collectes_silver')").fetchone()[0]:
        raise ValueError('Executer --installer sur Silver_test avant --apply')
    bilan = {}
    for snapshot in lots:
        source, h = snapshot['source'], empreinte(snapshot)
        accepted, rejected = preparer(snapshot)
        existing = conn.execute('SELECT payload_hash FROM public.geo_collectes_silver '
                                'WHERE collecte_id=%s AND source=%s', (cid, source)).fetchone()
        if existing:
            if existing[0] != h:
                raise ValueError('Collecte deja publiee avec un contenu different')
            bilan[source] = dict(deja_traitee=True, valides=len(accepted), rejetees=len(rejected))
            continue
        # Transaction unique : audit et lignes ne deviennent visibles qu'ensemble.
        conn.execute('INSERT INTO public.geo_collectes_silver '
            '(collecte_id,source,collecte_at,payload_hash,attendues,valides,rejetees) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s)', (cid,source,snapshot['collecte_at'],h,
             snapshot['attendu'],len(accepted),len(rejected)))
        # executemany utilise le pipeline psycopg : eviter un aller-retour reseau
        # pour chacune des 12600 geometries et 100000+ references.
        with conn.cursor() as cur:
            cur.executemany('INSERT INTO public.geometries_versions '
                '(source,identifiant,empreinte,proprietes,geom) '
                'VALUES (%s,%s,%s,%s,ST_Multi(ST_Force2D(ST_SetSRID(ST_GeomFromGeoJSON(%s),4326)))) '
                'ON CONFLICT DO NOTHING', [(source,key,fh,Jsonb(f['properties']),
                    json.dumps(f['geometry'],allow_nan=False)) for key,fh,f,_ in accepted])
            cur.executemany('INSERT INTO public.geo_observations VALUES (%s,%s,%s,%s)',
                            [(cid,source,key,fh) for key,fh,_,_ in accepted])
            ref_rows = [(key,fh,r['annee_reference'],r['indicateur'],r['valeur'],r['periode'],r['unite'])
                        for key,fh,_,refs in accepted for r in refs]
            if ref_rows:
                cur.executemany('INSERT INTO public.debits_reference_geo '
                    '(identifiant,empreinte,annee_reference,indicateur,valeur,periode,unite) '
                    'VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING', ref_rows)
        if rejected:
            with conn.cursor() as cur:
                cur.executemany('INSERT INTO public.geo_rejets VALUES (%s,%s,%s,%s,%s)',
                    [(cid,source,key,Jsonb([reason]),Jsonb(f)) for key,reason,f in rejected])
        n = conn.execute('SELECT count(*) FROM public.geo_observations WHERE collecte_id=%s '
                         'AND source=%s', (cid,source)).fetchone()[0]
        if n != len(accepted):
            raise ValueError('Nombre de geometries publiees incorrect')
        bilan[source] = dict(deja_traitee=False, valides=n, rejetees=len(rejected),
                             references=sum(len(x[3]) for x in accepted))
    return bilan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, default=Path('reports/collecte_geometries.json'))
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--installer', action='store_true')
    action.add_argument('--apply', action='store_true')
    parser.add_argument('--rapport', type=Path, default=Path('reports/qualite_geometries.json'))
    args = parser.parse_args()
    if args.installer:
        with connexion('silver') as conn:
            conn.execute(MIGRATION.read_text(encoding='utf-8'), prepare=False)
        print('Migration PostGIS et schema gold installes sur Silver_test')
        return
    cid, lots = lire(json.loads(args.manifest.read_text(encoding='utf-8')))
    if args.apply:
        with connexion('silver') as conn:
            conn.execute("SET LOCAL statement_timeout = '15min'")
            result = publier(cid, lots, conn)
    else:
        result = {}
        for snapshot in lots:
            a,r = preparer(snapshot)
            result[snapshot['source']] = dict(valides=len(a),rejetees=len(r))
    report = dict(mode='chargement' if args.apply else 'verification_sans_ecriture_silver',
                  collecte_id=str(cid), base_cible='Silver_test', resultats=result)
    args.rapport.parent.mkdir(parents=True, exist_ok=True)
    args.rapport.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

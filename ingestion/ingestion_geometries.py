"""Collecte WFS paginee -> archives GeoJSON -> table Bronze complementaire."""
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4, UUID
import argparse
import json
import sys
import xml.etree.ElementTree as ET

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ingestion.contrats_geospatiaux import URL_WFS, SOURCES_GEO, empreinte, identifiant
from transformation.integration_meteo_horaire import connexion


def session_http():
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    session = requests.Session()
    session.headers['User-Agent'] = 'Mozilla/5.0 MobiliteIQ/Gold'
    session.mount('https://', HTTPAdapter(max_retries=Retry(
        total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['GET'], respect_retry_after_header=True)))
    return session


def matched(session, source):
    response = session.get(URL_WFS, params=dict(service='wfs', version='2.0.0',
        request='getfeature', typename=SOURCES_GEO[source][0], resulttype='hits'), timeout=(15, 180))
    response.raise_for_status()
    root = ET.fromstring(response.content)
    count = root.get('numberMatched')
    if count is None or not count.isdigit():
        raise ValueError('WFS hits sans nombre total exploitable')
    return int(count)


def collecter(source, session, page_size=500):
    if source not in SOURCES_GEO or not 1 <= page_size <= 5000:
        raise ValueError('Source ou taille de page invalide')
    expected = matched(session, source)
    if not expected and source != 'travaux':
        raise ValueError('Referentiel routes/debits vide')
    features, seen = [], set()
    while len(features) < expected:
        # sortby est refuse par le service routes : ne pas inventer un tri.
        response = session.get(URL_WFS, params=dict(service='wfs', version='2.0.0',
            request='getfeature', typename=SOURCES_GEO[source][0], outputformat='geojson',
            srsname='EPSG:4326', startindex=len(features), count=page_size), timeout=(15, 180))
        response.raise_for_status()
        page = response.json()
        if page.get('type') != 'FeatureCollection' or page.get('crs'):
            raise ValueError('FeatureCollection RFC7946 attendue, sans autre CRS declare')
        batch = page.get('features')
        if not isinstance(batch, list) or not batch:
            raise ValueError('Pagination incomplete : page vide avant le total attendu')
        for feature in batch:
            key = identifiant(source, feature)
            if key in seen:
                raise ValueError('Identifiant duplique : pagination instable/incomplete')
            seen.add(key)
        features.extend(batch)
    if len(features) != expected or matched(session, source) != expected:
        raise ValueError('Le total WFS a change pendant la collecte ; relancer')
    return dict(source=source, couche=SOURCES_GEO[source][0], srid=4326,
                collecte_at=datetime.now(timezone.utc).isoformat(), attendu=expected,
                requete=dict(url=URL_WFS, outputformat='geojson', srsname='EPSG:4326'),
                features=features)


def enregistrer(lot):
    from psycopg.types.json import Jsonb
    with connexion('bronze') as conn:
        conn.execute('SELECT pg_advisory_xact_lock(740221201)')
        conn.execute('CREATE TABLE IF NOT EXISTS public.geo_collectes_brutes ('
                     'collecte_id uuid NOT NULL, source text NOT NULL, collecte_at timestamptz NOT NULL,'
                     'payload_hash text NOT NULL, payload jsonb NOT NULL, PRIMARY KEY(collecte_id,source))')
        for snapshot in lot['sources']:
            h = empreinte(snapshot)
            conn.execute('INSERT INTO public.geo_collectes_brutes VALUES (%s,%s,%s,%s,%s) '
                         'ON CONFLICT DO NOTHING', (UUID(lot['collecte_id']), snapshot['source'],
                         snapshot['collecte_at'], h, Jsonb(snapshot)))
            actual = conn.execute('SELECT payload_hash FROM public.geo_collectes_brutes '
                'WHERE collecte_id=%s AND source=%s', (UUID(lot['collecte_id']), snapshot['source'])).fetchone()[0]
            if actual != h:
                raise ValueError('Identifiant de collecte reutilise avec un contenu different')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sources', nargs='+', choices=SOURCES_GEO, default=list(SOURCES_GEO))
    parser.add_argument('--page-size', type=int, default=500)
    parser.add_argument('--json-seulement', action='store_true')
    parser.add_argument('--manifest', type=Path, default=Path('reports/collecte_geometries.json'))
    args = parser.parse_args()
    if len(set(args.sources)) != len(args.sources):
        parser.error('Sources dupliquees')
    lot = dict(version=1, collecte_id=str(uuid4()), sources=[])
    with session_http() as session:
        for source in args.sources:
            lot['sources'].append(collecter(source, session, args.page_size))
    folder = Path('ingestion/output_geometries') / lot['collecte_id']
    folder.mkdir(parents=True, exist_ok=True)
    for snapshot in lot['sources']:
        (folder / (snapshot['source'] + '.json')).write_text(
            json.dumps(snapshot, ensure_ascii=False, allow_nan=False), encoding='utf-8')
    # Le manifest n'est publie qu'apres validation de toutes les ecritures Bronze.
    if not args.json_seulement:
        enregistrer(lot)
    manifest = dict(version=1, collecte_id=lot['collecte_id'], bronze_ecrit=not args.json_seulement,
                    sources=[dict(source=x['source'], payload_hash=empreinte(x), attendu=x['attendu'])
                             for x in lot['sources']])
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(json.dumps(manifest, indent=2))


if __name__ == '__main__':
    main()

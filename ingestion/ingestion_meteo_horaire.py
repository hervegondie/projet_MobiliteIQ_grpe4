"""Open-Meteo actuelle + prévisions horaires -> JSON brut -> Neon Bronze."""
from datetime import datetime, timezone
from pathlib import Path
import argparse
import json
import sys
from uuid import uuid4, UUID

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from transformation.nettoyage_meteo_horaire import API_URL, parametres, transformer, valider_localisation
from transformation.integration_meteo_horaire import BRONZE, connexion, inserer, table
from ingestion.points_meteo import CONFIG, charger_points, verifier_lot


def collecter(lieu='quebec', latitude=46.8139, longitude=-71.2080, heures=48):
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    valider_localisation(lieu, latitude, longitude, heures)
    params = parametres(latitude, longitude, heures)
    collecte = datetime.now(timezone.utc)
    with requests.Session() as session:
        session.mount('https://', HTTPAdapter(max_retries=Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=['GET'], respect_retry_after_header=True)))
        response = session.get(API_URL, params=params, timeout=(15, 90))
        response.raise_for_status()
        data = response.json()
    if not isinstance(data, dict) or data.get('error'):
        raise ValueError('Open-Meteo a retourne une erreur')
    return dict(version=1, collecte_id=str(uuid4()), lieu_id=lieu, latitude=latitude,
                longitude=longitude, prevision_heures=heures,
                ingestion_timestamp=collecte.isoformat(), parametres=params, donnees=data)


def enregistrer_bronze_lot(envelopes, nom_table='meteo_horaire_bruts'):
    from psycopg import sql
    from psycopg.types.json import Jsonb
    verifier_lot(envelopes)
    if nom_table not in ('meteo_horaire_bruts', 'meteo_quotidienne_points_bruts'):
        raise ValueError('Table meteo Bronze inconnue')
    with connexion('bronze') as conn:
        conn.execute('SELECT pg_advisory_xact_lock(740221107)')
        table(conn, nom_table, BRONZE, ('collecte_id',))
        count = 0
        for envelope in envelopes:
            row = dict(collecte_id=UUID(envelope['collecte_id']), lieu_id=envelope['lieu_id'],
                       ingestion_timestamp=datetime.fromisoformat(envelope['ingestion_timestamp']), payload=Jsonb(envelope))
            count += inserer(conn, nom_table, BRONZE, [row])
            stored = conn.execute(sql.SQL('SELECT payload FROM public.{} WHERE collecte_id=%s').format(sql.Identifier(nom_table)),
                                  (row['collecte_id'],)).fetchone()[0]
            if stored != envelope:
                raise ValueError('Cet identifiant de collecte existe avec un autre contenu')
    return count


def enregistrer_bronze(envelope):
    return enregistrer_bronze_lot([envelope])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--points', type=Path, default=CONFIG)
    parser.add_argument('--lieu-id', help='Limiter la configuration a un lieu')
    parser.add_argument('--latitude', type=float)
    parser.add_argument('--longitude', type=float)
    parser.add_argument('--heures', type=int, default=48)
    parser.add_argument('--json-seulement', action='store_true', help='Aucune connexion Neon')
    parser.add_argument('--output-dir', type=Path,
                        default=Path(__file__).resolve().parent / 'output_meteo_horaire')
    parser.add_argument('--manifest', type=Path, default=Path('reports/collecte_meteo_horaire.json'))
    args = parser.parse_args()
    if args.latitude is not None or args.longitude is not None:
        if args.latitude is None or args.longitude is None or not args.lieu_id:
            parser.error('Fournir ensemble --lieu-id, --latitude et --longitude')
        points = [dict(lieu_id=args.lieu_id, latitude=args.latitude, longitude=args.longitude)]
    else:
        points = charger_points(args.points)
        if args.lieu_id:
            points = [p for p in points if p['lieu_id'] == args.lieu_id]
            if not points:
                parser.error('Lieu absent de la configuration')
    args.output_dir.mkdir(parents=True, exist_ok=True)
    lot_id, envelopes = str(uuid4()), []
    for point in points:
        envelope = collecter(point['lieu_id'], point['latitude'], point['longitude'], args.heures)
        envelope['lot_id'] = lot_id
        envelope['lieu_nom'] = point.get('nom', point['lieu_id'])
        archive = args.output_dir / (point['lieu_id'] + '_' + envelope['collecte_id'] + '.json')
        archive.write_text(json.dumps(envelope, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')
        envelopes.append(envelope)
    verifier_lot(envelopes)
    if args.json_seulement:
        for envelope in envelopes:
            transformer(envelope)
        mode, inserted = 'verification_sans_neon', 0
    else:
        inserted = enregistrer_bronze_lot(envelopes)
        mode = 'chargement_bronze'
    report = dict(version=2, flux='horaire', mode=mode, lot_id=lot_id,
                  lieux_attendus=[p['lieu_id'] for p in points], inserees=inserted,
                  collectes=[dict(collecte_id=e['collecte_id'], lieu_id=e['lieu_id']) for e in envelopes],
                  points=len(points), previsions=len(points) * args.heures)
    if len(envelopes) == 1:
        report['collecte_id'] = envelopes[0]['collecte_id']
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        message = str(exc) if isinstance(exc, ValueError) else f'Echec de collecte ({type(exc).__name__})'
        print(f'::error::{message}', file=sys.stderr)
        sys.exit(1)

"""Liste unique des lieux et manifestes partagés par les deux flux météo."""
import json
from pathlib import Path
from uuid import UUID
from transformation.nettoyage_meteo_horaire import valider_localisation

CONFIG = Path(__file__).resolve().parents[1] / 'config' / 'points_meteo_quebec.json'


def charger_points(path=CONFIG):
    data = json.loads(Path(path).read_text(encoding='utf-8'))
    points = data.get('points')
    if data.get('version') != 1 or not isinstance(points, list) or not 1 <= len(points) <= 100:
        raise ValueError('Configuration de points meteo invalide')
    identifiers, coordinates = set(), set()
    for point in points:
        if not isinstance(point, dict) or not {'lieu_id', 'latitude', 'longitude'} <= point.keys():
            raise ValueError('Chaque point doit avoir un lieu_id, une latitude et une longitude')
        valider_localisation(point['lieu_id'], point['latitude'], point['longitude'], 48)
        geo = (point['latitude'], point['longitude'])
        if point['lieu_id'] in identifiers or geo in coordinates:
            raise ValueError('Identifiant de lieu ou coordonnees dupliques dans la configuration')
        identifiers.add(point['lieu_id'])
        coordinates.add(geo)
    return points


def verifier_lot(envelopes):
    if not envelopes:
        raise ValueError('Lot meteo vide')
    identifiers = [e['collecte_id'] for e in envelopes]
    places = [e['lieu_id'] for e in envelopes]
    if len(set(identifiers)) != len(identifiers) or len(set(places)) != len(places):
        raise ValueError('Collecte ou lieu duplique dans le lot')
    lots = [e.get('lot_id') for e in envelopes]
    if any(lots) and (not all(lots) or len(set(lots)) != 1):
        raise ValueError('Des collectes de lots differents ont ete melangees')
    for identifiant in identifiers:
        UUID(identifiant)


def lire_manifest(path, flux):
    manifest = json.loads(Path(path).read_text(encoding='utf-8'))
    if manifest.get('mode') != 'chargement_bronze':
        raise ValueError('Manifest sans chargement Bronze reel')
    # Compatibilite avec le premier paquet horaire mono-point.
    if 'collectes' not in manifest and flux == 'horaire':
        return [(None, UUID(manifest['collecte_id']))]
    rows, expected = manifest.get('collectes'), manifest.get('lieux_attendus')
    if manifest.get('flux') != flux or not isinstance(rows, list) or not isinstance(expected, list) or not expected:
        raise ValueError('Manifest multi-points incompatible')
    verifier_lot(rows)
    if len(set(expected)) != len(expected) or set(expected) != {r['lieu_id'] for r in rows}:
        raise ValueError('Un ou plusieurs points attendus manquent dans le manifest')
    return [(r['lieu_id'], UUID(r['collecte_id'])) for r in rows]


def verifier_manifest_enveloppes(pairs, envelopes):
    verifier_lot(envelopes)
    if len(pairs) != len(envelopes):
        raise ValueError('Nombre de collectes incompatible avec le manifest')
    for (place, identifiant), envelope in zip(pairs, envelopes):
        if str(identifiant) != envelope['collecte_id'] or (place is not None and place != envelope['lieu_id']):
            raise ValueError('Le manifest ne correspond pas aux lieux des donnees Bronze')

"""Contrats complementaires aux CSV, sans acces reseau ni base."""
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math

URL_WFS = 'https://ws.mapserver.transports.gouv.qc.ca/swtq'
SOURCES_GEO = {
    'routes': ('ms:bgr_v_sous_route_res_sup_act', 'ide_sous_r'),
    'travaux': ('ms:chantiers_mtmdet', 'identifiant'),
    'debits': ('ms:circulation_routier', 'ide_sectn_trafc'),
}
METRIQUES = {
    'djma': ('annuelle', 'vehicules/jour'),
    'djme': ('estivale', 'vehicules/jour'),
    'djmh': ('hivernale', 'vehicules/jour'),
    'cam': ('annuelle', 'pourcent'),
}


def empreinte(obj):
    text = json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False)
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def identifiant(source, feature):
    if source not in SOURCES_GEO or feature.get('type') != 'Feature':
        raise ValueError('Source ou Feature inconnue')
    props = feature.get('properties')
    value = props.get(SOURCES_GEO[source][1]) if isinstance(props, dict) else None
    if value is None or isinstance(value, (bool, float)) or not str(value).strip():
        raise ValueError('Identifiant metier manquant ou non entier/texte')
    return str(value).strip()


def valider_geometrie(geom):
    if not isinstance(geom, dict) or geom.get('type') not in ('LineString', 'MultiLineString'):
        raise ValueError('Trace LineString ou MultiLineString obligatoire')
    coords = geom.get('coordinates')
    lines = [coords] if geom['type'] == 'LineString' else coords
    if not isinstance(lines, list) or not lines:
        raise ValueError('Geometrie vide')
    for line in lines:
        if not isinstance(line, list) or len(line) < 2:
            raise ValueError('Ligne incomplete')
        positions = []
        for point in line:
            if not isinstance(point, list) or len(point) not in (2, 3):
                raise ValueError('Position GeoJSON invalide')
            if any(isinstance(v, bool) or not isinstance(v, (int, float)) or
                   not math.isfinite(v) for v in point):
                raise ValueError('Coordonnee non numerique ou non finie')
            lon, lat = point[:2]
            # Garde-fou regional : detecter axes inverses et metres etiquetes 4326.
            if not (-85 <= lon <= -50 and 40 <= lat <= 70):
                raise ValueError('Coordonnees hors du domaine Quebec, ou axes inverses')
            positions.append((lon, lat))
        if len(set(positions)) < 2:
            raise ValueError('Ligne de longueur nulle')


def nombre(value):
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, bool):
        raise ValueError('Booleen interdit pour une mesure')
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise ValueError('Mesure non numerique') from None
    if not result.is_finite():
        raise ValueError('Mesure non finie')
    return result


def references_debits(props):
    """Passer de 10 colonnes par indicateur a annee + indicateur + valeur.

    On conserve des moyennes de reference, jamais un debit observe a collecte_at.
    val_30e_heure reste brut : son annee ne doit pas etre inventee.
    """
    rows, seen = [], {}
    for metric, (periode, unite) in METRIQUES.items():
        for i in range(1, 11):
            value = nombre(props.get(f'val_{metric}_annee_{i}'))
            if value is None:
                continue
            year = nombre(props.get(f'{metric}_annee_{i}'))
            if year is None or year != year.to_integral_value() or not 1900 <= year <= 2100:
                raise ValueError(f'{metric}: annee de reference absente/invalide')
            if value < 0 or (metric == 'cam' and value > 100):
                raise ValueError(f'{metric}: valeur hors plage')
            key = (metric, int(year))
            if key in seen:
                if seen[key] != value:
                    raise ValueError('Deux valeurs differentes pour le meme indicateur et annee')
                continue
            seen[key] = value
            rows.append(dict(indicateur=metric, annee_reference=int(year), valeur=value,
                             periode=periode, unite=unite))
    return rows


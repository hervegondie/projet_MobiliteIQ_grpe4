"""Réutiliser les conversions quotidiennes existantes en conservant chaque lieu."""
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

from ingestion import ingestion_meteo as legacy
from transformation.nettoyage import transform
from transformation.nettoyage_meteo_horaire import nombre, valider_localisation


def parametres(latitude, longitude, debut, fin):
    return dict(latitude=latitude, longitude=longitude, start_date=debut.isoformat(), end_date=fin.isoformat(),
                daily=','.join(legacy.MESURES), hourly='visibility', timezone=legacy.FUSEAU_METEO,
                timeformat='iso8601', temperature_unit='celsius', wind_speed_unit='kmh', precipitation_unit='mm')


def transformer(envelope):
    required = {'collecte_id', 'lieu_id', 'latitude', 'longitude', 'ingestion_timestamp', 'date_debut', 'date_fin', 'donnees'}
    if (not isinstance(envelope, dict) or type(envelope.get('version')) is not int
            or envelope['version'] != 1 or envelope.get('flux') != 'quotidienne' or not required <= envelope.keys()):
        raise ValueError('Enveloppe quotidienne multi-points invalide')
    identifiant = UUID(envelope['collecte_id'])
    lieu, latitude, longitude = envelope['lieu_id'], envelope['latitude'], envelope['longitude']
    valider_localisation(lieu, latitude, longitude, 48)
    collecte = datetime.fromisoformat(envelope['ingestion_timestamp'])
    if collecte.utcoffset() is None:
        raise ValueError('Date de collecte quotidienne sans fuseau')
    collecte = collecte.astimezone(timezone.utc)
    debut, fin = date.fromisoformat(envelope['date_debut']), date.fromisoformat(envelope['date_fin'])
    if debut > fin or (fin - debut).days > 3660:
        raise ValueError('Periode quotidienne invalide ou superieure a dix ans')
    if fin >= collecte.astimezone(ZoneInfo(legacy.FUSEAU_METEO)).date():
        raise ValueError('Les jours quotidiens doivent etre complets avant le jour de collecte')
    if envelope.get('api_url') not in (legacy.API_RECENTE, legacy.API_URL):
        raise ValueError('Source quotidienne inconnue')
    if envelope.get('parametres') != parametres(latitude, longitude, debut, fin):
        raise ValueError('Parametres de collecte quotidienne incompatibles')
    data = envelope['donnees']
    if not isinstance(data, dict) or data.get('timezone') != legacy.FUSEAU_METEO:
        raise ValueError('Les jours meteo doivent etre ceux du fuseau America/Toronto')
    daily_units, hourly_units = data.get('daily_units', {}), data.get('hourly_units', {})
    if daily_units.get('time') != 'iso8601' or hourly_units.get('time') != 'iso8601' or hourly_units.get('visibility') != 'm':
        raise ValueError('Unites de temps ou de visibilite quotidiennes incompatibles')
    units = {'temperature_2m_max': '°C', 'temperature_2m_min': '°C', 'precipitation_sum': 'mm',
             'rain_sum': 'mm', 'snowfall_sum': 'cm', 'wind_speed_10m_max': 'km/h', 'wind_gusts_10m_max': 'km/h'}
    if any(daily_units.get(k) != unit for k, unit in units.items()):
        raise ValueError('Unites quotidiennes incompatibles')
    daily_rows, hourly_rows = legacy.extraire_lignes(data, debut, fin)
    # Comme avg() dans l'ancien script Spark : ignorer NULL, conserver les zeros.
    visibility = defaultdict(list)
    for instant, value in hourly_rows:
        if value is not None:
            if value < 0:
                raise ValueError('Visibilite horaire negative')
            visibility[instant[:10]].append(Decimal(str(value)))
    base = dict(collecte_id=identifiant, lieu_id=lieu, latitude=nombre(latitude, 'latitude', -90, 90, True),
                longitude=nombre(longitude, 'longitude', -180, 180, True), collecte_at=collecte,
                latitude_modele=nombre(data.get('latitude'), 'latitude_modele', -90, 90, True),
                longitude_modele=nombre(data.get('longitude'), 'longitude_modele', -180, 180, True))
    output = []
    for daily in daily_rows:
        raw = dict(zip(['date', *legacy.MESURES.values()], daily))
        samples = visibility[raw['date']]
        raw['visibilite_moyenne'] = sum(samples) / len(samples) if samples else None
        raw['ingestion_timestamp'] = collecte.isoformat()
        values, errors = transform('meteo', raw, legacy.FUSEAU_METEO, 'UTC')
        if errors:
            raise ValueError('Donnees quotidiennes invalides : ' + '; '.join(errors))
        values.pop('ingestion_timestamp')
        output.append({**base, **values})
    return output

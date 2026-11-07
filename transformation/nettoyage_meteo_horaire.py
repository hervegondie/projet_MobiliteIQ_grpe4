"""Validation pure des conditions actuelles et des prévisions Open-Meteo."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import math
import re
from uuid import UUID

API_URL = 'https://api.open-meteo.com/v1/forecast'
# API : (colonne Silver, unité, minimum, maximum). La neige est en cm.
MESURES = {
    'temperature_2m': ('temperature_c', '°C', None, None),
    'apparent_temperature': ('ressenti_c', '°C', None, None),
    'relative_humidity_2m': ('humidite_pct', '%', 0, 100),
    'precipitation': ('precipitations_mm', 'mm', 0, None),
    'rain': ('pluie_mm', 'mm', 0, None),
    'snowfall': ('neige_cm', 'cm', 0, None),
    'weather_code': ('code_meteo', 'wmo code', 0, 99),
    'cloud_cover': ('nuages_pct', '%', 0, 100),
    'wind_speed_10m': ('vent_kmh', 'km/h', 0, None),
    'wind_direction_10m': ('direction_vent_deg', '°', 0, 360),
    'wind_gusts_10m': ('rafales_kmh', 'km/h', 0, None),
    'visibility': ('visibilite_m', 'm', 0, None),
}
PREVISIONS = {**MESURES, 'precipitation_probability': ('probabilite_pluie_pct', '%', 0, 100)}
WMO = {0, 1, 2, 3, 45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67,
       71, 73, 75, 77, 80, 81, 82, 85, 86, 95, 96, 99}


def nombre(value, name, lower=None, upper=None, required=False):
    if value is None and not required:
        return None
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f'{name} : nombre fini attendu')
    if (lower is not None and value < lower) or (upper is not None and value > upper):
        raise ValueError(f'{name} : valeur hors limites')
    return Decimal(str(value))


def instant(value):
    if type(value) is not int:
        raise ValueError('Horodatage API : entier Unix attendu')
    try:
        result = datetime.fromtimestamp(value, timezone.utc)
    except (OverflowError, OSError, ValueError):
        raise ValueError('Horodatage API invalide') from None
    if not 2000 <= result.year <= 2100:
        raise ValueError('Horodatage API hors periode attendue')
    return result


def valider_localisation(lieu, latitude, longitude, heures):
    if not isinstance(lieu, str) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', lieu):
        raise ValueError('lieu_id : utiliser 1 a 64 lettres, chiffres, tirets ou underscores')
    nombre(latitude, 'latitude', -90, 90, True)
    nombre(longitude, 'longitude', -180, 180, True)
    if type(heures) is not int or not 1 <= heures <= 168:
        raise ValueError('Horizon de prevision : entier entre 1 et 168 heures attendu')


def parametres(latitude, longitude, heures=48):
    return dict(latitude=latitude, longitude=longitude, current=','.join(MESURES),
                hourly=','.join(PREVISIONS), forecast_hours=heures, timezone='UTC',
                timeformat='unixtime', temperature_unit='celsius', wind_speed_unit='kmh',
                precipitation_unit='mm')


def mesures(values, units, specifications):
    if not isinstance(values, dict) or not isinstance(units, dict):
        raise ValueError('Mesures ou unites absentes')
    output = {}
    for api, (column, unit, lower, upper) in specifications.items():
        if api not in values or units.get(api) != unit:
            raise ValueError(f'{api} : champ absent ou unite inattendue')
        number = nombre(values[api], api, lower, upper,
                        required=api in ('temperature_2m', 'weather_code', 'wind_speed_10m'))
        if api == 'weather_code':
            if number != number.to_integral_value() or int(number) not in WMO:
                raise ValueError('Code meteo WMO inconnu')
            number = int(number)
        output[column] = number
    return output


def transformer(envelope):
    if not isinstance(envelope, dict) or type(envelope.get('version')) is not int or envelope['version'] != 1:
        raise ValueError('Enveloppe Bronze incompatible')
    try:
        identifiant = UUID(envelope['collecte_id'])
        collecte = datetime.fromisoformat(envelope['ingestion_timestamp'])
        lieu, lat, lon = envelope['lieu_id'], envelope['latitude'], envelope['longitude']
        heures, data = envelope['prevision_heures'], envelope['donnees']
    except (ValueError, KeyError, TypeError):
        raise ValueError('Metadonnees Bronze invalides') from None
    valider_localisation(lieu, lat, lon, heures)
    if collecte.tzinfo is None or collecte.utcoffset() is None:
        raise ValueError('La date de collecte doit inclure son fuseau')
    collecte = collecte.astimezone(timezone.utc)
    if envelope.get('parametres') != parametres(lat, lon, heures):
        raise ValueError('Parametres API incompatibles avec le contrat')
    if not isinstance(data, dict) or data.get('error') or data.get('utc_offset_seconds') != 0:
        raise ValueError('Reponse API invalide ou fuseau non UTC')
    base = dict(collecte_id=identifiant, lieu_id=lieu, latitude=nombre(lat, 'latitude', -90, 90, True),
                longitude=nombre(lon, 'longitude', -180, 180, True), collecte_at=collecte,
                latitude_modele=nombre(data.get('latitude'), 'latitude_modele', -90, 90, True),
                longitude_modele=nombre(data.get('longitude'), 'longitude_modele', -180, 180, True))
    current, hourly = data.get('current'), data.get('hourly')
    cu, hu = data.get('current_units'), data.get('hourly_units')
    if not isinstance(current, dict) or not isinstance(hourly, dict):
        raise ValueError('Sections current/hourly absentes')
    if not isinstance(cu, dict) or not isinstance(hu, dict) or cu.get('time') != 'unixtime' or hu.get('time') != 'unixtime':
        raise ValueError('Unites temporelles Unix attendues')
    if cu.get('interval') != 'seconds':
        raise ValueError('Intervalle de meteo actuelle : unite secondes attendue')
    current_time = instant(current.get('time'))
    if not -300 <= (collecte - current_time).total_seconds() <= 7200:
        raise ValueError('Meteo actuelle trop ancienne ou dans le futur par rapport a la collecte')
    interval = current.get('interval')
    if type(interval) is not int or not 1 <= interval <= 3600:
        raise ValueError('Intervalle de la meteo actuelle invalide')
    actuelle = {**base, 'heure_meteo': current_time, 'duree_secondes': interval,
                **mesures(current, cu, MESURES)}
    times = hourly.get('time')
    if not isinstance(times, list) or len(times) != heures:
        raise ValueError('Nombre de previsions horaires incomplet')
    parsed = [instant(t) for t in times]
    if parsed[0] != collecte.replace(minute=0, second=0, microsecond=0):
        raise ValueError('La prevision doit commencer a l’heure UTC de collecte')
    if any(b - a != timedelta(hours=1) for a, b in zip(parsed, parsed[1:])):
        raise ValueError('Heures manquantes, dupliquees ou desordonnees')
    for name in PREVISIONS:
        if not isinstance(hourly.get(name), list) or len(hourly[name]) != heures:
            raise ValueError(f'{name} : serie horaire incomplete')
    forecasts = []
    for i, valid_at in enumerate(parsed):
        values = {name: hourly[name][i] for name in PREVISIONS}
        forecasts.append({**base, 'heure_meteo': valid_at,
                          'horizon_heures': Decimal(str((valid_at - collecte).total_seconds())) / Decimal(3600),
                          **mesures(values, hu, PREVISIONS)})
    return actuelle, forecasts

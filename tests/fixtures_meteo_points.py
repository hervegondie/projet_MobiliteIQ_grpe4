"""Reponses quotidiennes et horaires de test pour plusieurs secteurs."""
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4
from zoneinfo import ZoneInfo

from fixtures_meteo_horaire import enveloppe
from ingestion import ingestion_meteo as legacy
from transformation.nettoyage_meteo_horaire import parametres as params_horaires
from transformation.nettoyage_meteo_quotidienne_points import parametres


def horaire(point, maintenant=None):
    result = enveloppe(maintenant=maintenant)
    result.update(lieu_id=point['lieu_id'], latitude=point['latitude'], longitude=point['longitude'])
    result['parametres'] = params_horaires(point['latitude'], point['longitude'], 48)
    result['donnees'].update(latitude=point['latitude'], longitude=point['longitude'])
    return result


def quotidienne(point, jours=7, debut=None, fin=None):
    now = datetime.now(timezone.utc)
    fin = fin or (now.astimezone(ZoneInfo('America/Toronto')).date() - timedelta(days=1))
    debut = debut or (fin - timedelta(days=jours - 1))
    dates = [(debut + timedelta(days=i)).isoformat() for i in range((fin - debut).days + 1)]
    daily = dict(time=dates, temperature_2m_max=[5.0] * len(dates), temperature_2m_min=[-2.0] * len(dates),
                 precipitation_sum=[0.0] * len(dates), rain_sum=[0.0] * len(dates), snowfall_sum=[0.0] * len(dates),
                 wind_speed_10m_max=[15.0] * len(dates), wind_gusts_10m_max=[25.0] * len(dates))
    times = [day + f'T{hour:02d}:00' for day in dates for hour in range(24)]
    data = dict(latitude=point['latitude'], longitude=point['longitude'], timezone='America/Toronto',
                daily=daily, hourly=dict(time=times, visibility=[1000.0, 3000.0] * (12 * len(dates))),
                daily_units=dict(time='iso8601', temperature_2m_max='°C', temperature_2m_min='°C',
                                 precipitation_sum='mm', rain_sum='mm', snowfall_sum='cm',
                                 wind_speed_10m_max='km/h', wind_gusts_10m_max='km/h'),
                hourly_units=dict(time='iso8601', visibility='m'))
    return dict(version=1, flux='quotidienne', collecte_id=str(uuid4()), lieu_id=point['lieu_id'],
                latitude=point['latitude'], longitude=point['longitude'], ingestion_timestamp=now.isoformat(),
                date_debut=debut.isoformat(), date_fin=fin.isoformat(), api_url=legacy.API_RECENTE,
                parametres=parametres(point['latitude'], point['longitude'], debut, fin), donnees=data)

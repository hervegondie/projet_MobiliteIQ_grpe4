"""Jeux météo synthétiques partagés par les tests hors ligne et Neon."""
from datetime import datetime, timezone
from uuid import uuid4
from transformation.nettoyage_meteo_horaire import MESURES, PREVISIONS, parametres


def enveloppe(heures=48, maintenant=None, temperature=4.5):
    now = maintenant or datetime.now(timezone.utc)
    hour = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    values = dict(temperature_2m=temperature, apparent_temperature=2.0, relative_humidity_2m=75,
                  precipitation=0.0, rain=0.0, snowfall=0.0, weather_code=3, cloud_cover=80,
                  wind_speed_10m=12.0, wind_direction_10m=240, wind_gusts_10m=20.0, visibility=15000)
    current = {**values, 'time': int(now.timestamp()) // 900 * 900, 'interval': 900}
    hourly = {k: [v] * heures for k, v in values.items()}
    hourly.update(time=[hour + i * 3600 for i in range(heures)], precipitation_probability=[20] * heures)
    data = dict(latitude=46.8, longitude=-71.2, utc_offset_seconds=0,
                current=current, current_units={'time': 'unixtime', 'interval': 'seconds',
                    **{k: v[1] for k, v in MESURES.items()}},
                hourly=hourly, hourly_units={'time': 'unixtime', **{k: v[1] for k, v in PREVISIONS.items()}})
    return dict(version=1, collecte_id=str(uuid4()), lieu_id='quebec', latitude=46.8139, longitude=-71.2080,
                prevision_heures=heures, ingestion_timestamp=now.isoformat(),
                parametres=parametres(46.8139, -71.2080, heures), donnees=data)

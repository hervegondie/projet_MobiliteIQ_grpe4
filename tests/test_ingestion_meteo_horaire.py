"""Tests hors ligne : contrat API, nettoyage, temps UTC et mode sans Neon."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from fixtures_meteo_horaire import enveloppe
from transformation.nettoyage_meteo_horaire import parametres, transformer, valider_localisation
from transformation.transformation_meteo_horaire import empreinte
from ingestion import ingestion_meteo_horaire as ingestion


class MeteoHoraireTests(unittest.TestCase):
    def test_actual_and_48_forecasts_are_separate(self):
        current, forecast = transformer(enveloppe())
        self.assertEqual(len(forecast), 48)
        self.assertEqual(current['temperature_c'], Decimal('4.5'))
        self.assertNotIn('probabilite_pluie_pct', current)
        self.assertEqual(forecast[0]['probabilite_pluie_pct'], 20)

    def test_zero_is_preserved_and_null_is_not_zero(self):
        data = enveloppe()
        data['donnees']['current']['visibility'] = None
        current, forecast = transformer(data)
        self.assertIsNone(current['visibilite_m'])
        self.assertEqual(current['pluie_mm'], 0)
        self.assertEqual(forecast[0]['neige_cm'], 0)

    def test_missing_temperature_is_refused(self):
        data = enveloppe()
        data['donnees']['current']['temperature_2m'] = None
        with self.assertRaisesRegex(ValueError, 'temperature_2m'):
            transformer(data)

    def test_percent_out_of_range_is_refused(self):
        data = enveloppe()
        data['donnees']['hourly']['precipitation_probability'][0] = 101
        with self.assertRaisesRegex(ValueError, 'hors limites'):
            transformer(data)

    def test_wrong_units_are_refused(self):
        data = enveloppe()
        data['donnees']['current_units']['wind_speed_10m'] = 'mph'
        with self.assertRaisesRegex(ValueError, 'unite inattendue'):
            transformer(data)

    def test_missing_and_duplicate_hours_are_refused(self):
        for modification in ('missing', 'duplicate'):
            data = enveloppe()
            times = data['donnees']['hourly']['time']
            if modification == 'missing':
                times.pop()
            else:
                times[1] = times[0]
            with self.subTest(modification=modification), self.assertRaises(ValueError):
                transformer(data)

    def test_incomplete_variable_series_is_refused(self):
        data = enveloppe()
        data['donnees']['hourly']['visibility'].pop()
        with self.assertRaisesRegex(ValueError, 'serie horaire incomplete'):
            transformer(data)

    def test_nan_boolean_negative_precipitation_are_refused(self):
        for value in (float('nan'), True, -1):
            data = enveloppe()
            data['donnees']['current']['precipitation'] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                transformer(data)

    def test_naive_collection_timestamp_is_refused(self):
        data = enveloppe()
        data['ingestion_timestamp'] = '2026-11-01T01:30:00'
        with self.assertRaisesRegex(ValueError, 'fuseau'):
            transformer(data)

    def test_dst_transition_has_unique_utc_hours(self):
        data = enveloppe(maintenant=datetime(2026, 11, 1, 4, 25, tzinfo=timezone.utc))
        _, rows = transformer(data)
        self.assertEqual(len({r['heure_meteo'] for r in rows}), 48)
        self.assertTrue(all(r['heure_meteo'].utcoffset() == timedelta(0) for r in rows))

    def test_stale_current_is_refused(self):
        data = enveloppe()
        data['donnees']['current']['time'] -= 3 * 3600
        with self.assertRaisesRegex(ValueError, 'ancienne'):
            transformer(data)

    def test_unknown_weather_code_is_refused(self):
        data = enveloppe()
        data['donnees']['current']['weather_code'] = 4
        with self.assertRaisesRegex(ValueError, 'WMO'):
            transformer(data)

    def test_wrong_forecast_start_is_refused(self):
        data = enveloppe()
        data['donnees']['hourly']['time'] = [t + 3600 for t in data['donnees']['hourly']['time']]
        with self.assertRaisesRegex(ValueError, 'commencer'):
            transformer(data)

    def test_bad_location_and_horizon_are_refused(self):
        for args in [('ville', 91.0, 0.0, 48), ('ville', 0.0, -181.0, 48),
                     ('ville', 0.0, 0.0, 0), ('ville', 0.0, 0.0, 169), ('../ville', 0.0, 0.0, 48)]:
            with self.subTest(args=args), self.assertRaises(ValueError):
                valider_localisation(*args)

    def test_payload_is_not_modified_by_transformation(self):
        data = enveloppe()
        before = deepcopy(data)
        digest = empreinte(data)
        transformer(data)
        self.assertEqual(data, before)
        self.assertEqual(empreinte(data), digest)

    def test_requested_units_and_timeformat_are_explicit(self):
        params = parametres(46.8139, -71.2080)
        self.assertEqual((params['timeformat'], params['timezone'], params['forecast_hours']), ('unixtime', 'UTC', 48))
        self.assertEqual(params['wind_speed_unit'], 'kmh')

    def test_json_only_never_opens_neon(self):
        with tempfile.TemporaryDirectory() as folder:
            manifest = Path(folder) / 'manifest.json'
            argv = ['ingestion_meteo_horaire.py', '--lieu-id', 'quebec', '--json-seulement', '--output-dir', folder, '--manifest', str(manifest)]
            with patch.object(ingestion, 'collecter', return_value=enveloppe()), \
                 patch.object(ingestion, 'enregistrer_bronze') as write, patch.object(sys, 'argv', argv):
                ingestion.main()
            write.assert_not_called()
            self.assertEqual(json.loads(manifest.read_text(encoding='utf-8'))['previsions'], 48)

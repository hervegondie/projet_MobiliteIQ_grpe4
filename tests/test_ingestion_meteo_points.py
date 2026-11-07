"""Configuration commune, lot complet et reemploi du nettoyage quotidien existant."""
from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from ingestion.points_meteo import CONFIG, charger_points, lire_manifest, verifier_manifest_enveloppes
from ingestion.points_meteo import verifier_lot
from ingestion import ingestion_meteo as legacy
from ingestion import ingestion_meteo_horaire as hourly
from ingestion import ingestion_meteo_quotidienne_points as daily
from fixtures_meteo_points import horaire, quotidienne
from transformation.nettoyage_meteo_quotidienne_points import transformer


class MeteoPointsTests(unittest.TestCase):
    def setUp(self):
        self.points = charger_points()

    def test_six_expected_sectors_have_distinct_coordinates(self):
        self.assertEqual({p['lieu_id'] for p in self.points},
                         {'quebec', 'sainte_foy', 'beauport', 'charlesbourg', 'val_belair', 'levis'})
        self.assertEqual(len({(p['latitude'], p['longitude']) for p in self.points}), 6)

    def test_duplicate_id_or_coordinates_are_refused(self):
        for duplicate_coordinates in (False, True):
            points = deepcopy(self.points)
            if duplicate_coordinates:
                points[1].update(latitude=points[0]['latitude'], longitude=points[0]['longitude'])
            else:
                points[1]['lieu_id'] = points[0]['lieu_id']
            with tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / 'points.json'
                path.write_text(json.dumps(dict(version=1, points=points)), encoding='utf-8')
                with self.assertRaisesRegex(ValueError, 'dupliques'):
                    charger_points(path)

    def test_manifest_cannot_omit_one_of_six_expected_places(self):
        envelopes = [horaire(p) for p in self.points]
        report = dict(mode='chargement_bronze', flux='horaire', lieux_attendus=[p['lieu_id'] for p in self.points],
                      collectes=[dict(collecte_id=e['collecte_id'], lieu_id=e['lieu_id']) for e in envelopes[:-1]])
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'manifest.json'
            path.write_text(json.dumps(report), encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'manquent'):
                lire_manifest(path, 'horaire')

    def test_manifest_lieu_must_match_bronze(self):
        from uuid import UUID
        envelope = horaire(self.points[0])
        with self.assertRaisesRegex(ValueError, 'lieux'):
            verifier_manifest_enveloppes([('levis', UUID(envelope['collecte_id']))], [envelope])

    def test_hourly_default_calls_all_six_points_before_one_bronze_write(self):
        def response(lieu, lat, lon, heures):
            return horaire(dict(lieu_id=lieu, latitude=lat, longitude=lon))
        with tempfile.TemporaryDirectory() as folder:
            manifest = Path(folder) / 'manifest.json'
            argv = ['horaire.py', '--output-dir', folder, '--manifest', str(manifest)]
            with patch.object(hourly, 'collecter', side_effect=response) as fetch, \
                 patch.object(hourly, 'enregistrer_bronze_lot', return_value=6) as write, patch.object(sys, 'argv', argv):
                hourly.main()
            self.assertEqual(fetch.call_count, 6)
            self.assertEqual(len(write.call_args.args[0]), 6)
            write.assert_called_once()
            report = json.loads(manifest.read_text(encoding='utf-8'))
            self.assertEqual((report['points'], report['previsions']), (6, 288))
            self.assertEqual(len(lire_manifest(manifest, 'horaire')), 6)

    def test_partial_api_failure_never_writes_a_partial_bronze_batch(self):
        calls = 0
        def response(lieu, lat, lon, heures):
            nonlocal calls
            calls += 1
            if calls == 3:
                raise ValueError('API indisponible pour ce point')
            return horaire(dict(lieu_id=lieu, latitude=lat, longitude=lon))
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(hourly, 'collecter', side_effect=response), \
                 patch.object(hourly, 'enregistrer_bronze_lot') as write, \
                 patch.object(sys, 'argv', ['horaire.py', '--output-dir', folder]), self.assertRaises(ValueError):
                hourly.main()
            write.assert_not_called()

    def test_six_points_produce_42_daily_rows_without_mixing_places(self):
        rows = [r for p in self.points for r in transformer(quotidienne(p))]
        self.assertEqual(len(rows), 42)
        self.assertEqual(len({(r['lieu_id'], r['date']) for r in rows}), 42)

    def test_daily_visibility_uses_same_average_as_existing_flow(self):
        row = transformer(quotidienne(self.points[0]))[0]
        self.assertEqual(row['visibilite_moyenne'], Decimal(2000))
        self.assertEqual((row['temperature_min'], row['temperature_max']), (Decimal(-2), Decimal(5)))

    def test_daily_visibility_null_is_not_zero(self):
        data = quotidienne(self.points[0])
        data['donnees']['hourly']['visibility'] = [None] * len(data['donnees']['hourly']['time'])
        self.assertIsNone(transformer(data)[0]['visibilite_moyenne'])

    def test_daily_temperature_inversion_is_refused(self):
        data = quotidienne(self.points[0])
        data['donnees']['daily']['temperature_2m_min'][0] = 10.0
        with self.assertRaisesRegex(ValueError, 'temperature_min'):
            transformer(data)

    def test_negative_visibility_cannot_be_hidden_in_a_positive_average(self):
        data = quotidienne(self.points[0])
        data['donnees']['hourly']['visibility'][0] = -10.0
        with self.assertRaisesRegex(ValueError, 'negative'):
            transformer(data)

    def test_missing_daily_metadata_is_refused(self):
        data = quotidienne(self.points[0])
        del data['date_fin']
        with self.assertRaisesRegex(ValueError, 'invalide'):
            transformer(data)

    def test_mixed_hourly_batches_are_refused(self):
        from uuid import uuid4
        envelopes = [horaire(p) for p in self.points]
        for envelope in envelopes:
            envelope['lot_id'] = str(uuid4())
        with self.assertRaisesRegex(ValueError, 'lots differents'):
            verifier_lot(envelopes)

    def test_daily_wrong_units_or_missing_dates_are_refused(self):
        for change in ('units', 'date'):
            data = quotidienne(self.points[0])
            if change == 'units':
                data['donnees']['daily_units']['temperature_2m_max'] = '°F'
            else:
                data['donnees']['daily']['time'].pop()
            with self.subTest(change=change), self.assertRaises(ValueError):
                transformer(data)

    def test_legacy_cli_delegates_points_mode_without_using_spark(self):
        with patch.object(sys, 'argv', ['meteo.py', '--points', str(CONFIG), '--csv-seulement']), \
             patch.object(daily, 'executer') as execute, patch.object(legacy, 'creer_csv') as spark:
            legacy.main()
            execute.assert_called_once()
            self.assertTrue(execute.call_args.args[4])
            spark.assert_not_called()

    def test_legacy_http_extraction_accepts_point_coordinates(self):
        data = quotidienne(self.points[1])
        from datetime import date
        requests = MagicMock()
        requests.get.return_value.json.return_value = data['donnees']
        with patch.dict(sys.modules, requests=requests):
            legacy.telecharger_brut(date.fromisoformat(data['date_debut']), date.fromisoformat(data['date_fin']),
                                    latitude=data['latitude'], longitude=data['longitude'])
        self.assertEqual(requests.get.call_args.kwargs['params']['latitude'], self.points[1]['latitude'])

    def test_daily_csv_only_produces_42_rows_without_neon(self):
        def response(point, debut, fin, api_url):
            return quotidienne(point, debut=debut, fin=fin)
        with tempfile.TemporaryDirectory() as folder, patch.object(daily, 'collecter', side_effect=response), \
             patch.object(daily, 'enregistrer_bronze_lot') as write:
            report = daily.executer(CONFIG, sans_neon=True, output_dir=Path(folder) / 'source',
                                    manifest_path=Path(folder) / 'manifest.json')
            self.assertEqual((report['points'], report['jours']), (6, 42))
            self.assertEqual(len(list((Path(folder) / 'source').rglob('*.csv'))), 1)
            write.assert_not_called()

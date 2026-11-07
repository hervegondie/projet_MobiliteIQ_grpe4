"""Lots de six lieux, reelles transactions Neon et reponses HTTP controlees."""
from contextlib import contextmanager
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import threading
from unittest.mock import patch
from urllib.parse import urlsplit, parse_qs
from uuid import UUID

from test_neon_meteo_horaire import MeteoPointsNeonBase
from fixtures_meteo_points import horaire, quotidienne
from ingestion.points_meteo import CONFIG, charger_points, lire_manifest
from ingestion import ingestion_meteo as legacy
from ingestion import ingestion_meteo_horaire as hourly
from ingestion import ingestion_meteo_quotidienne_points as daily
from transformation import transformation_meteo_horaire as silver_hourly
from transformation import transformation_meteo_quotidienne_points as silver_daily


@contextmanager
def serveur_points():
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_GET(self):
            params = parse_qs(urlsplit(self.path).query)
            point = dict(lieu_id='test', latitude=float(params['latitude'][0]), longitude=float(params['longitude'][0]))
            if 'daily' in params:
                envelope = quotidienne(point, debut=date.fromisoformat(params['start_date'][0]),
                                       fin=date.fromisoformat(params['end_date'][0]))
            else:
                envelope = horaire(point)
            body = json.dumps(envelope['donnees']).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}/forecast'
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)


class MeteoMultiPointsNeonTests(MeteoPointsNeonBase):
    def tracker(self, function):
        def write(envelopes, *args):
            self.hourly_ids.extend(UUID(e['collecte_id']) for e in envelopes)
            return function(envelopes, *args)
        return write

    def nombre(self, role, name):
        with self.pg.connect(self.urls[role]) as conn:
            if not conn.execute('SELECT to_regclass(%s)', ('public.' + name,)).fetchone()[0]:
                return 0
            return conn.execute(self.sql.SQL('SELECT count(*) FROM public.{}').format(self.sql.Identifier(name))).fetchone()[0]

    def test_six_hourly_http_sources_produce_six_current_and_288_forecasts(self):
        manifest = self.folder / 'hourly.json'
        argv = ['horaire.py', '--points', str(CONFIG), '--output-dir', str(self.folder / 'raw'), '--manifest', str(manifest)]
        original = hourly.enregistrer_bronze_lot
        with serveur_points() as url, patch.object(hourly, 'API_URL', url), \
             patch.object(hourly, 'enregistrer_bronze_lot', side_effect=self.tracker(original)), \
             patch.object(sys, 'argv', argv):
            hourly.main()
        pairs = lire_manifest(manifest, 'horaire')
        envelopes = [silver_hourly.lire_bronze(identifiant) for _, identifiant in pairs]
        result = silver_hourly.charger_lot(envelopes)
        self.assertEqual(sum(r['previsions_inserees'] for r in result), 288)
        self.assertEqual(self.nombre('silver', 'v_meteo_actuelle_derniere'), 6)
        self.assertEqual(self.nombre('silver', 'v_meteo_previsions_dernieres'), 288)
        self.assertTrue(all(silver_hourly.verifier(e)['statut'] == 'OK' for e in envelopes))

    def test_hourly_sql_failure_on_third_point_rolls_back_first_two(self):
        envelopes = [horaire(point) for point in charger_points()]
        self.hourly_ids.extend(UUID(e['collecte_id']) for e in envelopes)
        hourly.enregistrer_bronze_lot(envelopes)
        original = silver_hourly._charger_connexion
        calls = 0
        def fail_third(conn, envelope, actuelle, forecasts):
            nonlocal calls
            calls += 1
            if calls == 3:
                raise RuntimeError('Panne injectee pendant le troisieme point')
            return original(conn, envelope, actuelle, forecasts)
        with patch.object(silver_hourly, '_charger_connexion', side_effect=fail_third), self.assertRaises(RuntimeError):
            silver_hourly.charger_lot(envelopes)
        self.assertEqual(self.nombre('silver', 'meteo_actuelle'), 0)
        self.assertEqual(self.nombre('silver', 'meteo_previsions_horaires'), 0)
        self.assertEqual(self.nombre('bronze', 'meteo_horaire_bruts'), 6)

    def test_legacy_points_cli_http_to_daily_silver_and_replay(self):
        manifest = self.folder / 'daily.json'
        original_http, original_write = legacy.telecharger_brut, daily.enregistrer_bronze_lot
        with serveur_points() as url:
            def redirect(debut, fin, api_url, latitude, longitude):
                return original_http(debut, fin, url, latitude, longitude)
            argv = ['meteo.py', '--points', str(CONFIG), '--output-dir', str(self.folder / 'daily'), '--manifest', str(manifest)]
            with patch.object(legacy, 'telecharger_brut', side_effect=redirect), \
                 patch.object(daily, 'enregistrer_bronze_lot', side_effect=self.tracker(original_write)), \
                 patch.object(sys, 'argv', argv):
                legacy.main()
        envelopes = [silver_daily.lire_bronze(identifiant) for _, identifiant in lire_manifest(manifest, 'quotidienne')]
        result = silver_daily.charger_lot(envelopes)
        self.assertEqual(sum(r['inserees'] for r in result), 42)
        self.assertEqual(self.nombre('silver', 'v_meteo_quotidienne_points_dernieres'), 42)
        self.assertEqual(sum(r['inserees'] for r in silver_daily.charger_lot(envelopes)), 0)
        self.assertTrue(all(silver_daily.verifier(e)['statut'] == 'OK' for e in envelopes))
        # Le chemin multi-points n'ecrit jamais dans la table historique mono-point.
        self.assertEqual(self.nombre('bronze', 'meteo'), 0)

    def test_bad_daily_point_rejects_the_entire_six_point_lot(self):
        envelopes = [quotidienne(point) for point in charger_points()]
        envelopes[2]['donnees']['daily']['temperature_2m_min'][0] = 99.0
        self.hourly_ids.extend(UUID(e['collecte_id']) for e in envelopes)
        hourly.enregistrer_bronze_lot(envelopes, 'meteo_quotidienne_points_bruts')
        with self.assertRaisesRegex(ValueError, 'Lot quotidien refuse'):
            silver_daily.charger_lot(envelopes)
        self.assertEqual(self.nombre('silver', 'meteo_quotidienne_points'), 0)
        self.assertEqual(self.nombre('silver', 'meteo_quotidienne_points_rejets'), 1)
        self.assertEqual(self.nombre('bronze', 'meteo_quotidienne_points_bruts'), 6)

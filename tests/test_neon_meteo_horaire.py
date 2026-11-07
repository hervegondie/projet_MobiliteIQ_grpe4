"""Vrai HTTP simule -> nouvelle collecte -> Bronze/Silver Neon de ci-test-123."""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
from unittest.mock import patch
from uuid import UUID

from neon_support import NeonCase
from fixtures_meteo_horaire import enveloppe
from ingestion import ingestion_meteo_horaire as ingestion
from transformation import transformation_meteo_horaire as silver


@contextmanager
def serveur_api():
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_GET(self):
            body = json.dumps(enveloppe()['donnees']).encode('utf-8')
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


class MeteoPointsNeonBase(NeonCase):
    def setUp(self):
        super().setUp()
        self.hourly_ids = []
        self.addCleanup(self.nettoyer_horaire)
        self.env_patch = patch.dict(os.environ, BRONZE_DATABASE_URL=self.urls['bronze'],
                                    DATABASE_URL=self.urls['bronze'], SILVER_DATABASE_URL=self.urls['silver'])
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)
        for role, tables in [('bronze', ['meteo_horaire_bruts', 'meteo_quotidienne_points_bruts']),
                             ('silver', ['meteo_actuelle', 'meteo_previsions_horaires', 'meteo_quotidienne_points',
                                         'meteo_horaire_runs', 'meteo_horaire_rejets',
                                         'meteo_quotidienne_points_runs', 'meteo_quotidienne_points_rejets'])]:
            with self.pg.connect(self.urls[role]) as conn:
                for name in tables:
                    if conn.execute('SELECT to_regclass(%s)', ('public.' + name,)).fetchone()[0]:
                        n = conn.execute(self.sql.SQL('SELECT count(*) FROM public.{}').format(self.sql.Identifier(name))).fetchone()[0]
                        if n:
                            raise ValueError('Les nouvelles tables meteo de test doivent etre vides')

    def nettoyer_horaire(self):
        for role, tables in [('silver', ['meteo_actuelle', 'meteo_previsions_horaires', 'meteo_horaire_runs', 'meteo_horaire_rejets', 'meteo_quotidienne_points', 'meteo_quotidienne_points_runs', 'meteo_quotidienne_points_rejets']),
                             ('bronze', ['meteo_horaire_bruts', 'meteo_quotidienne_points_bruts'])]:
            with self.pg.connect(self.urls[role]) as conn:
                for name in tables:
                    if conn.execute('SELECT to_regclass(%s)', ('public.' + name,)).fetchone()[0]:
                        conn.execute(self.sql.SQL('DELETE FROM public.{} WHERE collecte_id=ANY(%s)').format(
                            self.sql.Identifier(name)), (self.hourly_ids,))

    def enregistrer(self, envelope):
        self.hourly_ids.append(UUID(envelope['collecte_id']))
        self.assertEqual(ingestion.enregistrer_bronze(envelope), 1)
        return silver.lire_bronze(UUID(envelope['collecte_id']))

class MeteoHoraireNeonTests(MeteoPointsNeonBase):
    def test_http_to_bronze_to_silver_then_replay(self):
        with serveur_api() as url, patch.object(ingestion, 'API_URL', url):
            data = ingestion.collecter()
        stored = self.enregistrer(data)
        self.assertEqual(stored, data)
        result = silver.charger(stored)
        self.assertEqual((result['actuelles_inserees'], result['previsions_inserees']), (1, 48))
        second = silver.charger(stored)
        self.assertEqual((second['actuelles_inserees'], second['previsions_inserees']), (0, 0))
        self.assertEqual(silver.verifier(stored)['statut'], 'OK')

    def test_invalid_payload_keeps_bronze_and_records_reject(self):
        data = enveloppe()
        data['donnees']['hourly']['visibility'].pop()
        self.enregistrer(data)
        with self.assertRaises(ValueError):
            silver.charger(data)
        with self.pg.connect(self.urls['silver']) as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM public.meteo_horaire_rejets WHERE collecte_id=%s',
                                          (UUID(data['collecte_id']),)).fetchone()[0], 1)
            for table in ('meteo_actuelle', 'meteo_previsions_horaires'):
                if conn.execute('SELECT to_regclass(%s)', ('public.' + table,)).fetchone()[0]:
                    self.assertEqual(conn.execute(self.sql.SQL('SELECT count(*) FROM public.{} WHERE collecte_id=%s').format(
                        self.sql.Identifier(table)), (UUID(data['collecte_id']),)).fetchone()[0], 0)

    def test_latest_views_keep_only_the_latest_whole_forecast(self):
        now = datetime.now(timezone.utc)
        first = self.enregistrer(enveloppe(maintenant=now - timedelta(hours=1), temperature=1.0))
        second = self.enregistrer(enveloppe(maintenant=now, temperature=9.0))
        silver.charger(first)
        silver.charger(second)
        with self.pg.connect(self.urls['silver']) as conn:
            rows = conn.execute('SELECT DISTINCT collecte_id,temperature_c FROM public.v_meteo_previsions_dernieres').fetchall()
            self.assertEqual(rows, [(UUID(second['collecte_id']), 9)])
            self.assertEqual(conn.execute('SELECT count(*) FROM public.v_meteo_previsions_dernieres').fetchone()[0], 48)
            self.assertEqual(conn.execute('SELECT collecte_id FROM public.v_meteo_actuelle_derniere').fetchone()[0], UUID(second['collecte_id']))

    def test_quality_detects_a_missing_silver_hour(self):
        data = self.enregistrer(enveloppe())
        silver.charger(data)
        with self.pg.connect(self.urls['silver']) as conn:
            conn.execute('DELETE FROM public.meteo_previsions_horaires WHERE collecte_id=%s AND heure_meteo='
                         '(SELECT min(heure_meteo) FROM public.meteo_previsions_horaires WHERE collecte_id=%s)',
                         (UUID(data['collecte_id']), UUID(data['collecte_id'])))
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            silver.verifier(data)

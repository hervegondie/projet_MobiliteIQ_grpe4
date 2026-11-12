"""Integration PostGIS dans ci-test-123 seulement (garde-fous existants)."""
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from decimal import Decimal
from unittest.mock import patch
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import json

from neon_support import NeonCase
from test_gold_prerequis import feature
from transformation.transformation_geospatiale import publier, MIGRATION
from transformation import transformation_geospatiale as silver
from ingestion import ingestion_geometries as bronze


class GoldNeonTests(NeonCase):
    def setUp(self):
        super().setUp()
        self.cids = []
        self.ids = []
        self.addCleanup(self.cleanup_geo)
        with self.pg.connect(self.urls['silver']) as conn:
            conn.execute(MIGRATION.read_text(encoding='utf-8'),prepare=False)

    def cleanup_geo(self):
        if not self.cids:
            return
        with self.pg.connect(self.urls['silver']) as conn:
            if not conn.execute("SELECT to_regclass('public.geo_collectes_silver')").fetchone()[0]:
                return
            for name in ['geo_rejets','geo_observations','geo_collectes_silver']:
                conn.execute(self.sql.SQL('DELETE FROM public.{} WHERE collecte_id=ANY(%s)').format(
                    self.sql.Identifier(name)), (self.cids,))
            conn.execute('DELETE FROM public.debits_reference_geo WHERE identifiant=ANY(%s)',(self.ids,))
            conn.execute('DELETE FROM public.geometries_versions WHERE identifiant=ANY(%s)',(self.ids,))
        with self.pg.connect(self.urls['bronze']) as conn:
            if conn.execute("SELECT to_regclass('public.geo_collectes_brutes')").fetchone()[0]:
                conn.execute('DELETE FROM public.geo_collectes_brutes WHERE collecte_id=ANY(%s)',(self.cids,))

    def snapshot(self,source,key=None,at=None):
        key=key or 'ci_geo_'+uuid4().hex
        self.ids.append(key)
        f=feature(key,source)
        if source=='debits':
            f['properties'].update(djma_annee_1=2025,val_djma_annee_1='1780.0')
        return dict(source=source,srid=4326,collecte_at=(at or datetime.now(timezone.utc)).isoformat(),
                    attendu=1,features=[f])

    def publish(self,snapshots,cid=None):
        cid=cid or uuid4();self.cids.append(cid)
        with self.pg.connect(self.urls['silver']) as conn:
            result=publier(cid,snapshots,conn)
        return cid,result

    def test_postgis_reference_and_replay(self):
        s=self.snapshot('debits');cid,result=self.publish([s])
        self.assertEqual(result['debits']['references'],1)
        _,again=self.publish([s],cid)
        self.assertTrue(again['debits']['deja_traitee'])
        with self.pg.connect(self.urls['silver']) as conn:
            row=conn.execute('SELECT annee_reference,valeur,temps_reel,unite '
                'FROM gold.fact_debit_reference WHERE section_trafic_id=%s',(self.ids[0],)).fetchone()
            self.assertEqual(row,(2025,Decimal('1780.0'),False,'vehicules/jour'))
            self.assertEqual(conn.execute('SELECT ST_SRID(geom),ST_IsValid(geom),GeometryType(geom) '
                'FROM public.geometries_versions WHERE identifiant=%s',(self.ids[0],)).fetchone(),
                (4326,True,'MULTILINESTRING'))

    def test_empty_new_snapshot_does_not_keep_disappeared_work(self):
        s=self.snapshot('travaux');self.publish([s])
        empty=dict(source='travaux',srid=4326,attendu=0,features=[],
                   collecte_at=(datetime.now(timezone.utc)+timedelta(seconds=2)).isoformat())
        self.publish([empty])
        with self.pg.connect(self.urls['silver']) as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM public.v_geometries_courantes '
                'WHERE source=%s AND identifiant=%s',('travaux',self.ids[0])).fetchone()[0],0)
            self.assertEqual(conn.execute('SELECT count(*) FROM public.geometries_versions '
                'WHERE identifiant=%s',(self.ids[0],)).fetchone()[0],1)

    def test_publication_is_rolled_back_if_transaction_fails(self):
        s=self.snapshot('routes');cid=uuid4();self.cids.append(cid)
        with self.assertRaises(self.pg.errors.DivisionByZero):
            with self.pg.connect(self.urls['silver']) as conn:
                publier(cid,[s],conn)
                conn.execute('SELECT 1/0')
        with self.pg.connect(self.urls['silver']) as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM public.geo_collectes_silver '
                                          'WHERE collecte_id=%s',(cid,)).fetchone()[0],0)
            self.assertEqual(conn.execute('SELECT count(*) FROM public.geometries_versions '
                                          'WHERE identifiant=%s',(self.ids[0],)).fetchone()[0],0)

    def test_http_bronze_postgis_and_gold(self):
        from urllib.parse import parse_qs, urlsplit
        from ingestion.contrats_geospatiaux import empreinte
        sample=self.snapshot('debits')['features'][0]
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):
                pass
            def do_GET(self):
                query=parse_qs(urlsplit(self.path).query)
                if query.get('resulttype')==['hits']:
                    body=b'<FeatureCollection numberMatched="1"/>'
                else:
                    body=json.dumps(dict(type='FeatureCollection',features=[sample])).encode()
                self.send_response(200);self.end_headers();self.wfile.write(body)
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        worker=threading.Thread(target=server.serve_forever,daemon=True);worker.start()
        @contextmanager
        def test_connection(role,readonly=False):
            with self.pg.connect(self.urls[role]) as conn:
                if readonly:conn.execute('SET TRANSACTION READ ONLY')
                yield conn
        try:
            with patch.object(bronze,'URL_WFS',f'http://127.0.0.1:{server.server_port}/wfs'):
                with bronze.session_http() as session:snapshot=bronze.collecter('debits',session)
            cid=uuid4();self.cids.append(cid)
            lot=dict(collecte_id=str(cid),sources=[snapshot])
            with patch.object(bronze,'connexion',test_connection):bronze.enregistrer(lot)
            manifest=dict(version=1,bronze_ecrit=True,collecte_id=str(cid),sources=[dict(
                source='debits',payload_hash=empreinte(snapshot),attendu=1)])
            with patch.object(silver,'connexion',test_connection):read_id,lots=silver.lire(manifest)
            with self.pg.connect(self.urls['silver']) as conn:publier(read_id,lots,conn)
            with self.pg.connect(self.urls['silver']) as conn:
                row=conn.execute('SELECT annee_reference,valeur FROM gold.fact_debit_reference '
                    'WHERE section_trafic_id=%s',(sample['properties']['ide_sectn_trafc'],)).fetchone()
                self.assertEqual(row,(2025,Decimal('1780.0')))
        finally:
            server.shutdown();server.server_close();worker.join(timeout=5)


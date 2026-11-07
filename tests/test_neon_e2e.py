"""HTTP contrôlé -> vrais collecteurs -> Bronze Neon -> vrai pipeline -> Silver Neon."""
from contextlib import contextmanager
import csv
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import sys
import threading
from urllib.parse import urlsplit
from unittest.mock import patch

from neon_support import NeonCase, TABLES, fixture_rows
from ingestion._contrats_csv import COLONNES


@contextmanager
def fixture_api(token):
    """Les données publiques sont substituées, jamais le pilote SQL ou les modules métier."""
    import requests
    from ingestion import ingestion_meteo as weather
    from ingestion import ingestion_travaux as works
    from ingestion._contrats_csv import SOURCES
    payloads = {}
    for source,cols in [('route',COLONNES['rtss']),('circulation',COLONNES['debits']),
                        ('travaux_bruts',works.COLONNES_SOURCE)]:
        text = io.StringIO(newline='')
        writer = csv.DictWriter(text,fieldnames=cols)
        writer.writeheader(); writer.writerows(fixture_rows(source,token))
        layer = works.PARAMETRES['typename'] if source=='travaux_bruts' else SOURCES['rtss' if source=='route' else 'debits']['layer']
        payloads[layer] = text.getvalue().encode('utf-8')

    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):
            pass
        def do_GET(self):
            from urllib.parse import parse_qs
            params = parse_qs(urlsplit(self.path).query)
            if self.path.startswith('/meteo'):
                start,end = date.fromisoformat(params['start_date'][0]),date.fromisoformat(params['end_date'][0])
                days = [(start+timedelta(days=i)).isoformat() for i in range((end-start).days+1)]
                daily = {'time':days,**{c:[0.0]*len(days) for c in weather.MESURES}}
                daily['temperature_2m_max']=[5.0]*len(days)
                daily['temperature_2m_min']=[-2.0]*len(days)
                hourly = {'time':[], 'visibility':[]}
                for day in days:
                    hourly['time'] += [day+'T00:00', day+'T01:00']
                    hourly['visibility'] += [1000.0,3000.0]
                body=json.dumps({'daily':daily,'hourly':hourly}).encode()
                content_type='application/json'
            else:
                body=payloads.get(params.get('typename',[''])[0])
                if body is None:
                    self.send_error(404); return
                content_type='text/csv; charset=utf-8'
            self.send_response(200)
            self.send_header('Content-Type',content_type)
            self.send_header('Content-Length',str(len(body)))
            self.end_headers(); self.wfile.write(body)

    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    worker=threading.Thread(target=server.serve_forever,daemon=True); worker.start()
    original=requests.sessions.Session.request
    def redirect(session,method,url,*args,**kwargs):
        host=urlsplit(url).hostname
        if host=='ws.mapserver.transports.gouv.qc.ca':
            target='/wfs'
        elif host in ('api.open-meteo.com','archive-api.open-meteo.com'):
            target='/meteo'
        else:
            raise AssertionError('Appel HTTP externe inattendu pendant le test contrôlé')
        return original(session,method,f'http://127.0.0.1:{server.server_port}'+target,*args,**kwargs)
    try:
        with patch.object(requests.sessions.Session,'request',redirect):
            yield
    finally:
        server.shutdown(); server.server_close(); worker.join(timeout=5)


class PipelineEndToEndTests(NeonCase):
    def collectors(self,dry_run=False):
        from ingestion import _csv_bronze as flux
        from ingestion import ingestion_meteo as weather
        from ingestion import ingestion_travaux as works
        env={'BRONZE_DATABASE_URL':self.urls['bronze'],'DATABASE_URL':self.urls['bronze']}
        flag=['--csv-seulement'] if dry_run else []
        # Les chemins et les connexions sont ceux du code réel; seul l'endpoint API est redirigé.
        try:
            with patch.dict(os.environ,env), fixture_api(self.token):
                with patch.object(sys,'argv',['ingestion_meteo.py','--date-debut','2014-01-01',
                                              '--date-fin','2014-01-02','--output-dir',str(self.folder/'meteo'),*flag]):
                    weather.main()
                for source in ('rtss','debits'):
                    with patch.object(sys,'argv',[source+'.py','--output-dir',str(self.folder/source),*flag]):
                        flux.executer(source)
                with patch.object(sys,'argv',['ingestion_travaux.py',*flag]):
                    works.main()
        finally:
            if not dry_run:
                with self.pg.connect(self.urls['bronze']) as conn:
                    for table,_ in TABLES.values():
                        self.stamps.update(str(r[0]) for r in conn.execute(self.sql.SQL(
                            'SELECT DISTINCT ingestion_timestamp FROM public.{}').format(self.sql.Identifier(table))).fetchall())

    def test_four_collectors_to_silver_then_replay(self):
        self.collectors()
        expected={'meteo':2,'route':1,'travaux_bruts':1,'circulation':1}
        first=self.pipeline(list(expected))
        for source,n in expected.items():
            row=first['resultats'][source]
            self.assertEqual((row['inserees'],row['rejetees']),(n,0),source)
            self.assertEqual(self.count('silver',TABLES[source][1]),n)
        second=self.pipeline(list(expected))
        for source,n in expected.items():
            row=second['resultats'][source]
            self.assertEqual((row['inserees'],row['deja_presentes']),(0,n),source)
        with self.pg.connect(self.urls['silver']) as conn:
            visibility=conn.execute('SELECT DISTINCT visibilite_moyenne FROM public.meteo').fetchall()
        self.assertEqual(visibility,[(2000,)])

    def test_csv_only_does_not_write_either_database(self):
        self.collectors(dry_run=True)
        for source,(bronze,silver) in TABLES.items():
            self.assertEqual(self.count('bronze',bronze),0,source)
            self.assertEqual(self.count('silver',silver),0,source)

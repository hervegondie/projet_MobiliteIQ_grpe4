"""Outils réservés à ci-test-123 : aucune suppression globale de données."""
from contextlib import contextmanager, redirect_stdout
import csv
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

from transformation.integration import CONTRACT

TABLES = {'meteo': ('meteo','meteo'), 'route': ('route','routes'),
          'travaux_bruts': ('travaux_bruts','travaux'),
          'circulation': ('circulation_debits_bruts','circulation')}


def validate_settings(env):
    """Contrôles locaux avant toute connexion; jamais de repli sur les secrets réels."""
    from psycopg.conninfo import conninfo_to_dict
    host = env.get('TEST_NEON_HOST','').strip()
    if not host:
        raise ValueError('Configurer la variable GitHub TEST_NEON_HOST avec l’hôte de ci-test-123')
    urls = {}
    for role, name, database in [('bronze','TEST_BRONZE_DATABASE_URL','mobiliteIQIngestion'),
                                  ('silver','TEST_SILVER_DATABASE_URL','Silver_test')]:
        value = env.get(name,'').strip()
        if not value:
            raise ValueError(f'Secret de test absent : {name}')
        try:
            options = conninfo_to_dict(value)
        except Exception:
            raise ValueError(f'{name} invalide; aucune connexion effectuée') from None
        if options.get('host') != host or options.get('dbname') != database:
            raise ValueError(f'{name} ne cible pas l’hôte de test et la base attendus')
        if options.get('sslmode') not in ('require','verify-ca','verify-full'):
            raise ValueError(f'{name} doit activer TLS')
        urls[role] = value
    return urls


def guarded_connections():
    import psycopg
    urls = validate_settings(os.environ)
    for role, url in urls.items():
        with psycopg.connect(url, connect_timeout=30) as conn:
            exists = conn.execute("SELECT to_regclass('public.ci_test_environment')").fetchone()[0]
            if not exists:
                raise ValueError('Marqueur ci_test_environment absent : aucune écriture de test autorisée')
            names = conn.execute('SELECT environment FROM public.ci_test_environment').fetchall()
            if names != [('ci-test-123',)]:
                raise ValueError('La base ne porte pas le marqueur unique ci-test-123')
    return urls


def fixture_rows(source, token):
    """Toutes les valeurs sont synthétiques et les identifiants uniques au test."""
    cols = [c for c in CONTRACT[source] if not c.startswith('ingestion_')]
    row = {c:'' for c in cols}
    if source == 'meteo':
        row.update(date='2014-01-01', temperature_max='5', temperature_min='-2',
                   precipitation_totale='0', pluie='0', neige='', vitesse_vent_max='12',
                   rafales_vent_max='20', visibilite_moyenne='1500')
    elif source == 'route':
        row.update(ide_sous_r='ci_'+token, num_rts='000350206531C0', num_route='00035',
                   chang_debu='0.0', val_longr_='444', dat_debut_='20240830', dah_cretn='20260306',
                   nom_sous_r='Pont "A", voie\nfermée')
    elif source == 'circulation':
        row.update(ide_sectn_trafc='ci_'+token, num_sectn_trafc='0007358032',
                   dat_debut_sectn_trafc='2019/02/05', djma_annee_1='2025',
                   val_djma_annee_1='1780.0', cam_annee_1='2025', val_cam_annee_1='7.5')
    else:
        row.update(identifiant='ci_'+token, identifiantChantier='ci_'+token, routeAutoroute='00035',
                   debut='2026-11-01T08:00:00-05:00', fin='2026-11-02T08:00:00-05:00',
                   descriptionFrancais='Chantier "A", voie\nfermée')
    return [row]


class NeonCase(unittest.TestCase):
    def setUp(self):
        if os.environ.get('RUN_NEON_TESTS') != '1':
            self.skipTest('Tests Neon désactivés; utiliser le workflow manuel réservé aux tests')
        import psycopg
        from psycopg import sql
        self.pg, self.sql = psycopg, sql
        self.urls = guarded_connections()
        self.lock = psycopg.connect(self.urls['bronze'], connect_timeout=30, autocommit=True)
        if not self.lock.execute('SELECT pg_try_advisory_lock(740221999)').fetchone()[0]:
            self.lock.close()
            raise RuntimeError('Un autre test Neon utilise ci-test-123')
        self.addCleanup(self.lock.close)
        self.tmp = tempfile.TemporaryDirectory(prefix='mobiliteiq_test_')
        self.addCleanup(self.tmp.cleanup)
        self.folder = Path(self.tmp.name)
        self.stamps = set()
        self.runs = []
        self.token = uuid4().hex
        # Ne pas mélanger les jeux synthétiques à des données déjà présentes.
        for role, url in self.urls.items():
            with psycopg.connect(url) as conn:
                for names in TABLES.values():
                    table = names[0 if role=='bronze' else 1]
                    n = conn.execute(sql.SQL('SELECT count(*) FROM public.{}').format(sql.Identifier(table))).fetchone()[0]
                    if n:
                        raise ValueError(f'public.{table} contient déjà des lignes : nettoyer uniquement les anciens jeux de test avant de relancer')

    def tearDown(self):
        if not hasattr(self,'stamps'):
            return
        # Suppression limitée aux horodatages/identifiants des jeux créés par CE test.
        for role, url in self.urls.items():
            with self.pg.connect(url) as conn:
                if role == 'silver':
                    for _, table in TABLES.values():
                        conn.execute(self.sql.SQL('DELETE FROM public.{} WHERE _silver_run=ANY(%s)').format(
                            self.sql.Identifier(table)), (self.runs,))
                    conn.execute('DELETE FROM public.pipeline_silver_rejets WHERE run_id=ANY(%s)', (self.runs,))
                    conn.execute('DELETE FROM public.pipeline_silver_runs WHERE run_id=ANY(%s)', (self.runs,))
                elif self.stamps:
                    for table, _ in TABLES.values():
                        conn.execute(self.sql.SQL('DELETE FROM public.{} WHERE ingestion_timestamp::timestamptz=ANY(%s::timestamptz[])').format(
                            self.sql.Identifier(table)), (list(self.stamps),))

    def prepare_csv(self, source, rows=None):
        rows = rows or fixture_rows(source,self.token)
        cols = [c for c in CONTRACT[source] if not c.startswith('ingestion_')]
        stamp = datetime.now(timezone.utc)
        self.stamps.add(stamp.isoformat())
        path = self.folder/(source+'_'+uuid4().hex+'.csv')
        with path.open('w',encoding='utf-8',newline='') as f:
            w = csv.writer(f)
            metadata = ['ingestion_timestamp'] if source=='meteo' else ['ingestion_timestamp','ingestion_date','ingestion_time']
            w.writerow(cols+metadata)
            for row in rows:
                values = [stamp.isoformat()] if source=='meteo' else [stamp.isoformat(), stamp.date().isoformat(), stamp.time().replace(tzinfo=None).isoformat()]
                w.writerow([row[c] for c in cols]+values)
        return path, cols, len(rows)

    def ingest(self, source, rows=None):
        from ingestion import _csv_bronze as flux
        from ingestion import ingestion_meteo as weather
        path, cols, count = self.prepare_csv(source,rows)
        if source=='meteo':
            weather.importer_csv(path,self.urls['bronze'])
        else:
            flux.importer_csv(path,TABLES[source][0],cols,self.urls['bronze'],count)
        return path

    def pipeline(self, sources, apply=True):
        from transformation import transformation_4_sources as pipeline
        args = ['transformation_4_sources.py','--sources',*sources]
        if apply:
            args.append('--apply')
        env = {'BRONZE_DATABASE_URL':self.urls['bronze'], 'DATABASE_URL':self.urls['bronze'],
               'SILVER_DATABASE_URL':self.urls['silver']}
        output = io.StringIO()
        with patch.dict(os.environ,env), patch.object(sys,'argv',args), redirect_stdout(output):
            pipeline.main()
        report = json.loads(output.getvalue()[output.getvalue().find('{'):])
        if apply:
            self.runs.append(report['run_id'])
        return report

    def count(self, role, table):
        with self.pg.connect(self.urls[role]) as conn:
            return conn.execute(self.sql.SQL('SELECT count(*) FROM public.{}').format(self.sql.Identifier(table))).fetchone()[0]

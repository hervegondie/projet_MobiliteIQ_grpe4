"""Intégration réelle PostgreSQL, exclusivement sur ci-test-123."""
from datetime import date
from decimal import Decimal
from neon_support import NeonCase, fixture_rows


class PostgreSQLIntegrationTests(NeonCase):
    def test_copy_preserves_text_null_and_metadata(self):
        self.ingest('route')
        with self.pg.connect(self.urls['bronze']) as conn:
            row = conn.execute('SELECT num_route,nom_sous_r,des_type_r FROM public.route').fetchone()
        self.assertEqual(row,('00035','Pont "A", voie\nfermée',None))

    def test_failed_copy_rolls_back_new_rows(self):
        from ingestion import _csv_bronze as flux
        self.ingest('route')
        path,cols,count = self.prepare_csv('route')
        with self.assertRaisesRegex(ValueError,'transaction annulée'):
            flux.importer_csv(path,'route',cols,self.urls['bronze'],count+1)
        self.assertEqual(self.count('bronze','route'),1)

    def test_real_silver_types(self):
        self.ingest('route'); self.pipeline(['route'])
        with self.pg.connect(self.urls['silver']) as conn:
            row = conn.execute('SELECT num_route,dat_debut_,val_longr_ FROM public.routes').fetchone()
        self.assertEqual(row,('00035',date(2024,8,30),Decimal(444)))

    def test_invalid_row_is_saved_in_rejects(self):
        row = fixture_rows('circulation',self.token)[0]
        bad = dict(row,ide_sectn_trafc='ci_bad_'+self.token,val_cam_annee_1='101')
        self.ingest('circulation',[row,bad])
        result = self.pipeline(['circulation'])['resultats']['circulation']
        self.assertEqual((result['inserees'],result['rejetees']),(1,1))
        with self.pg.connect(self.urls['silver']) as conn:
            reasons = conn.execute('SELECT raisons FROM public.pipeline_silver_rejets WHERE run_id=%s',(self.runs[-1],)).fetchone()[0]
        self.assertTrue(any('100' in r for r in reasons))

    def test_empty_source_rolls_back_whole_silver_transaction(self):
        self.ingest('route')
        with self.assertRaisesRegex(ValueError,'source vide'):
            self.pipeline(['route','meteo'])
        self.assertEqual(self.count('silver','routes'),0)
        self.assertEqual(self.count('silver','pipeline_silver_runs'),0)

    def test_replay_does_not_insert_duplicates(self):
        self.ingest('route')
        first = self.pipeline(['route'])['resultats']['route']
        second = self.pipeline(['route'])['resultats']['route']
        self.assertEqual(first['inserees'],1)
        self.assertEqual((second['inserees'],second['deja_presentes']),(0,1))
        self.assertEqual(self.count('silver','routes'),1)

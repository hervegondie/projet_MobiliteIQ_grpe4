"""Fraicheur Gold : aucune connexion ni dependance psycopg necessaire."""
from datetime import datetime, timedelta, timezone
import sys
from types import ModuleType
from unittest import TestCase
from unittest.mock import Mock, patch

from transformation.fraicheur_geospatiale import verifier_sources
from transformation import correspondances_geospatiales as corr


class FraicheurSourcesTests(TestCase):
    def setUp(self):
        self.now=datetime(2026,11,14,14,tzinfo=timezone.utc)
        self.sources={s:dict(collecte_at=(self.now-timedelta(hours=1)).isoformat())
                      for s in ['routes','travaux','debits']}

    def test_limite_quatre_heures_acceptee(self):
        self.sources['travaux']['collecte_at']=(self.now-timedelta(hours=4)).isoformat()
        self.assertEqual(verifier_sources(self.sources,self.now)['travaux'],4)

    def test_depassement_meme_minime_refuse(self):
        self.sources['travaux']['collecte_at']=(self.now-timedelta(hours=4,microseconds=1)).isoformat()
        with self.assertRaisesRegex(ValueError,'travaux'):verifier_sources(self.sources,self.now)

    def test_source_absente_explicite(self):
        del self.sources['travaux']
        with self.assertRaisesRegex(ValueError,'absente ou invalide'):verifier_sources(self.sources,self.now)

    def test_date_invalide_explicite(self):
        self.sources['travaux']['collecte_at']='invalide'
        with self.assertRaises(ValueError):verifier_sources(self.sources,self.now)

    def test_date_naive_refusee(self):
        self.sources['travaux']['collecte_at']='2026-11-14T12:00:00'
        with self.assertRaisesRegex(ValueError,'sans fuseau'):verifier_sources(self.sources,self.now)

    def test_instant_controle_naif_refuse(self):
        with self.assertRaises(ValueError):verifier_sources(self.sources,self.now.replace(tzinfo=None))


class PublicationProtegeeTests(TestCase):
    def setUp(self):
        self.now=datetime(2026,11,14,14,tzinfo=timezone.utc)
        self.sources={s:dict(collecte_at=(self.now-timedelta(hours=1)).isoformat())
                      for s in ['routes','travaux','debits']}
        self.conn=Mock()
        self.sql=[]
        self.old=None
        def execute(query,*args,**kwargs):
            self.sql.append(query)
            r=Mock()
            if 'to_regclass' in query:r.fetchone.return_value=('gold.correspondance_runs',)
            elif 'SELECT run_id,bilan' in query:r.fetchone.return_value=self.old
            elif 'count(DISTINCT troncon_id)' in query:r.fetchone.return_value=(1,1)
            elif 'SELECT count(*) FROM (' in query:r.fetchone.return_value=(0,)
            return r
        self.conn.execute.side_effect=execute
        self.conn.cursor.return_value.__enter__=Mock(return_value=Mock())
        self.conn.cursor.return_value.__exit__=Mock(return_value=False)
        json_module=ModuleType('psycopg.types.json');json_module.Jsonb=lambda x:x
        self.modules=patch.dict(sys.modules,{'psycopg.types.json':json_module})
        self.modules.start();self.addCleanup(self.modules.stop)
        self.config=patch.object(corr,'valider_config',return_value={});self.config.start();self.addCleanup(self.config.stop)
        self.input=patch.object(corr,'entrees',side_effect=lambda *_:(self.sources,{},'empreinte'))
        self.input.start();self.addCleanup(self.input.stop)
        self.prepare=patch.object(corr,'preparer_tables');self.prepare_mock=self.prepare.start();self.addCleanup(self.prepare.stop)
        self.summary=patch.object(corr,'bilan',return_value=dict(troncons=1));self.summary.start();self.addCleanup(self.summary.stop)

    def run_case(self,times,apply=True):
        with patch.object(corr,'datetime') as clock:
            clock.now.side_effect=times
            return corr.construire(self.conn,{},[dict(lieu_id='test')],apply)

    def test_travaux_anciens_refuses_avant_calcul_et_insert(self):
        self.sources['travaux']['collecte_at']=(self.now-timedelta(hours=7)).isoformat()
        with self.assertRaises(ValueError):self.run_case([self.now])
        self.prepare_mock.assert_not_called()
        self.assertFalse(any(q.startswith('INSERT') for q in self.sql))

    def test_rejeu_ancien_ne_republie_pas(self):
        self.old=('run_existant',{})
        self.sources['travaux']['collecte_at']=(self.now-timedelta(hours=7)).isoformat()
        with self.assertRaises(ValueError):self.run_case([self.now])
        self.assertFalse(any('INSERT INTO gold.correspondance_publication' in q for q in self.sql))

    def test_source_expire_pendant_calcul_ne_change_pas_pointeur(self):
        with self.assertRaises(ValueError):self.run_case([self.now,self.now+timedelta(hours=4)])
        self.assertFalse(any('INSERT INTO gold.correspondance_publication' in q for q in self.sql))

    def test_rejeu_frais_controle_puis_reutilise(self):
        self.old=('run_existant',{})
        r=self.run_case([self.now,self.now])
        self.assertTrue(r['deja_traite']);self.prepare_mock.assert_not_called()
        self.assertTrue(any('INSERT INTO gold.correspondance_publication' in q for q in self.sql))

    def test_preview_ne_modifie_pas_publication(self):
        r=self.run_case([self.now],apply=False)
        self.assertIsNone(r['run_id'])
        self.assertFalse(any(q.startswith('INSERT INTO gold.') for q in self.sql))

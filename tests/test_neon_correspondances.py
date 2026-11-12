"""Vrais calculs PostGIS sur donnees synthetiques de ci-test-123 uniquement."""
from datetime import datetime,timedelta,timezone
from uuid import uuid4
import copy

from neon_support import NeonCase
from transformation.transformation_geospatiale import publier,MIGRATION
from transformation.correspondances_geospatiales import construire,SCHEMA
from transformation.regles_correspondances import valider_config


def nettoyer_runs_gold(conn,run_ids):
    """Supprimer uniquement les runs du test, enfants avant parents.

    Les deux FK vers points_meteo_correspondance ne sont pas en cascade.
    Ne pas dependre de l'ordre des cascades declenchees depuis le run.
    """
    ids=list(dict.fromkeys(run_ids))
    if not ids:return
    for table in ['pont_troncon_meteo','pont_troncon_travaux','pont_troncon_debit',
                  'correspondances_non_appariees','correspondance_publication',
                  'troncons_correspondance','points_meteo_correspondance','correspondance_runs']:
        conn.execute(f'DELETE FROM gold.{table} WHERE run_id=ANY(%s)',(ids,))


class CorrespondancesPostgisTests(NeonCase):
    def setUp(self):
        super().setUp()
        self.prefix='ci_corr_'+uuid4().hex
        self.cid=uuid4();self.geo_runs=[];self.keys=[]
        self.addCleanup(self.clean_correspondances)
        self.cfg=valider_config(dict(version=1,bbox=[-71.65,46.65,-70.95,47.05],meteo_rayon_m=10000))
        self.points=[dict(lieu_id='test_quebec',nom='Point synthetique',latitude=46.8,longitude=-71.195)]
        now=datetime.now(timezone.utc)
        self.now=now
        roads=[self.make('r1',[[-71.2,46.8],[-71.19,46.8]],num_route='00035',num_rts='000350206531C0'),
               self.make('r2',[[-71.2,46.81],[-71.19,46.81]],num_route='00035',num_rts='000350206531D0'),
               self.make('r3',[[-71.60,47.03],[-71.59,47.03]],num_route='00035',num_rts='000350206531E0')]
        works=[self.make('w1',roads[0]['geometry']['coordinates'],routeAutoroute='35',
                        identifiantChantier=self.prefix+'_chantier',debut=(now-timedelta(days=2)).isoformat(),fin=(now+timedelta(days=2)).isoformat()),
               self.make('w2',roads[0]['geometry']['coordinates'],routeAutoroute='35',
                        identifiantChantier=self.prefix+'_chantier',debut=(now-timedelta(days=2)).isoformat(),fin=(now+timedelta(days=2)).isoformat()),
               self.make('w_voisine',[[-71.2,46.80004],[-71.19,46.80004]],routeAutoroute='35')]
        debits=[self.make('d1',roads[0]['geometry']['coordinates'],rtss_debut='00035-02-065-31C0',rtss_fin='00035-02-065-31C0',djma_annee_1=2025,val_djma_annee_1='10000'),
                self.make('d2',roads[1]['geometry']['coordinates'],rtss_debut='00035-02-065-31D0',rtss_fin='00035-02-065-31D0',djma_annee_1=2025,val_djma_annee_1='20000'),
                self.make('d3',roads[1]['geometry']['coordinates'],rtss_debut='00035-02-065-31D0',rtss_fin='00035-02-065-31D0',djma_annee_1=2025,val_djma_annee_1='30000')]
        for f in roads:f['properties']['ide_sous_r']=f['id']
        for f in works:f['properties']['identifiant']=f['id']
        for f in debits:f['properties']['ide_sectn_trafc']=f['id']
        lots=[dict(source=s,srid=4326,attendu=len(fs),features=fs,
                   collecte_at=(now+timedelta(seconds=2)).isoformat())
              for s,fs in [('routes',roads),('travaux',works),('debits',debits)]]
        with self.pg.connect(self.urls['silver']) as conn:
            conn.execute(MIGRATION.read_text(encoding='utf-8'),prepare=False)
            conn.execute(SCHEMA.read_text(encoding='utf-8'),prepare=False)
            publier(self.cid,lots,conn)

    def make(self,key,coords,**props):
        key=self.prefix+'_'+key;self.keys.append(key)
        return dict(type='Feature',id=key,properties=props,
                    geometry=dict(type='LineString',coordinates=coords))

    def clean_correspondances(self):
        with self.pg.connect(self.urls['silver']) as conn:
            if conn.execute("SELECT to_regclass('gold.correspondance_runs')").fetchone()[0] and self.geo_runs:
                nettoyer_runs_gold(conn,self.geo_runs)
            for name in ['geo_rejets','geo_observations','geo_collectes_silver']:
                if conn.execute('SELECT to_regclass(%s)',('public.'+name,)).fetchone()[0]:
                    conn.execute(self.sql.SQL('DELETE FROM public.{} WHERE collecte_id=%s').format(
                        self.sql.Identifier(name)),(self.cid,))
            if conn.execute("SELECT to_regclass('public.debits_reference_geo')").fetchone()[0]:
                conn.execute('DELETE FROM public.debits_reference_geo WHERE identifiant=ANY(%s)',(self.keys,))
                conn.execute('DELETE FROM public.geometries_versions WHERE identifiant=ANY(%s)',(self.keys,))

    def calculate(self,apply=True):
        from uuid import UUID
        with self.pg.connect(self.urls['silver']) as conn:
            result=construire(conn,self.cfg,self.points,apply)
            if result.get('run_id'):self.geo_runs.append(UUID(result['run_id']))
        return result

    def test_preview_does_not_publish_and_replay_is_idempotent(self):
        preview=self.calculate(False);self.assertIsNone(preview['run_id'])
        self.assertEqual(preview['bilan']['troncons'],3)
        first=self.calculate();second=self.calculate()
        self.assertEqual(first['run_id'],second['run_id']);self.assertTrue(second['deja_traite'])

    def test_weather_outside_radius_is_not_forced(self):
        self.calculate()
        with self.pg.connect(self.urls['silver']) as conn:
            row=conn.execute('SELECT lieu_id,statut FROM gold.v_correspondances_meteo WHERE troncon_id=%s',
                             (self.prefix+'_r3',)).fetchone()
            self.assertEqual(row,(None,'hors_couverture'))

    def test_no_double_count_for_two_entraves_on_same_chantier(self):
        self.calculate()
        with self.pg.connect(self.urls['silver']) as conn:
            row=conn.execute('SELECT nb_entraves_actives_probables,nb_chantiers_actifs_probables,'
                'longueur_proximite_active_m,longueur_m FROM gold.v_contexte_troncon_courant '
                'WHERE troncon_id=%s',(self.prefix+'_r1',)).fetchone()
            self.assertEqual(row[:2],(2,1));self.assertLessEqual(row[2],row[3])
            self.assertGreater(row[2],row[3]*0.99)
            self.assertEqual(conn.execute('SELECT statut FROM gold.v_correspondances_travaux '
                'WHERE travaux_id=%s AND troncon_id=%s',(self.prefix+'_w_voisine',self.prefix+'_r1')).fetchone()[0],
                'a_verifier')

    def test_ambiguous_debit_is_not_given_a_djma(self):
        self.calculate()
        with self.pg.connect(self.urls['silver']) as conn:
            row=conn.execute('SELECT djma_reference FROM gold.v_contexte_troncon_courant '
                             'WHERE troncon_id=%s',(self.prefix+'_r2',)).fetchone()
            self.assertIsNone(row[0])
            self.assertEqual(conn.execute('SELECT count(*) FROM gold.v_contexte_troncon_courant').fetchone()[0],3)
            row=conn.execute('SELECT djma_reference,annee_djma,debit_temps_reel '
                'FROM gold.v_contexte_troncon_courant WHERE troncon_id=%s',(self.prefix+'_r1',)).fetchone()
            self.assertEqual(row,(10000,2025,False))

    def test_same_rtss_normalisation_in_python_and_sql(self):
        from transformation.regles_correspondances import normaliser_rtss
        with self.pg.connect(self.urls['silver']) as conn:
            for value in ['00035-02-065-31C0','000350206531C0','35-02-065-31C0','00035-02-065-31C0 (0)']:
                self.assertEqual(conn.execute('SELECT gold.normaliser_rtss(%s)',(value,)).fetchone()[0],normaliser_rtss(value))


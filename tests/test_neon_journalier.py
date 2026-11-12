"""Faits historiques : vrais SQL sur ci-test-123 seulement, aucune source publique appelee."""
from datetime import date,datetime,timedelta,timezone
from uuid import uuid4,UUID

from neon_support import NeonCase
from test_neon_correspondances import nettoyer_runs_gold
from transformation.transformation_geospatiale import MIGRATION
from transformation.correspondances_geospatiales import SCHEMA as SCHEMA_CORR
from transformation.situations_journalieres import SCHEMA,charger_jour
from transformation.regles_journalieres import fenetre_jour
from transformation import transformation_meteo_quotidienne_points as meteo
from transformation.integration_meteo_horaire import inserer


class SituationsJournalieresNeonTests(NeonCase):
    def setUp(self):
        super().setUp()
        self.day=date(2014,1,1);self.start,self.end=fenetre_jour(self.day)
        self.cutoff=self.end+timedelta(hours=12)
        self.prefix='ci_jour_'+uuid4().hex;self.troncon=self.prefix+'_r1'
        self.cid=uuid4();self.daily_runs=[];self.weather_ids=[];self.corr_ids=[self.cid]
        self.addCleanup(self.clean_daily)
        from psycopg.types.json import Jsonb
        with self.pg.connect(self.urls['silver']) as conn:
            conn.execute(MIGRATION.read_text(encoding='utf-8'),prepare=False)
            meteo.initialiser(conn)
            conn.execute(SCHEMA_CORR.read_text(encoding='utf-8'),prepare=False)
            conn.execute(SCHEMA.read_text(encoding='utf-8'),prepare=False)
            sources={s:dict(collecte_id=str(uuid4()),collecte_at=(self.end-timedelta(hours=1)).isoformat())
                     for s in ['routes','travaux','debits']}
            conn.execute('INSERT INTO gold.correspondance_runs '
                '(run_id,empreinte_entrees,algorithme,cree_at,sources,configuration,bilan) '
                'VALUES (%s,%s,%s,%s,%s,%s,%s)',(self.cid,self.prefix,'correspondances-1',
                self.end-timedelta(minutes=30),Jsonb(sources),Jsonb({}),Jsonb({})))
            conn.execute('INSERT INTO gold.points_meteo_correspondance VALUES (%s,%s,%s,'
                'ST_SetSRID(ST_MakePoint(-71.195,46.8),4326))',(self.cid,self.prefix,self.prefix))
            conn.execute('INSERT INTO gold.troncons_correspondance '
                'SELECT %s,%s,%s,%s,%s,%s,g,ST_Length(ST_Transform(g,32198)) '
                'FROM(SELECT ST_Multi(ST_GeomFromText(\'LINESTRING(-71.2 46.8,-71.19 46.8)\',4326)) g) q',
                (self.cid,self.troncon,self.prefix,'00035','000350206531C0',Jsonb({'des_clasf_':'Autoroute'})))
            conn.execute('INSERT INTO gold.pont_troncon_meteo VALUES (%s,%s,%s,%s,0,1000,NULL,%s)',
                         (self.cid,self.troncon,self.prefix,self.prefix,'representatif'))
            for n in [1,2]:
                conn.execute('INSERT INTO gold.pont_troncon_travaux '
                    'SELECT %s,%s,%s,%s,%s,0,ST_Length(g),1,ST_Length(g),true,%s,%s,%s,%s,g '
                    'FROM(SELECT ST_Multi(ST_Transform(ST_GeomFromText('
                    '\'LINESTRING(-71.2 46.8,-71.19 46.8)\',4326),32198)) g) q',
                    (self.cid,self.troncon,self.prefix+f'_w{n}',self.prefix+'_chantier',self.prefix,
                     self.start,self.end+timedelta(days=2),'ok','probable'))
        self.add_weather(-2,7)

    def add_weather(self,temp,hours):
        ident=uuid4();self.weather_ids.append(ident)
        collected=self.end+timedelta(hours=hours)
        row=dict(collecte_id=ident,lieu_id=self.prefix,latitude=46.8,longitude=-71.195,
            latitude_modele=46.8,longitude_modele=-71.195,collecte_at=collected,date=self.day,
            temperature_min=temp,temperature_max=5,precipitation_totale=1,pluie=0,neige=1,
            vitesse_vent_max=12,rafales_vent_max=20,visibilite_moyenne=1000)
        with self.pg.connect(self.urls['silver']) as conn:
            inserer(conn,'meteo_quotidienne_points',meteo.COLONNES,[row])
            inserer(conn,'meteo_quotidienne_points_runs',meteo.RUNS,[dict(collecte_id=ident,
                lieu_id=self.prefix,collecte_at=collected,processed_at=collected+timedelta(minutes=1),
                payload_hash=str(ident),nb_jours=1)])

    def calculate(self,cutoff=None,apply=True):
        with self.pg.connect(self.urls['silver']) as conn:
            result=charger_jour(conn,self.day,cutoff or self.cutoff,apply=apply)
            if result.get('run_id'):self.daily_runs.append(UUID(result['run_id']))
        return result

    def clean_daily(self):
        with self.pg.connect(self.urls['silver']) as conn:
            if conn.execute("SELECT to_regclass('gold.situation_jour_runs')").fetchone()[0]:
                for table in ['situation_jour_publication','fact_situation_journaliere','situation_jour_runs']:
                    conn.execute(f'DELETE FROM gold.{table} WHERE run_id=ANY(%s)',(self.daily_runs,))
                conn.execute('DELETE FROM gold.dim_troncon_historique WHERE troncon_id=%s',(self.troncon,))
            if conn.execute("SELECT to_regclass('gold.correspondance_runs')").fetchone()[0]:
                nettoyer_runs_gold(conn,self.corr_ids)
            if self.weather_ids:
                for table in ['meteo_quotidienne_points','meteo_quotidienne_points_runs']:
                    conn.execute(f'DELETE FROM public.{table} WHERE collecte_id=ANY(%s)',(self.weather_ids,))

    def test_daily_snapshot_no_double_count_and_replay(self):
        preview=self.calculate(apply=False);self.assertIsNone(preview['run_id'])
        first=self.calculate();second=self.calculate()
        self.assertEqual(first['run_id'],second['run_id']);self.assertTrue(second['deja_traite'])
        with self.pg.connect(self.urls['silver']) as conn:
            row=conn.execute('SELECT nb_entraves_actives,nb_chantiers_actifs,qualite_ok,'
                'part_longueur_entravee FROM gold.v_situation_journaliere WHERE troncon_id=%s',(self.troncon,)).fetchone()
            self.assertEqual(row[:3],(2,1,1));self.assertLessEqual(row[3],1)

    def test_weather_revision_is_versioned_without_future_leakage(self):
        first=self.calculate(self.end+timedelta(hours=8))
        self.add_weather(-9,10)
        replay=self.calculate(self.end+timedelta(hours=8))
        self.assertEqual(first['run_id'],replay['run_id'])
        revised=self.calculate(self.end+timedelta(hours=12))
        self.assertNotEqual(first['run_id'],revised['run_id'])
        with self.pg.connect(self.urls['silver']) as conn:
            self.assertEqual(conn.execute('SELECT temperature_min FROM gold.v_situation_journaliere '
                'WHERE troncon_id=%s',(self.troncon,)).fetchone()[0],-9)
            self.assertEqual(conn.execute('SELECT count(*) FROM gold.fact_situation_journaliere '
                'WHERE troncon_id=%s',(self.troncon,)).fetchone()[0],2)

    def test_old_work_source_is_unknown_not_zero_and_excluded_from_ml(self):
        from psycopg.types.json import Jsonb
        with self.pg.connect(self.urls['silver']) as conn:
            sources=conn.execute('SELECT sources FROM gold.correspondance_runs WHERE run_id=%s',(self.cid,)).fetchone()[0]
            sources['travaux']['collecte_at']=(self.end-timedelta(hours=10)).isoformat()
            conn.execute('UPDATE gold.correspondance_runs SET sources=%s WHERE run_id=%s',(Jsonb(sources),self.cid))
        self.calculate()
        with self.pg.connect(self.urls['silver']) as conn:
            row=conn.execute('SELECT nb_entraves_actives,qualite_ok FROM gold.v_situation_journaliere '
                'WHERE troncon_id=%s',(self.troncon,)).fetchone()
            self.assertEqual(row,(None,0))
            self.assertEqual(conn.execute('SELECT count(*) FROM gold.v_ml_situation_journaliere '
                'WHERE troncon_id=%s',(self.troncon,)).fetchone()[0],0)

    def test_no_reconstruction_from_a_later_correspondence(self):
        with self.pg.connect(self.urls['silver']) as conn:
            conn.execute('UPDATE gold.correspondance_runs SET cree_at=%s WHERE run_id=%s',
                         (self.end+timedelta(hours=1),self.cid))
        result=self.calculate()
        self.assertEqual(result['statut'],'historique_correspondances_indisponible')
        self.assertEqual(result['lignes'],0)


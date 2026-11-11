"""Correspondances geographiques versionnees dans Gold (Quebec, sans ML)."""
from pathlib import Path
from uuid import uuid4
from datetime import datetime, timezone
import argparse
import json
import sys

if __package__ in (None,''):
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from transformation.integration_meteo_horaire import connexion
from transformation.regles_correspondances import ALGORITHME,valider_config,routes_travaux,dates_travaux
from ingestion.points_meteo import charger_points
from ingestion.contrats_geospatiaux import empreinte

ROOT=Path(__file__).resolve().parents[1]
SCHEMA=ROOT/'sql/gold/003_correspondances_schema.sql'
CALCUL=ROOT/'sql/gold/004_calcul_correspondances.sql'


def entrees(conn,config,points):
    sources=conn.execute('SELECT DISTINCT ON(source) source,collecte_id,collecte_at '
        'FROM public.geo_collectes_silver ORDER BY source,collecte_at DESC,collecte_id DESC').fetchall()
    if {row[0] for row in sources}!={'routes','travaux','debits'}:
        raise ValueError('Trois collectes geographiques publiees requises : routes, travaux, debits')
    metadata={source:dict(collecte_id=str(cid),collecte_at=at.isoformat()) for source,cid,at in sources}
    configuration=dict(parametres=config,points=points)
    h=empreinte(dict(algorithme=ALGORITHME,sources=metadata,configuration=configuration))
    return metadata,configuration,h


def preparer_tables(conn,sources,config,points):
    from psycopg.types.json import Jsonb
    conn.execute('CREATE TEMP TABLE g2_config(cfg jsonb) ON COMMIT DROP')
    conn.execute('INSERT INTO g2_config VALUES (%s)',(Jsonb(config),))
    conn.execute('CREATE TEMP TABLE g2_points(lieu_id text PRIMARY KEY,nom text,'
                 'geom geometry(Point,4326),geom_m geometry(Point,32198)) ON COMMIT DROP')
    with conn.cursor() as cur:
        cur.executemany('INSERT INTO g2_points SELECT %s,%s,g,ST_Transform(g,32198) '
            'FROM(SELECT ST_SetSRID(ST_MakePoint(%s,%s),4326) AS g) q',
            [(p['lieu_id'],p.get('nom',p['lieu_id']),p['longitude'],p['latitude']) for p in points])
    conn.execute('CREATE TEMP TABLE g2_routes ON COMMIT DROP AS '
        'WITH base AS (SELECT g.*,ST_Transform(g.geom,32198) AS geom_m '
        'FROM public.geo_observations o JOIN public.geometries_versions g '
        'USING(source,identifiant,empreinte) WHERE o.source=\'routes\' AND o.collecte_id=%s '
        'AND ST_Intersects(g.geom,ST_MakeEnvelope(%s,%s,%s,%s,4326))) '
        'SELECT *,proprietes->>\'num_route\' AS numero_route,'
        'gold.normaliser_rtss(proprietes->>\'num_rts\') AS rtss,'
        'ST_Length(geom_m) AS longueur_m,ST_ClosestPoint(geom_m,ST_Centroid(geom_m)) AS repere_m '
        'FROM base', (sources['routes']['collecte_id'],*config['bbox']))
    if conn.execute('SELECT count(*) FROM g2_routes').fetchone()[0]==0:
        raise ValueError('Aucun troncon dans le perimetre : verifier la bbox')
    for source,name in [('travaux','g2_travaux'),('debits','g2_debits')]:
        # Les noms de tables ci-dessus sont constants, pas fournis par le manifest.
        conn.execute(f'CREATE TEMP TABLE {name} ON COMMIT DROP AS '
            'SELECT g.*,ST_Transform(g.geom,32198) AS geom_m '
            'FROM public.geo_observations o JOIN public.geometries_versions g '
            'USING(source,identifiant,empreinte) WHERE o.source=%s AND o.collecte_id=%s',
            (source,sources[source]['collecte_id']))
    conn.execute('ALTER TABLE g2_travaux ADD COLUMN numeros_routes text[] NOT NULL DEFAULT \'{}\','
                 'ADD COLUMN debut timestamptz,ADD COLUMN fin timestamptz,'
                 'ADD COLUMN statut_dates text NOT NULL DEFAULT \'inconnues\','
                 'ADD COLUMN dans_perimetre boolean')
    conn.execute('UPDATE g2_travaux SET dans_perimetre=ST_Intersects(geom,ST_MakeEnvelope(%s,%s,%s,%s,4326))',
                 tuple(config['bbox']))
    rows=conn.execute('SELECT identifiant,proprietes FROM g2_travaux').fetchall()
    with conn.cursor() as cur:
        cur.executemany('UPDATE g2_travaux SET numeros_routes=%s,debut=%s,fin=%s,statut_dates=%s '
            'WHERE identifiant=%s',[(routes_travaux(p.get('routeAutoroute')),*dates_travaux(p),key)
                                    for key,p in rows])
    conn.execute('ALTER TABLE g2_debits ADD COLUMN rtss_debut text,ADD COLUMN rtss_fin text')
    conn.execute('UPDATE g2_debits SET rtss_debut=gold.normaliser_rtss(proprietes->>\'rtss_debut\'),'
                 'rtss_fin=gold.normaliser_rtss(proprietes->>\'rtss_fin\')')
    conn.execute(CALCUL.read_text(encoding='utf-8'),prepare=False)


def bilan(conn,sources):
    result={}
    for table,column in [('g2_meteo','statut'),('g2_pont_travaux','statut'),('g2_pont_debits','statut')]:
        result[table.removeprefix('g2_')]=dict(conn.execute(
            f'SELECT {column},count(*) FROM {table} GROUP BY {column}').fetchall())
    result['troncons']=conn.execute('SELECT count(*) FROM g2_routes').fetchone()[0]
    result['non_apparies']=dict(conn.execute('SELECT type_objet,count(*) FROM g2_non_apparies '
                                            'GROUP BY type_objet').fetchall())
    now=datetime.now(timezone.utc)
    result['age_sources_heures']={s:round((now-datetime.fromisoformat(x['collecte_at'])).total_seconds()/3600,2)
                                  for s,x in sources.items()}
    result['date_travaux_inconnue']=conn.execute('SELECT count(*) FROM g2_travaux '
        'WHERE statut_dates=\'inconnues\'').fetchone()[0]
    return result


def construire(conn,config,points,apply=False):
    from psycopg.types.json import Jsonb
    config=valider_config(config)
    if not points or len({p['lieu_id'] for p in points})!=len(points):
        raise ValueError('Liste de points vide ou dupliquee')
    if not conn.execute("SELECT to_regclass('gold.correspondance_runs')").fetchone()[0]:
        raise ValueError('Executer --installer pour installer les tables de correspondance')
    conn.execute('SELECT pg_advisory_xact_lock(740221203)')
    sources,configuration,h=entrees(conn,config,points)
    if apply:
        old=conn.execute('SELECT run_id,bilan FROM gold.correspondance_runs '
                         'WHERE empreinte_entrees=%s',(h,)).fetchone()
        if old:
            conn.execute('INSERT INTO gold.correspondance_publication(singleton,run_id) VALUES (true,%s) '
                'ON CONFLICT(singleton) DO UPDATE SET run_id=EXCLUDED.run_id,publie_at=now() '
                'WHERE gold.correspondance_publication.run_id<>EXCLUDED.run_id',(old[0],))
            return dict(mode='chargement',run_id=str(old[0]),deja_traite=True,bilan=old[1])
    preparer_tables(conn,sources,config,points)
    result=bilan(conn,sources)
    report=dict(mode='chargement' if apply else 'verification_sans_publication_gold',
                run_id=None,deja_traite=False,configuration=configuration,sources=sources,bilan=result)
    if not apply:return report
    rid=uuid4();report['run_id']=str(rid)
    conn.execute('INSERT INTO gold.correspondance_runs '
        '(run_id,empreinte_entrees,algorithme,sources,configuration,bilan) VALUES (%s,%s,%s,%s,%s,%s)',
        (rid,h,ALGORITHME,Jsonb(sources),Jsonb(configuration),Jsonb(result)))
    conn.execute('INSERT INTO gold.points_meteo_correspondance SELECT %s,lieu_id,nom,geom FROM g2_points',(rid,))
    conn.execute('INSERT INTO gold.troncons_correspondance '
        'SELECT %s,identifiant,empreinte,numero_route,rtss,proprietes,geom,longueur_m FROM g2_routes',(rid,))
    conn.execute('INSERT INTO gold.pont_troncon_meteo SELECT %s,* FROM g2_meteo',(rid,))
    conn.execute('INSERT INTO gold.pont_troncon_travaux SELECT %s,* FROM g2_pont_travaux',(rid,))
    conn.execute('INSERT INTO gold.pont_troncon_debit SELECT %s,* FROM g2_pont_debits',(rid,))
    conn.execute('INSERT INTO gold.correspondances_non_appariees SELECT %s,* FROM g2_non_apparies',(rid,))
    # Invariants importants avant commit : une ligne de contexte par troncon.
    counts=conn.execute('SELECT count(*),count(DISTINCT troncon_id) FROM gold.troncons_correspondance '
                        'WHERE run_id=%s',(rid,)).fetchone()
    if counts!=(result['troncons'],result['troncons']):raise ValueError('Troncons dupliques')
    duplicates=conn.execute('SELECT count(*) FROM (SELECT troncon_id FROM gold.pont_troncon_debit '
        'WHERE run_id=%s AND statut=\'probable\' GROUP BY troncon_id HAVING count(*)>1) q',(rid,)).fetchone()[0]
    if duplicates:raise ValueError('Plusieurs debits probables pour un troncon')
    conn.execute('INSERT INTO gold.correspondance_publication(singleton,run_id) VALUES (true,%s) '
        'ON CONFLICT(singleton) DO UPDATE SET run_id=EXCLUDED.run_id,publie_at=now()',(rid,))
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,default=ROOT/'config/correspondances_quebec.json')
    parser.add_argument('--points',type=Path,default=ROOT/'config/points_meteo_quebec.json')
    action=parser.add_mutually_exclusive_group()
    action.add_argument('--installer',action='store_true')
    action.add_argument('--apply',action='store_true')
    parser.add_argument('--rapport',type=Path,default=Path('reports/correspondances_geospatiales.json'))
    args=parser.parse_args()
    with connexion('silver') as conn:
        conn.execute("SET LOCAL statement_timeout='15min'")
        if args.installer:
            conn.execute(SCHEMA.read_text(encoding='utf-8'),prepare=False)
            print('Tables de correspondance installees dans Silver_test.gold')
            return
        result=construire(conn,json.loads(args.config.read_text(encoding='utf-8')),
                         charger_points(args.points),args.apply)
    args.rapport.parent.mkdir(parents=True,exist_ok=True)
    args.rapport.write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(result,indent=2,ensure_ascii=False))


if __name__=='__main__':main()


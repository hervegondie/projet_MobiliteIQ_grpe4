"""Figer des faits troncon-jour retrospectifs, sans inventer l'historique manquant."""
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
from uuid import uuid4
import argparse
import json
import sys

if __package__ in (None,''):
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from transformation.integration_meteo_horaire import connexion
from transformation.regles_journalieres import ALGORITHME,TZ,fenetre_jour,jours_complets,valider_config
from ingestion.contrats_geospatiaux import empreinte

ROOT=Path(__file__).resolve().parents[1]
SCHEMA=ROOT/'sql/gold/007_situations_journalieres_schema.sql'
CALCUL=ROOT/'sql/gold/008_calcul_situation_journaliere.sql'
CHAMPS=('date_jour','troncon_id','troncon_cle','lieu_id','statut_meteo',
    'nb_entraves_actives','nb_chantiers_actifs','nb_associations_a_verifier',
    'longueur_proximite_active_m','part_longueur_entravee','duree_max_entrave_jours',
    'temperature_min','temperature_max','precipitation_totale','pluie','neige_cm',
    'vent_max_kmh','visibilite_moyenne_m','djma_reference','annee_djma','debit_temps_reel',
    'collecte_meteo_id','meteo_collecte_at','sources','raisons_qualite','qualite_ok')


def hash_entrees(jour,correspondance,config,rows):
    # Le cutoff n'entre pas dans la cle : avancer l'horloge sans nouvelles donnees
    # ne cree pas de revision. Les donnees selectionnees, elles, sont empreintees.
    obj=dict(algorithme=ALGORITHME,date=str(jour),correspondance=str(correspondance),
             configuration=config,lignes=sorted(rows,key=lambda r:r['troncon_id']))
    return empreinte(json.loads(json.dumps(obj,default=str,allow_nan=False)))


def charger_jour(conn,jour,connu_au,config=None,apply=False):
    from psycopg.rows import dict_row
    from psycopg import sql
    from psycopg.types.json import Jsonb
    config=valider_config(config or {})
    jours_complets(jour,jour,connu_au)
    debut,fin=fenetre_jour(jour)
    reference=fin-timedelta(microseconds=1)
    conn.execute('SELECT pg_advisory_xact_lock(740221204)')
    if not conn.execute("SELECT to_regclass('gold.situation_jour_runs')").fetchone()[0]:
        raise ValueError('Executer situations_journalieres.py --installer avant le calcul')
    if apply:
        conn.execute('INSERT INTO gold.dim_date VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
            (jour,jour.year,jour.month,jour.isoweekday(),jour.isoweekday()>=6,int((fin-debut).total_seconds()/3600)))
    run=conn.execute('SELECT run_id FROM gold.correspondance_runs WHERE cree_at<=%s '
                     'AND algorithme=%s ORDER BY cree_at DESC,run_id DESC LIMIT 1',
                     (reference,'correspondances-1')).fetchone()
    if run is None:
        return dict(date_jour=str(jour),statut='historique_correspondances_indisponible',lignes=0,
                    explication='Aucun run calcule avant la fin de cette journee. Pas de reconstruction avec le run actuel.')
    # Ces DROP ciblent exclusivement nos tables temporaires de la connexion.
    conn.execute('DROP TABLE IF EXISTS pg_temp.j3_faits,pg_temp.j3_param')
    conn.execute('CREATE TEMP TABLE j3_param(date_jour date,fin_jour timestamptz,'
        'instant_reference timestamptz,connaissance_arretee_at timestamptz,'
        'correspondance_run_id uuid,cfg jsonb) ON COMMIT DROP')
    conn.execute('INSERT INTO j3_param VALUES (%s,%s,%s,%s,%s,%s)',
                 (jour,fin,reference,connu_au,run[0],Jsonb(config)))
    conn.execute(CALCUL.read_text(encoding='utf-8'),prepare=False)
    with conn.cursor(row_factory=dict_row) as cursor:
        rows=cursor.execute('SELECT * FROM j3_faits ORDER BY troncon_id').fetchall()
    if not rows:raise ValueError('Run historique sans troncons : aucune publication')
    ids=[r['troncon_id'] for r in rows]
    if len(set(ids))!=len(ids):raise ValueError('Plusieurs lignes pour un meme troncon-jour')
    for r in rows:
        r['troncon_cle']=empreinte([r['troncon_id'],r['empreinte_source']])
    h=hash_entrees(jour,run[0],config,rows)
    summary=dict(lignes=len(rows),eligibles_ml=sum(r['qualite_ok'] for r in rows),
        non_eligibles_ml=sum(1-r['qualite_ok'] for r in rows),
        avec_meteo=sum(r['collecte_meteo_id'] is not None for r in rows),
        avec_djma=sum(r['djma_reference'] is not None for r in rows),
        travaux_inconnus=sum(r['nb_entraves_actives'] is None for r in rows))
    report=dict(date_jour=str(jour),statut='prepare',mode='chargement' if apply else 'verification_sans_publication',
        correspondance_run_id=str(run[0]),run_id=None,deja_traite=False,instant_reference=reference.isoformat(),
        connaissance_arretee_at=connu_au.isoformat(),bilan=summary)
    if not apply:return report
    old=conn.execute('SELECT run_id FROM gold.situation_jour_runs WHERE empreinte_entrees=%s',(h,)).fetchone()
    if old:
        rid=old[0];report.update(statut='publie',run_id=str(rid),deja_traite=True)
    else:
        rid=uuid4();report.update(statut='publie',run_id=str(rid))
        conn.execute('INSERT INTO gold.situation_jour_runs '
            '(run_id,date_jour,empreinte_entrees,algorithme,correspondance_run_id,instant_reference,'
            'connaissance_arretee_at,configuration,bilan) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            (rid,jour,h,ALGORITHME,run[0],reference,connu_au,Jsonb(config),Jsonb(summary)))
        with conn.cursor() as cur:
            cur.executemany('INSERT INTO gold.dim_troncon_historique VALUES '
                '(%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                [(r['troncon_cle'],r['troncon_id'],r['empreinte_source'],r['numero_route'],r['classe_route'],
                  r['longueur_m'],r['latitude_repere'],r['longitude_repere'],r['geojson']) for r in rows])
            query=sql.SQL('INSERT INTO gold.fact_situation_journaliere ({}) VALUES ({})').format(
                sql.SQL(',').join(map(sql.Identifier,('run_id',*CHAMPS))),
                sql.SQL(',').join(sql.Placeholder() for _ in range(len(CHAMPS)+1)))
            def values(r):
                return (rid,*(Jsonb(r[k]) if k in ('sources','raisons_qualite') else r[k] for k in CHAMPS))
            cur.executemany(query,[values(r) for r in rows])
    conn.execute('INSERT INTO gold.situation_jour_publication(date_jour,run_id) VALUES (%s,%s) '
        'ON CONFLICT(date_jour) DO UPDATE SET run_id=EXCLUDED.run_id,publie_at=now() '
        'WHERE gold.situation_jour_publication.run_id<>EXCLUDED.run_id',(jour,rid))
    actual=conn.execute('SELECT count(*),count(DISTINCT troncon_id) FROM gold.v_situation_journaliere '
                        'WHERE date_jour=%s',(jour,)).fetchone()
    if actual!=(len(rows),len(rows)):raise ValueError('Publication journaliere incomplete ou dupliquee')
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date',type=date.fromisoformat)
    parser.add_argument('--depuis',type=date.fromisoformat)
    parser.add_argument('--jusqua',type=date.fromisoformat)
    parser.add_argument('--jours',type=int,default=1)
    parser.add_argument('--connu-au',type=datetime.fromisoformat)
    parser.add_argument('--max-age-travaux-heures',type=int,default=4)
    action=parser.add_mutually_exclusive_group()
    action.add_argument('--installer',action='store_true');action.add_argument('--apply',action='store_true')
    parser.add_argument('--rapport',type=Path,default=Path('reports/situations_journalieres.json'))
    args=parser.parse_args();now=datetime.now(timezone.utc)
    cutoff=args.connu_au or now
    if cutoff.utcoffset() is None or cutoff>now:parser.error('--connu-au doit etre un instant passe avec fuseau')
    if not 1<=args.jours<=31:parser.error('--jours doit etre entre 1 et 31')
    if args.date and (args.depuis or args.jusqua):parser.error('Utiliser --date ou --depuis/--jusqua')
    if args.jours!=1 and (args.date or args.depuis or args.jusqua):parser.error('--jours ne se combine pas aux dates explicites')
    if bool(args.depuis)!=bool(args.jusqua):parser.error('--depuis et --jusqua sont requis ensemble')
    yesterday=cutoff.astimezone(TZ).date()-timedelta(days=1)
    days=jours_complets(args.date or args.depuis or (yesterday-timedelta(days=args.jours-1)),
                       args.date or args.jusqua or yesterday,cutoff)
    with connexion('silver') as conn:
        conn.execute("SET LOCAL statement_timeout='15min'")
        if args.installer:
            conn.execute(SCHEMA.read_text(encoding='utf-8'),prepare=False)
            print('Tables historiques journalieres installees dans Silver_test.gold');return
        results=[charger_jour(conn,d,cutoff,dict(max_age_travaux_heures=args.max_age_travaux_heures),args.apply)
                 for d in days]
    report=dict(mode='chargement' if args.apply else 'verification_sans_publication',base_cible='Silver_test',resultats=results)
    args.rapport.parent.mkdir(parents=True,exist_ok=True)
    args.rapport.write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(report,indent=2,ensure_ascii=False))


if __name__=='__main__':main()


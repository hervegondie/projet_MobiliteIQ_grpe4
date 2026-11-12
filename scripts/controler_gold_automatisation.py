"""Controles avant diffusion ; aucun changement de donnees Neon."""
import argparse
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

if __package__ in (None,''):
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]))


def verifier_sources(sources,maintenant):
    ages={}
    for source,maximum in [('travaux',4),('routes',168),('debits',168)]:
        at=datetime.fromisoformat(sources[source]['collecte_at'])
        if at.utcoffset() is None:raise ValueError('Collecte sans fuseau : '+source)
        age=(maintenant-at).total_seconds()/3600
        if not 0<=age<=maximum:raise ValueError('Source trop ancienne ou future : '+source)
        ages[source]=round(age,2)
    return ages


def verifier_jour(rapport,jour):
    if rapport.get('mode')!='chargement':raise ValueError('Le calcul journalier ne publie pas')
    rows=[r for r in rapport['resultats'] if r['date_jour']==str(jour)]
    if len(rows)!=1 or rows[0].get('statut')!='publie':
        raise ValueError('Journee cible non publiee : '+str(jour))
    r=rows[0];b=r['bilan'];n=b['lignes']
    if n<=0 or b['avec_meteo']<=0 or b['travaux_inconnus']>=n:
        raise ValueError('Publication incomplete : aucune meteo ou tous les travaux inconnus')
    return r


def main():
    from transformation.integration_meteo_horaire import connexion
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('mode',choices=['correspondances','journalier'])
    p.add_argument('--rapport',type=Path,required=True)
    p.add_argument('--manifest',type=Path)
    a=p.parse_args();report=json.loads(a.rapport.read_text(encoding='utf-8'))
    now=datetime.now(timezone.utc)
    with connexion('silver',readonly=True) as conn:
        if a.mode=='correspondances':
            if report.get('mode')!='chargement' or not report.get('run_id'):
                raise ValueError('Correspondances non publiees')
            row=conn.execute('SELECT r.sources FROM gold.correspondance_runs r '
                'JOIN gold.correspondance_publication p USING(run_id) WHERE r.run_id=%s',
                (report['run_id'],)).fetchone()
            if not row:raise ValueError('Le run calcule ne correspond pas a la publication courante')
            ages=verifier_sources(row[0],now)
            if a.manifest:
                manifest=json.loads(a.manifest.read_text(encoding='utf-8'))
                for source in manifest['sources']:
                    if row[0][source['source']]['collecte_id']!=manifest['collecte_id']:
                        raise ValueError('Correspondances basees sur une autre collecte')
            print(json.dumps(dict(controle='correspondances_validees',age_sources_heures=ages)))
        else:
            jour=now.astimezone(ZoneInfo('America/Toronto')).date()-timedelta(days=1)
            r=verifier_jour(report,jour)
            counts=conn.execute('SELECT count(*),count(DISTINCT troncon_id) '
                'FROM gold.v_situation_journaliere WHERE date_jour=%s AND run_id=%s',
                (jour,r['run_id'])).fetchone()
            if counts!=(r['bilan']['lignes'],)*2:
                raise ValueError('Publication courante incomplete ou dupliquee')
            print(json.dumps(dict(controle='journee_validee',date=str(jour),bilan=r['bilan'])))


if __name__=='__main__':main()

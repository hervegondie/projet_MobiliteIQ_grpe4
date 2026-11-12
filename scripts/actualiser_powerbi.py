"""Declencher UNE actualisation Power BI puis attendre son resultat exact."""
import argparse
import json
import os
from pathlib import Path
import time
from uuid import UUID


def configuration(env):
    result={}
    for name in ['PBI_TENANT_ID','PBI_CLIENT_ID','PBI_CLIENT_SECRET','PBI_WORKSPACE_ID','PBI_DATASET_ID']:
        value=env.get(name,'').strip()
        if not value:raise ValueError('Configurer '+name)
        if name!='PBI_CLIENT_SECRET':value=str(UUID(value))
        result[name]=value
    return result


def verifier_http(response,attendu,operation):
    if response.status_code!=attendu:
        # Ne jamais afficher les corps de reponse, secrets ou jetons.
        raise RuntimeError(f'{operation} : HTTP {response.status_code}. Consulter Power BI/Entra.')


def actualiser(config,session,attente=1800,pause=time.sleep,horloge=time.monotonic):
    token=session.post('https://login.microsoftonline.com/'+config['PBI_TENANT_ID']+'/oauth2/v2.0/token',
        data=dict(grant_type='client_credentials',client_id=config['PBI_CLIENT_ID'],
                  client_secret=config['PBI_CLIENT_SECRET'],scope='https://analysis.windows.net/powerbi/api/.default'),
        timeout=30,allow_redirects=False)
    verifier_http(token,200,'Authentification Microsoft')
    headers={'Authorization':'Bearer '+token.json()['access_token']}
    url=('https://api.powerbi.com/v1.0/myorg/groups/'+config['PBI_WORKSPACE_ID']+
         '/datasets/'+config['PBI_DATASET_ID']+'/refreshes')
    history=session.get(url,headers=headers,params={'$top':60},timeout=30,allow_redirects=False)
    verifier_http(history,200,'Historique avant actualisation')
    if any(x.get('status') in ('Unknown','InProgress','NotStarted') for x in history.json().get('value',[])):
        raise RuntimeError('Une actualisation est deja en cours : aucun nouvel appel envoye')
    # Pas de retry du POST : une erreur reseau peut cacher une demande deja acceptee.
    response=session.post(url,headers=headers,json={},timeout=30,allow_redirects=False)
    verifier_http(response,202,'Demande actualisation')
    request_id=response.headers.get('x-ms-request-id') or response.headers.get('Location','').rstrip('/').split('/')[-1]
    if not request_id:raise RuntimeError('Demande acceptee sans identifiant : verifier dans Power BI, ne pas relancer aveuglement')
    request_id=str(UUID(request_id))
    deadline=horloge()+attente
    while horloge()<deadline:
        response=session.get(url,headers=headers,params={'$top':60},timeout=30,allow_redirects=False)
        verifier_http(response,200,'Suivi actualisation')
        rows=[x for x in response.json().get('value',[]) if x.get('requestId')==request_id]
        if rows:
            status=rows[0]['status']
            if status=='Completed':return dict(statut='Completed',request_id=request_id)
            if status not in ('Unknown','InProgress','NotStarted'):
                raise RuntimeError('Actualisation Power BI terminee avec statut '+status+' ; request_id='+request_id)
        pause(20)
    raise TimeoutError('Actualisation non confirmee apres le delai ; verifier Power BI avant de relancer. request_id='+request_id)


def main():
    import requests
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--verifier-config',action='store_true')
    p.add_argument('--rapport',type=Path,default=Path('reports/powerbi_actualisation.json'))
    args=p.parse_args();cfg=configuration(os.environ)
    if args.verifier_config:
        print('Configuration presente (authentification non testee)');return
    try:
        with requests.Session() as session:result=actualiser(cfg,session)
    except requests.RequestException:
        raise SystemExit('Erreur reseau Power BI. Verifier son historique avant une nouvelle demande.') from None
    args.rapport.parent.mkdir(parents=True,exist_ok=True)
    args.rapport.write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result))


if __name__=='__main__':main()

"""Regles pures et partagees des correspondances geographiques."""
from datetime import datetime
import math
import re
from zoneinfo import ZoneInfo
from transformation.nettoyage import parse_timestamp

ALGORITHME = 'correspondances-1'
DEFAULTS = dict(meteo_rayon_m=15000, meteo_ecart_min_m=500, meteo_fraicheur_minutes=120,
    travaux_distance_m=15, travaux_longueur_min_m=10, travaux_fraction_min=0.10,
    debit_distance_m=10, debit_fraction_min=0.50, debit_ecart_fraction_min=0.10)


def normaliser_rtss(value):
    if not isinstance(value,str):return None
    value=value.strip().upper()
    if re.fullmatch(r'\d{5}-\d{2}-\d{3}-[A-Z0-9]{4}',value):
        return value.replace('-','')
    return value if re.fullmatch(r'\d{10}[A-Z0-9]{4}',value) else None


def routes_travaux(value):
    """Champ routeAutoroute uniquement ; jamais les nombres d'une description."""
    if value is None:return []
    value=str(value).strip()
    if not value:return []
    # Accepter seulement des listes explicites de numeros et leur prefixe.
    tokens=re.split(r'\s*(?:[,;/]|\bet\b)\s*',value,flags=re.I)
    out=[]
    for token in tokens:
        m=re.fullmatch(r'(?:(?:A|R|Route|Autoroute)[ -]*)?(\d{1,5})',token.strip(),re.I)
        if not m:return []
        out.append(m[1].zfill(5))
    return sorted(set(out))


def dates_travaux(props):
    values=[]
    try:
        for name in ['debut','fin']:
            raw=props.get(name)
            values.append(parse_timestamp(raw,ZoneInfo('America/Toronto')) if raw else None)
        if None in values or values[1] <= values[0]:
            raise ValueError('Dates absentes ou ordre invalide')
    except (ValueError,TypeError,OverflowError):
        return None,None,'inconnues'
    return *values,'ok'


def valider_config(config):
    if config.get('version') != 1:raise ValueError('Version de configuration inconnue')
    result={'version':1,'bbox':config.get('bbox'),**{k:config.get(k,v) for k,v in DEFAULTS.items()}}
    b=result['bbox']
    if not isinstance(b,list) or len(b)!=4 or any(isinstance(v,bool) or not isinstance(v,(int,float)) or not math.isfinite(v) for v in b):
        raise ValueError('bbox doit contenir quatre nombres finis')
    if not (-85<=b[0]<b[2]<=-50 and 40<=b[1]<b[3]<=70):
        raise ValueError('bbox invalide pour le territoire cible')
    for name in DEFAULTS:
        v=result[name]
        if isinstance(v,bool) or not isinstance(v,(int,float)) or not math.isfinite(v) or v<=0:
            raise ValueError(f'{name}: nombre positif fini obligatoire')
        if 'fraction' in name and v>1:raise ValueError(f'{name}: fraction <= 1 obligatoire')
        if name=='meteo_fraicheur_minutes' and int(v)!=v:
            raise ValueError('meteo_fraicheur_minutes doit etre entier')
    if result['meteo_rayon_m']>50000 or max(result['travaux_distance_m'],result['debit_distance_m'])>100:
        raise ValueError('Seuil de proximite trop large pour cette premiere version')
    return result


def statut_meteo(distance,max_distance,second_distance,config):
    if max_distance>config['meteo_rayon_m']:return 'hors_couverture'
    if second_distance is not None and second_distance-distance<config['meteo_ecart_min_m']:
        return 'a_verifier'
    return 'representatif'

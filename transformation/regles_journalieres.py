"""Regles temporelles pures : jours locaux complets et donnees disponibles."""
from datetime import date,datetime,time,timedelta,timezone
from zoneinfo import ZoneInfo

ALGORITHME='situation-jour-1'
TZ=ZoneInfo('America/Toronto')
DEFAULTS=dict(max_age_travaux_heures=4,max_age_routes_heures=168,
              max_age_debits_heures=168,retard_meteo_max_heures=72)


def fenetre_jour(jour):
    if type(jour) is not date:raise ValueError('Une date civile est requise')
    debut=datetime.combine(jour,time.min,TZ)
    fin=datetime.combine(jour+timedelta(days=1),time.min,TZ)
    return debut.astimezone(timezone.utc),fin.astimezone(timezone.utc)


def jours_complets(debut,fin,maintenant):
    if maintenant.utcoffset() is None:raise ValueError('Date de connaissance sans fuseau')
    if debut>fin or (fin-debut).days>30:raise ValueError('Periode invalide ou superieure a 31 jours')
    if fin>=maintenant.astimezone(TZ).date():raise ValueError('Choisir uniquement des journees locales terminees')
    return [debut+timedelta(days=i) for i in range((fin-debut).days+1)]


def valider_config(config):
    result={k:config.get(k,v) for k,v in DEFAULTS.items()}
    if set(config)-set(DEFAULTS):raise ValueError('Parametre journalier inconnu')
    for k,v in result.items():
        if type(v) is not int or not 1<=v<=744:raise ValueError(f'{k}: entier entre 1 et 744 obligatoire')
    return result


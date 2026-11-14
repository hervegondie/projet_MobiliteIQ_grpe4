"""Seuils de fraicheur communs au calcul Gold et au controle de diffusion."""
from datetime import datetime

LIMITES_HEURES = {'travaux': 4, 'routes': 168, 'debits': 168}


def verifier_sources(sources, maintenant):
    if maintenant.utcoffset() is None:
        raise ValueError('Instant de controle sans fuseau')
    ages = {}
    for source, maximum in LIMITES_HEURES.items():
        try:
            valeur = sources[source]['collecte_at']
            at = datetime.fromisoformat(valeur)
        except (KeyError, TypeError, ValueError):
            raise ValueError('Date de collecte absente ou invalide : ' + source) from None
        if at.utcoffset() is None:
            raise ValueError('Collecte sans fuseau : ' + source)
        age = (maintenant - at).total_seconds() / 3600
        if not 0 <= age <= maximum:
            raise ValueError(
                f'Source trop ancienne ou future : {source} '
                f'(age={age:.2f} h, maximum={maximum} h, collecte_at={at.isoformat()})'
            )
        ages[source] = round(age, 2)
    return ages

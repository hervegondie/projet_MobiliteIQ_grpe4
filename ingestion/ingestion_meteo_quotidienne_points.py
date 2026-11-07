"""Variante multi-points de la collecte quotidienne, sans modifier public.meteo."""
from datetime import datetime, timezone
import csv
import json
from pathlib import Path
from uuid import uuid4

from ingestion import ingestion_meteo as legacy
from ingestion.points_meteo import charger_points
from ingestion.ingestion_meteo_horaire import enregistrer_bronze_lot
from transformation.nettoyage_meteo_quotidienne_points import parametres, transformer


def collecter(point, debut, fin, api_url):
    stamp = datetime.now(timezone.utc).isoformat()
    data = legacy.telecharger_brut(debut, fin, api_url, point['latitude'], point['longitude'])
    return dict(version=1, flux='quotidienne', collecte_id=str(uuid4()), lieu_id=point['lieu_id'],
                latitude=point['latitude'], longitude=point['longitude'], ingestion_timestamp=stamp,
                date_debut=debut.isoformat(), date_fin=fin.isoformat(), api_url=api_url,
                parametres=parametres(point['latitude'], point['longitude'], debut, fin), donnees=data)


def executer(points_path, debut=None, fin=None, jours=7, sans_neon=False,
             output_dir=Path('ingestion/output_meteo_points'), manifest_path=Path('reports/collecte_meteo_quotidienne_points.json')):
    points = charger_points(points_path)
    debut, fin, api_url = legacy.determiner_periode(debut, fin, jours)
    lot_id = str(uuid4())
    folder = Path(output_dir) / lot_id
    folder.mkdir(parents=True, exist_ok=False)
    envelopes, csv_rows = [], []
    for point in points:
        print(f"Collecte quotidienne : {point['lieu_id']} ({debut} au {fin})")
        envelope = collecter(point, debut, fin, api_url)
        envelope['lot_id'] = lot_id
        envelope['lieu_nom'] = point.get('nom', point['lieu_id'])
        (folder / (point['lieu_id'] + '.json')).write_text(json.dumps(envelope, ensure_ascii=False, allow_nan=False), encoding='utf-8')
        envelopes.append(envelope)
    if sans_neon:
        for envelope in envelopes:
            csv_rows.extend(transformer(envelope))
    else:
        # Le brut est conserve meme si le nettoyage Silver signale ensuite un rejet.
        inserted = enregistrer_bronze_lot(envelopes, 'meteo_quotidienne_points_bruts')
    if csv_rows:
        with (folder / 'meteo_quotidienne_points.csv').open('w', encoding='utf-8', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(csv_rows[0]))
            writer.writeheader()
            writer.writerows(csv_rows)
    report = dict(version=2, flux='quotidienne', mode='verification_sans_neon' if sans_neon else 'chargement_bronze',
                  lot_id=lot_id, points=len(points), lieux_attendus=[p['lieu_id'] for p in points],
                  collectes=[dict(collecte_id=e['collecte_id'], lieu_id=e['lieu_id']) for e in envelopes],
                  date_debut=debut.isoformat(), date_fin=fin.isoformat(),
                  jours=len(points) * ((fin - debut).days + 1), inserees=0 if sans_neon else inserted)
    Path(manifest_path).parent.mkdir(parents=True, exist_ok=True)
    Path(manifest_path).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report

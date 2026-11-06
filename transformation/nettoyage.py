"""Nettoyer, typer et valider les quatre sources, sans accès à Neon."""
from __future__ import annotations

from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
import re
import unicodedata

METEO_NUMBERS = (
    "temperature_max temperature_min precipitation_totale pluie neige "
    "vitesse_vent_max rafales_vent_max visibilite_moyenne"
).split()


TYPES = {
    "meteo": {"date": "date", **{c: "numeric" for c in METEO_NUMBERS}},
    "route": {"chang_debu": "numeric", "val_longr_": "numeric",
              "dat_debut_": "date", "dah_cretn": "date"},
    "travaux_bruts": {c: "timestamp with time zone" for c in ("debut", "fin", "miseAJour")},
    "circulation": {
        "dat_debut_sectn_trafc": "date", "val_chang_debut": "numeric",
        "val_chang_fin": "numeric", "val_30e_heure": "numeric",
        **{f"{metric}_annee_{i}": "integer"
           for metric in ("djma", "djme", "djmh", "cam") for i in range(1, 11)},
        **{f"val_{metric}_annee_{i}": "numeric"
           for metric in ("djma", "djme", "djmh", "cam") for i in range(1, 11)},
    },
}


COMMON_TYPES = {"ingestion_timestamp": "timestamp with time zone",
                "ingestion_date": "date", "ingestion_time": "time without time zone"}


def clean(value):
    if value is None:
        return None
    if not isinstance(value, str):
        return value
    value = unicodedata.normalize("NFC", value).strip()
    return None if value.casefold() in ("", "null", "\\n") else value


def parse_timestamp(value, tz):
    if isinstance(value, datetime):
        result = value
    else:
        text = str(value)
        # Format des travaux visible dans l'export Neon : AAAA/MM/JJ HH:MM:SS.
        if re.match(r"^\d{4}/\d{2}/\d{2}(?: |T)", text):
            text = text[:10].replace("/", "-") + text[10:]
        result = datetime.fromisoformat(text)
    if result.tzinfo is None:
        # Refuser les heures locales ambiguës ou inexistantes lors du changement d'heure.
        first = result.replace(tzinfo=tz, fold=0)
        second = result.replace(tzinfo=tz, fold=1)
        if first.utcoffset() != second.utcoffset():
            raise ValueError("heure locale ambiguë ou inexistante; fournir un décalage ISO")
        result = first
    return result.astimezone(timezone.utc)


def cast(value, kind, tz):
    value = clean(value)
    if value is None:
        return None
    if kind in ("numeric", "integer"):
        # La virgule est un séparateur décimal, jamais un séparateur de milliers.
        number = Decimal(str(value).replace(",", "."))
        if not number.is_finite():
            raise ValueError("nombre non fini")
        if kind == "integer":
            if number != number.to_integral_value() or not -2147483648 <= number <= 2147483647:
                raise ValueError("entier PostgreSQL attendu")
            return int(number)
        return number
    if kind == "date":
        if isinstance(value, datetime):
            raise ValueError("date attendue, horodatage reçu")
        if isinstance(value, date):
            return value
        text = str(value)
        if re.fullmatch(r"\d{8}", text):
            # Formats réels dat_debut_ et dah_cretn dans route.xlsx.
            return datetime.strptime(text, "%Y%m%d").date()
        if re.fullmatch(r"\d{4}/\d{2}/\d{2}", text):
            return datetime.strptime(text, "%Y/%m/%d").date()
        return date.fromisoformat(text)
    if kind == "time without time zone":
        result = value if isinstance(value, time) else time.fromisoformat(str(value))
        if result.tzinfo is not None:
            raise ValueError("heure sans fuseau attendue")
        return result
    if kind == "timestamp with time zone":
        return parse_timestamp(value, tz)
    return str(value)


def target_types(source, columns):
    return {c: TYPES[source].get(c, COMMON_TYPES.get(c, "text")) for c in columns}


def transform(source, raw, source_tz, ingestion_tz):
    converted, errors = {}, []
    for column, kind in target_types(source, raw).items():
        try:
            tz = ingestion_tz if column == "ingestion_timestamp" else source_tz
            converted[column] = cast(raw[column], kind, tz)
        except (ValueError, InvalidOperation, OverflowError, TypeError):
            converted[column] = None
            errors.append(f"{column}: valeur invalide pour {kind}")
    if source == "meteo":
        if converted.get("date") is None:
            errors.append("date obligatoire")
        for c in METEO_NUMBERS[2:]:
            if converted.get(c) is not None and converted[c] < 0:
                errors.append(f"{c}: valeur négative")
        lo, hi = converted.get("temperature_min"), converted.get("temperature_max")
        if lo is not None and hi is not None and lo > hi:
            errors.append("temperature_min supérieure à temperature_max")
    elif source == "travaux_bruts":
        if not converted.get("identifiant"):
            errors.append("identifiant obligatoire")
        start, end = converted.get("debut"), converted.get("fin")
        if start is not None and end is not None and end < start:
            errors.append("fin antérieure au début")
    elif source == "route":
        if not converted.get("ide_sous_r"):
            errors.append("ide_sous_r obligatoire")
        if converted.get("val_longr_") is not None and converted["val_longr_"] < 0:
            errors.append("val_longr_: longueur négative")
        if converted.get("chang_debu") is not None and converted["chang_debu"] < 0:
            errors.append("chang_debu: chaînage négatif")
    elif source == "circulation":
        for c in ("ide_sectn_trafc", "num_sectn_trafc"):
            if not converted.get(c):
                errors.append(f"{c}: identifiant obligatoire")
        for c, value in converted.items():
            if value is None:
                continue
            if c in TYPES[source] and TYPES[source][c] == "numeric":
                if value < 0:
                    errors.append(f"{c}: valeur négative")
                if c.startswith("val_cam_annee_") and value > 100:
                    errors.append(f"{c}: pourcentage supérieur à 100")
            if c in TYPES[source] and TYPES[source][c] == "integer" and not 1900 <= value <= 2100:
                errors.append(f"{c}: année hors de la plage 1900–2100")
        for metric in ("djma", "djme", "djmh", "cam"):
            for i in range(1, 11):
                year, measure = f"{metric}_annee_{i}", f"val_{metric}_annee_{i}"
                if converted.get(measure) is not None and converted.get(year) is None:
                    errors.append(f"{measure}: année associée absente ou invalide")
        start, end = converted.get("val_chang_debut"), converted.get("val_chang_fin")
        if (converted.get("rtss_debut") and converted.get("rtss_debut") == converted.get("rtss_fin")
                and start is not None and end is not None and end < start):
            errors.append("chaînage de fin inférieur au début sur le même RTSS")
    # Uniformiser les métadonnées en UTC si l'horodatage est disponible.
    # Les valeurs d'origine, dont les dates vides du RTSS, restent dans _silver_raw.
    observed_at = converted.get("ingestion_timestamp")
    if observed_at is not None:
        if "ingestion_date" in raw:
            converted["ingestion_date"] = observed_at.date()
        if "ingestion_time" in raw:
            converted["ingestion_time"] = observed_at.time().replace(tzinfo=None)
    return converted, errors


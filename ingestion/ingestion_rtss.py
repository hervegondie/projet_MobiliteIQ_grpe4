"""Collecter rtss dans mobiliteIQIngestion.public.route.

Sans option : ajout dans Neon. --csv-seulement : validation locale sans écriture.
Le fichier source est archivé et toutes les observations existantes sont conservées.
"""
if __package__:
    from ._csv_bronze import lancer
else:
    from _csv_bronze import lancer


if __name__ == "__main__":
    lancer("rtss")

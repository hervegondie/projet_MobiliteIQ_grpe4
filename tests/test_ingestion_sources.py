"""Tests hors ligne : aucun appel réseau et aucune écriture dans Neon."""
import csv
from datetime import date, datetime, timezone
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from ingestion import _csv_bronze as flux
from ingestion._contrats_csv import COLONNES, SOURCES
from ingestion import ingestion_meteo as meteo
from transformation.integration import CONTRACT
from transformation.nettoyage import transform


class CSVTests(unittest.TestCase):
    def fixture(self, folder, columns=None, row=None):
        columns = columns or ['id', 'texte']
        p = Path(folder)/'source.csv'
        with p.open('w',encoding='utf-8',newline='') as f:
            w=csv.writer(f); w.writerow(columns)
            if row is not None: w.writerow(row)
        return p

    def test_quotes_multiline_and_leading_zeroes(self):
        with tempfile.TemporaryDirectory() as folder:
            src=self.fixture(folder,row=['0012','Pont "A", voie\nfermée'])
            dst=Path(folder)/'normal.csv'
            n=flux.normaliser_csv(src,dst,['id','texte'],datetime(2026,11,6,tzinfo=timezone.utc))
            with dst.open(encoding='utf-8',newline='') as f: row=next(csv.DictReader(f))
            self.assertEqual(n,1)
            self.assertEqual(row['id'],'0012')
            self.assertEqual(row['texte'],'Pont "A", voie\nfermée')
            self.assertEqual(row['ingestion_date'],'2026-11-06')
            self.assertTrue(row['ingestion_timestamp'].endswith('+00:00'))

    def test_source_column_order_is_normalized(self):
        with tempfile.TemporaryDirectory() as folder:
            src=self.fixture(folder,['texte','id'],['ok','005'])
            dst=Path(folder)/'normal.csv'
            flux.normaliser_csv(src,dst,['id','texte'],datetime.now(timezone.utc))
            with dst.open(encoding='utf-8',newline='') as f:
                r=csv.reader(f); next(r); self.assertEqual(next(r)[:2],['005','ok'])

    def test_wrong_source_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            src=self.fixture(folder,row=['1','ok'])
            with self.assertRaisesRegex(ValueError,'Schéma'):
                flux.normaliser_csv(src,Path(folder)/'out.csv',['date','temperature_max'],datetime.now(timezone.utc))

    def test_duplicate_headers_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            src=self.fixture(folder,['id','id'],['1','2'])
            with self.assertRaises(ValueError):
                flux.normaliser_csv(src,Path(folder)/'out.csv',['id','texte'],datetime.now(timezone.utc))

    def test_empty_reference_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            src=self.fixture(folder)
            with self.assertRaisesRegex(ValueError,'sans données'):
                flux.normaliser_csv(src,Path(folder)/'out.csv',['id','texte'],datetime.now(timezone.utc))

    def test_missing_fields_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            src=self.fixture(folder,row=['1'])
            with self.assertRaisesRegex(ValueError,'Nombre de champs'):
                flux.normaliser_csv(src,Path(folder)/'out.csv',['id','texte'],datetime.now(timezone.utc))

    def test_naive_collection_timestamp_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            src=self.fixture(folder,row=['1','ok'])
            with self.assertRaises(ValueError):
                flux.normaliser_csv(src,Path(folder)/'out.csv',['id','texte'],datetime(2026,11,6))

    def test_contracts_match_silver(self):
        self.assertEqual(set(COLONNES['rtss'])|set(flux.METADONNEES),set(CONTRACT['route']))
        self.assertEqual(set(COLONNES['debits']),set(CONTRACT['circulation']))
        self.assertEqual(SOURCES['debits']['table'],'circulation_debits_bruts')
        self.assertEqual(SOURCES['rtss']['table'],'route')
        self.assertEqual(meteo.COLONNES,CONTRACT['meteo'])

    def test_non_bronze_url_blocked(self):
        url='postgresql://example:demo@example.invalid/Silver_test?sslmode=require'
        with patch.dict(os.environ,{'BRONZE_DATABASE_URL':url},clear=True):
            with self.assertRaisesRegex(ValueError,'mobiliteIQIngestion'): flux.connexion_bronze()

    def test_tls_required(self):
        url='postgresql://example:demo@example.invalid/mobiliteIQIngestion?sslmode=disable'
        with patch.dict(os.environ,{'BRONZE_DATABASE_URL':url},clear=True):
            with self.assertRaisesRegex(ValueError,'TLS'): flux.connexion_bronze()

    def test_copy_preserves_values_and_nulls(self):
        driver=MagicMock(); conn=driver.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value=[(c,'text') for c in ['id','texte']+flux.METADONNEES]
        with tempfile.TemporaryDirectory() as folder:
            src=self.fixture(folder,row=['001','']); dst=Path(folder)/'normal.csv'
            flux.normaliser_csv(src,dst,['id','texte'],datetime.now(timezone.utc))
            with patch.dict(sys.modules,{'psycopg':driver}):
                self.assertEqual(flux.importer_csv(dst,'route',['id','texte'],'not-used',1),1)
        copy=conn.cursor.return_value.__enter__.return_value.copy.return_value.__enter__.return_value
        self.assertEqual(copy.write_row.call_args.args[0][:2],('001',None))

    def test_incompatible_existing_table_blocks_copy(self):
        driver=MagicMock(); conn=driver.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value=[('wrong','text')]
        with tempfile.TemporaryDirectory() as folder:
            src=self.fixture(folder,row=['001','']); dst=Path(folder)/'normal.csv'
            flux.normaliser_csv(src,dst,['id','texte'],datetime.now(timezone.utc))
            with patch.dict(sys.modules,{'psycopg':driver}):
                with self.assertRaisesRegex(ValueError,'incompatible'):
                    flux.importer_csv(dst,'route',['id','texte'],'not-used',1)
        conn.cursor.assert_not_called()

    def test_wrong_expected_count_fails_transaction(self):
        driver=MagicMock(); conn=driver.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value=[(c,'text') for c in ['id','texte']+flux.METADONNEES]
        with tempfile.TemporaryDirectory() as folder:
            src=self.fixture(folder,row=['001','']); dst=Path(folder)/'normal.csv'
            flux.normaliser_csv(src,dst,['id','texte'],datetime.now(timezone.utc))
            with patch.dict(sys.modules,{'psycopg':driver}):
                with self.assertRaisesRegex(ValueError,'transaction annulée'):
                    flux.importer_csv(dst,'route',['id','texte'],'not-used',2)

    def test_normalized_rows_are_accepted_by_silver(self):
        for key, silver in [('rtss','route'),('debits','circulation')]:
            values={c:'' for c in COLONNES[key]}
            values.update({'ide_sous_r':'001'} if key=='rtss' else {'ide_sectn_trafc':'123','num_sectn_trafc':'000123'})
            with tempfile.TemporaryDirectory() as folder:
                src=self.fixture(folder,COLONNES[key],[values[c] for c in COLONNES[key]])
                dst=Path(folder)/'normal.csv'
                flux.normaliser_csv(src,dst,COLONNES[key],datetime.now(timezone.utc))
                with dst.open(encoding='utf-8',newline='') as f:
                    raw={k:v or None for k,v in next(csv.DictReader(f)).items()}
                _, errors=transform(silver,raw,timezone.utc,timezone.utc)
                self.assertEqual(errors,[])


class WeatherTests(unittest.TestCase):
    def test_default_window_excludes_local_today(self):
        start,end,url=meteo.determiner_periode(maintenant=datetime(2026,11,6,12,tzinfo=timezone.utc))
        self.assertEqual((start,end),(date(2026,10,30),date(2026,11,5)))
        self.assertEqual(url,meteo.API_RECENTE)

    def test_window_uses_quebec_day_at_utc_midnight(self):
        start,end,_=meteo.determiner_periode(jours=1,maintenant=datetime(2026,11,6,2,tzinfo=timezone.utc))
        self.assertEqual((start,end),(date(2026,11,4),date(2026,11,4)))

    def test_explicit_archive_window_preserved(self):
        self.assertEqual(meteo.determiner_periode(date(2023,1,1),date(2025,12,31)),
                         (date(2023,1,1),date(2025,12,31),meteo.API_URL))

    def test_invalid_period_and_days(self):
        for args in ({'debut':date(2024,1,1)}, {'jours':0}, {'jours':93},
                     {'debut':date(2024,2,1),'fin':date(2024,1,1)}):
            with self.assertRaises(ValueError): meteo.determiner_periode(**args)

    def payload(self):
        return {'daily': {'time':['2026-11-04','2026-11-05'],**{c:[0,None] for c in meteo.MESURES}},
                'hourly': {'time':['2026-11-04T00:00','2026-11-05T00:00'],'visibility':[1000,None]}}

    def test_null_and_zero_preserved(self):
        daily,hourly=meteo.extraire_lignes(self.payload(),date(2026,11,4),date(2026,11,5))
        self.assertEqual(daily[0][1],0)
        self.assertIsNone(daily[1][1]); self.assertIsNone(hourly[1][1])

    def test_missing_visibility_blocks_load(self):
        payload=self.payload(); del payload['hourly']['visibility']
        with self.assertRaises(ValueError): meteo.extraire_lignes(payload,date(2026,11,4),date(2026,11,5))

    def test_non_finite_weather_rejected(self):
        for value in (float('nan'),float('inf'),True):
            with self.assertRaises(ValueError): meteo.nombre_ou_null(value)


if __name__=='__main__': unittest.main()

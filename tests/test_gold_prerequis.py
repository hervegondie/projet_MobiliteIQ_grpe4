"""Tests hors ligne : pagination complete, coordonnees, valeurs et annees."""
import copy
from decimal import Decimal
import unittest
from unittest.mock import Mock

from ingestion.contrats_geospatiaux import (
    empreinte, identifiant, references_debits, valider_geometrie)
from ingestion.ingestion_geometries import collecter
from transformation.transformation_geospatiale import preparer


def feature(key='001', source='routes'):
    field = {'routes':'ide_sous_r','travaux':'identifiant','debits':'ide_sectn_trafc'}[source]
    return dict(type='Feature', properties={field:key, 'num_route':'00035'},
                geometry=dict(type='LineString',coordinates=[[-71.2,46.8],[-71.21,46.81]]))


def response(data=None, count=None):
    r = Mock()
    r.json.return_value = dict(type='FeatureCollection',features=data or [])
    r.content = f'<FeatureCollection numberMatched="{count}"/>'.encode()
    return r


class GoldPrerequisTests(unittest.TestCase):
    def test_identifier_preserves_leading_zeroes(self):
        self.assertEqual(identifiant('routes',feature('00035')), '00035')

    def test_identifier_rejects_missing_or_float(self):
        for key in [None, '', 123.0, True]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                identifiant('routes',feature(key))

    def test_hash_is_independent_of_dictionary_order(self):
        self.assertEqual(empreinte({'b':1,'a':2}),empreinte({'a':2,'b':1}))

    def test_geometry_accepts_line_and_multiline(self):
        geom = feature()['geometry']
        valider_geometrie(geom)
        valider_geometrie(dict(type='MultiLineString',coordinates=[geom['coordinates']]))

    def test_geometry_rejects_axis_inversion_and_projected_coordinates(self):
        for xy in [[[46.8,-71.2],[46.81,-71.21]],[[300000,5100000],[300001,5100010]]]:
            with self.subTest(xy=xy), self.assertRaises(ValueError):
                valider_geometrie(dict(type='LineString',coordinates=xy))

    def test_geometry_rejects_null_empty_degenerate_nonfinite(self):
        for geom in [None,dict(type='Point',coordinates=[-71,46]),
                     dict(type='LineString',coordinates=[]),
                     dict(type='LineString',coordinates=[[-71,46],[-71,46]]),
                     dict(type='LineString',coordinates=[[-71,46],[-71,float('nan')]])]:
            with self.subTest(geom=geom), self.assertRaises(ValueError):
                valider_geometrie(geom)

    def test_reference_year_is_not_collection_year(self):
        rows = references_debits(dict(djma_annee_1='2025.0',val_djma_annee_1='1780.0'))
        self.assertEqual(rows,[dict(indicateur='djma',annee_reference=2025,
                                   valeur=Decimal('1780.0'),periode='annuelle',unite='vehicules/jour')])

    def test_seasonal_metrics_keep_distinct_years_and_units(self):
        rows=references_debits(dict(djme_annee_1=2025,val_djme_annee_1=100,
            djmh_annee_1=2024,val_djmh_annee_1=90,cam_annee_1=2025,val_cam_annee_1='7.5'))
        self.assertEqual([(r['periode'],r['annee_reference'],r['unite']) for r in rows],
                         [('estivale',2025,'vehicules/jour'),('hivernale',2024,'vehicules/jour'),
                          ('annuelle',2025,'pourcent')])

    def test_null_is_not_zero_and_30th_hour_is_not_invented(self):
        self.assertEqual(references_debits({'djma_annee_1':2025,'val_djma_annee_1':'',
                                           'val_30e_heure':500}),[])
        rows=references_debits({'djma_annee_1':2025,'val_djma_annee_1':'0'})
        self.assertEqual(rows[0]['valeur'],0)

    def test_invalid_references_are_rejected(self):
        for props in [dict(val_djma_annee_1=100),dict(djma_annee_1='2025.5',val_djma_annee_1=10),
                      dict(cam_annee_1=2025,val_cam_annee_1=101),
                      dict(djma_annee_1=2025,val_djma_annee_1=-1),
                      dict(djma_annee_1=2025,val_djma_annee_1='NaN')]:
            with self.subTest(props=props), self.assertRaises(ValueError):
                references_debits(props)

    def test_duplicate_year_different_value_is_rejected(self):
        with self.assertRaises(ValueError):
            references_debits(dict(djma_annee_1=2025,val_djma_annee_1=10,
                                   djma_annee_2=2025,val_djma_annee_2=11))

    def test_complete_pagination(self):
        s=Mock();s.get.side_effect=[response(count=3),response([feature('a'),feature('b')]),
                                   response([feature('c')]),response(count=3)]
        snapshot=collecter('routes',s,page_size=2)
        self.assertEqual(len(snapshot['features']),3)
        self.assertEqual(s.get.call_args_list[2].kwargs['params']['startindex'],2)
        self.assertEqual(snapshot['srid'],4326)

    def test_repeated_page_is_not_silently_deduplicated(self):
        s=Mock();s.get.side_effect=[response(count=3),response([feature('a'),feature('b')]),
                                   response([feature('a')])]
        with self.assertRaisesRegex(ValueError,'duplique'):
            collecter('routes',s,2)

    def test_empty_page_before_total_fails(self):
        s=Mock();s.get.side_effect=[response(count=2),response([])]
        with self.assertRaisesRegex(ValueError,'incomplete'):
            collecter('routes',s)

    def test_changed_total_fails(self):
        s=Mock();s.get.side_effect=[response(count=1),response([feature()]),response(count=2)]
        with self.assertRaisesRegex(ValueError,'change'):
            collecter('routes',s)

    def test_empty_works_snapshot_is_valid(self):
        s=Mock();s.get.side_effect=[response(count=0),response(count=0)]
        self.assertEqual(collecter('travaux',s)['features'],[])

    def test_empty_route_reference_fails(self):
        s=Mock();s.get.return_value=response(count=0)
        with self.assertRaisesRegex(ValueError,'vide'):
            collecter('routes',s)

    def test_reject_keeps_raw_feature_and_reason(self):
        bad=feature('bad');bad['geometry']=None
        good=feature('ok')
        a,r=preparer(dict(source='routes',srid=4326,attendu=2,features=[good,bad]))
        self.assertEqual(len(a),1);self.assertEqual(r[0][0],'bad');self.assertEqual(r[0][2],bad)

    def test_truncated_snapshot_or_wrong_srid_fails(self):
        for values in [dict(source='routes',srid=4326,attendu=2,features=[feature()]),
                       dict(source='routes',srid=3857,attendu=1,features=[feature()])]:
            with self.subTest(values=values),self.assertRaises(ValueError):preparer(values)

    def test_all_invalid_does_not_replace_current_reference(self):
        f=feature();f['geometry']=None
        with self.assertRaisesRegex(ValueError,'Aucune'):
            preparer(dict(source='routes',srid=4326,attendu=1,features=[f]))


if __name__=='__main__':
    unittest.main()


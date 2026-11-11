import copy
import unittest
from transformation.regles_correspondances import (
    normaliser_rtss,routes_travaux,dates_travaux,valider_config,statut_meteo)


def config():
    return valider_config(dict(version=1,bbox=[-71.65,46.65,-70.95,47.05]))


class ReglesCorrespondanceTests(unittest.TestCase):
    def test_rtss_equivalence_and_leading_zeroes(self):
        self.assertEqual(normaliser_rtss('00035-02-065-31C0'),'000350206531C0')
        self.assertEqual(normaliser_rtss(' 000350206531c0 '),'000350206531C0')

    def test_rtss_does_not_guess_truncated_or_chainage_strings(self):
        for value in [None,12345,'35-02-065-31C0','00035-02-065-31C0 (0)','00035/02/065/31C0']:
            with self.subTest(value=value):self.assertIsNone(normaliser_rtss(value))

    def test_route_list_is_normalised_not_description_numbers(self):
        self.assertEqual(routes_travaux('A-20 / 40 et Route 138'),['00020','00040','00138'])
        self.assertEqual(routes_travaux('Travaux en 2026 sur 25'),[])
        self.assertEqual(routes_travaux(None),[])

    def test_naive_work_dates_use_toronto_timezone(self):
        start,end,status=dates_travaux(dict(debut='2026/11/11 08:00:00',fin='2026/11/12 09:00:00'))
        self.assertEqual(status,'ok');self.assertEqual(start.hour,13);self.assertGreater(end,start)

    def test_missing_or_ambiguous_dates_are_not_active_by_default(self):
        for props in [{},dict(debut='2026/11/01 01:30:00',fin='2026/11/02 08:00:00'),
                      dict(debut='n/a',fin='2026-11-11')]:
            with self.subTest(props=props):self.assertEqual(dates_travaux(props),(None,None,'inconnues'))

    def test_bad_configuration_fails(self):
        for update in [dict(bbox=[-70,47,-71,46]),dict(meteo_rayon_m=True),
                       dict(debit_distance_m=1000),dict(debit_fraction_min=2),dict(travaux_distance_m=float('nan'))]:
            cfg=dict(version=1,bbox=[-71.65,46.65,-70.95,47.05]);cfg.update(update)
            with self.subTest(update=update),self.assertRaises(ValueError):valider_config(cfg)

    def test_whole_road_outside_weather_radius_remains_uncovered(self):
        self.assertEqual(statut_meteo(100,20000,4000,config()),'hors_couverture')

    def test_nearly_equal_weather_points_are_flagged(self):
        self.assertEqual(statut_meteo(1000,2000,1100,config()),'a_verifier')

    def test_weather_representative_point(self):
        self.assertEqual(statut_meteo(1000,2000,5000,config()),'representatif')


if __name__=='__main__':unittest.main()


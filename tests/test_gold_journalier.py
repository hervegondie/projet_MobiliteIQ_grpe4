from datetime import date,datetime,timezone
import unittest
from transformation.regles_journalieres import fenetre_jour,jours_complets,valider_config
from transformation.situations_journalieres import hash_entrees


class ReglesJournalieresTests(unittest.TestCase):
    def test_dst_days_have_23_and_25_hours(self):
        for day,hours in [(date(2026,3,8),23),(date(2026,11,1),25),(date(2026,11,11),24)]:
            a,b=fenetre_jour(day);self.assertEqual((b-a).total_seconds()/3600,hours)

    def test_current_local_day_is_refused_even_if_utc_date_differs(self):
        now=datetime(2026,11,12,1,tzinfo=timezone.utc) # 11 novembre au Quebec
        with self.assertRaises(ValueError):jours_complets(date(2026,11,11),date(2026,11,11),now)
        self.assertEqual(jours_complets(date(2026,11,10),date(2026,11,10),now),[date(2026,11,10)])

    def test_naive_cutoff_is_refused(self):
        with self.assertRaises(ValueError):jours_complets(date(2026,1,1),date(2026,1,1),datetime(2026,2,1))

    def test_range_is_limited(self):
        with self.assertRaises(ValueError):jours_complets(date(2026,1,1),date(2026,3,1),datetime(2026,4,1,tzinfo=timezone.utc))

    def test_configuration_is_strict(self):
        for cfg in [dict(max_age_travaux_heures=True),dict(max_age_travaux_heures=0),dict(inconnu=3)]:
            with self.subTest(cfg=cfg),self.assertRaises(ValueError):valider_config(cfg)

    def test_hash_stable_for_row_order_but_changes_on_revision(self):
        rows=[dict(troncon_id='b',temperature_min=-2),dict(troncon_id='a',temperature_min=0)]
        h=hash_entrees(date(2026,1,1),'correspondance',{},rows)
        self.assertEqual(h,hash_entrees(date(2026,1,1),'correspondance',{},list(reversed(rows))))
        rows[0]['temperature_min']=-9
        self.assertNotEqual(h,hash_entrees(date(2026,1,1),'correspondance',{},rows))


if __name__=='__main__':unittest.main()


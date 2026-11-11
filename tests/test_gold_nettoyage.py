"""Regression du nettoyage : dependances FK et donnees d'un autre run preservees."""
import re
import unittest
from unittest.mock import Mock
from uuid import uuid4
from test_neon_correspondances import nettoyer_runs_gold


class NettoyageGoldTests(unittest.TestCase):
    def test_link_children_are_deleted_before_points_and_run(self):
        own,other=uuid4(),uuid4()
        dependances={
            'correspondance_runs':['points_meteo_correspondance','troncons_correspondance',
                                    'correspondance_publication','correspondances_non_appariees'],
            'points_meteo_correspondance':['pont_troncon_meteo'],
            'troncons_correspondance':['pont_troncon_meteo','pont_troncon_travaux','pont_troncon_debit'],
        }
        tables=set(dependances)|{child for children in dependances.values() for child in children}
        rows={name:{own,other} for name in tables}
        def execute(query,params):
            m=re.fullmatch(r'DELETE FROM gold\.([a-z_]+) WHERE run_id=ANY\(%s\)',query)
            self.assertIsNotNone(m,'Suppression non limitee aux identifiants de test')
            table=m[1];ids=set(params[0])
            for child in dependances.get(table,[]):
                if ids & rows[child]:
                    raise RuntimeError('Violation FK simulee : '+child)
            rows[table]-=ids
        conn=Mock();conn.execute.side_effect=execute
        nettoyer_runs_gold(conn,[own])
        self.assertTrue(all(values=={other} for values in rows.values()))

    def test_empty_run_list_never_deletes(self):
        conn=Mock();nettoyer_runs_gold(conn,[]);conn.execute.assert_not_called()

    def test_replay_uses_only_the_same_unique_identifiers(self):
        conn=Mock();rid=uuid4();nettoyer_runs_gold(conn,[rid,rid])
        for call in conn.execute.call_args_list:
            self.assertEqual(call.args[1],([rid],))


if __name__=='__main__':unittest.main()


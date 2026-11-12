"""Controles de diffusion et API Power BI simulee ; aucun acces externe."""
from datetime import date,datetime,timedelta,timezone
from unittest import TestCase
from unittest.mock import Mock
from scripts.controler_gold_automatisation import verifier_sources,verifier_jour
from scripts.actualiser_powerbi import configuration,actualiser

UUID='11111111-1111-4111-8111-111111111111'


class ControleDiffusionTests(TestCase):
    def setUp(self):self.now=datetime(2026,11,12,12,tzinfo=timezone.utc)

    def sources(self):
        return {s:{'collecte_at':(self.now-timedelta(hours=1)).isoformat()} for s in ['routes','travaux','debits']}

    def test_travaux_anciens_bloquent_la_diffusion(self):
        s=self.sources();s['travaux']['collecte_at']=(self.now-timedelta(hours=5)).isoformat()
        with self.assertRaises(ValueError):verifier_sources(s,self.now)

    def test_reference_ancienne_bloque_la_diffusion(self):
        s=self.sources();s['routes']['collecte_at']=(self.now-timedelta(hours=169)).isoformat()
        with self.assertRaises(ValueError):verifier_sources(s,self.now)

    def test_collecte_future_refusee(self):
        s=self.sources();s['travaux']['collecte_at']=(self.now+timedelta(minutes=1)).isoformat()
        with self.assertRaises(ValueError):verifier_sources(s,self.now)

    def test_sources_fraiches_acceptees(self):
        self.assertEqual(verifier_sources(self.sources(),self.now)['travaux'],1)

    def rapport(self):
        return dict(mode='chargement',resultats=[dict(date_jour='2026-11-11',statut='publie',
            bilan=dict(lignes=100,avec_meteo=74,travaux_inconnus=0,eligibles_ml=60))])

    def test_zero_meteo_bloque_powerbi(self):
        r=self.rapport();r['resultats'][0]['bilan']['avec_meteo']=0
        with self.assertRaises(ValueError):verifier_jour(r,date(2026,11,11))

    def test_tous_travaux_inconnus_bloquent_powerbi(self):
        r=self.rapport();r['resultats'][0]['bilan']['travaux_inconnus']=100
        with self.assertRaises(ValueError):verifier_jour(r,date(2026,11,11))

    def test_autre_jour_ne_suffit_pas(self):
        with self.assertRaises(ValueError):verifier_jour(self.rapport(),date(2026,11,12))

    def test_preview_ne_declenche_pas_powerbi(self):
        r=self.rapport();r['mode']='verification_sans_publication'
        with self.assertRaises(ValueError):verifier_jour(r,date(2026,11,11))

    def test_couverture_partielle_acceptee_sans_exiger_tout_le_ml(self):
        self.assertEqual(verifier_jour(self.rapport(),date(2026,11,11))['bilan']['lignes'],100)


class ActualisationPowerBITests(TestCase):
    def config(self):
        return {k:('SECRET_FICTIF' if k=='PBI_CLIENT_SECRET' else UUID) for k in
            ['PBI_TENANT_ID','PBI_CLIENT_ID','PBI_CLIENT_SECRET','PBI_WORKSPACE_ID','PBI_DATASET_ID']}

    def response(self,status,data=None,headers=None):
        r=Mock(status_code=status,headers=headers or {});r.json.return_value=data or {};return r

    def session(self,statut='Completed'):
        s=Mock()
        s.post.side_effect=[self.response(200,dict(access_token='JETON_FICTIF')),
                            self.response(202,headers={'x-ms-request-id':UUID})]
        s.get.side_effect=[self.response(200,{'value':[]}),
                          self.response(200,{'value':[dict(requestId=UUID,status=statut)]})]
        return s

    def test_secret_absent_refuse_avant_appel(self):
        cfg=self.config();del cfg['PBI_CLIENT_SECRET']
        with self.assertRaises(ValueError):configuration(cfg)

    def test_succes_attend_le_bon_identifiant(self):
        s=self.session();result=actualiser(self.config(),s,pause=lambda _:None)
        self.assertEqual(result['statut'],'Completed');self.assertEqual(s.post.call_count,2)

    def test_echec_powerbi_ne_devient_pas_un_succes_pipeline(self):
        with self.assertRaises(RuntimeError):actualiser(self.config(),self.session('Failed'))

    def test_actualisation_deja_en_cours_evite_second_post(self):
        s=self.session();s.get.side_effect=[self.response(200,{'value':[dict(status='Unknown')]})]
        with self.assertRaises(RuntimeError):actualiser(self.config(),s)
        self.assertEqual(s.post.call_count,1)  # Authentification seulement.

    def test_autre_actualisation_terminee_ne_valide_pas_la_notre(self):
        s=self.session();s.get.side_effect=[self.response(200,{'value':[]}),
            self.response(200,{'value':[dict(requestId='autre',status='Completed')]})]
        times=iter([0,0,2])
        with self.assertRaises(TimeoutError):
            actualiser(self.config(),s,attente=1,pause=lambda _:None,horloge=lambda:next(times))

    def test_429_ne_relance_pas_le_post(self):
        s=self.session();s.post.side_effect=[self.response(200,dict(access_token='JETON_FICTIF')),
                                           self.response(429)]
        with self.assertRaises(RuntimeError):actualiser(self.config(),s)
        self.assertEqual(s.post.call_count,2)

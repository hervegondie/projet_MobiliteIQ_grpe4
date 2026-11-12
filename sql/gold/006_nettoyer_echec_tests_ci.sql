-- Nettoyage exceptionnel des QUATRE runs du journal GitHub 43116511860.
-- Uniquement ci-test-123 / Silver_test. Refuse la production sans marqueur de test.
-- Ne pas generaliser ces DELETE ni retirer les verifications.
BEGIN;
DO $$ BEGIN
 IF current_database()<>'Silver_test' THEN RAISE EXCEPTION 'Base Silver_test requise'; END IF;
 IF to_regclass('public.ci_test_environment') IS NULL THEN
  RAISE EXCEPTION 'Marqueur de test absent : aucune suppression autorisee';
 END IF;
 IF (SELECT count(*) FROM public.ci_test_environment)<>1 OR
    NOT EXISTS(SELECT 1 FROM public.ci_test_environment WHERE environment='ci-test-123') THEN
  RAISE EXCEPTION 'Ce script est reserve a ci-test-123';
 END IF;
END $$;
SELECT pg_advisory_xact_lock(740221999);
CREATE TEMP TABLE nettoyage_runs ON COMMIT DROP AS
SELECT * FROM gold.correspondance_runs
WHERE run_id IN (
 'fdb80626-fb43-47f4-b785-851dab377bd0'::uuid,
 'bd71df81-deba-4185-a199-4e71f173c061'::uuid,
 'b84dde22-e710-4632-8e1a-c27460c05d20'::uuid,
 'a3f26a25-fe43-4400-a9e7-c99cb35afb9d'::uuid
);
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM nettoyage_runs WHERE
  coalesce(jsonb_array_length(configuration->'points'),-1)<>1 OR
  coalesce(configuration->'points'->0->>'lieu_id','')<>'test_quebec') THEN
  RAISE EXCEPTION 'Un run ne correspond pas au jeu synthetique attendu';
 END IF;
 IF EXISTS(SELECT 1 FROM gold.troncons_correspondance t JOIN nettoyage_runs r USING(run_id)
  WHERE t.troncon_id !~ '^ci_corr_[0-9a-f]{32}_r[123]$') THEN
  RAISE EXCEPTION 'Un troncon cible ne porte pas un identifiant synthetique';
 END IF;
END $$;
CREATE TEMP TABLE nettoyage_collectes ON COMMIT DROP AS
SELECT DISTINCT (s.value->>'collecte_id')::uuid AS collecte_id
FROM nettoyage_runs r CROSS JOIN LATERAL jsonb_each(r.sources) s;
CREATE TEMP TABLE nettoyage_versions ON COMMIT DROP AS
SELECT DISTINCT o.source,o.identifiant,o.empreinte FROM public.geo_observations o
JOIN nettoyage_collectes c USING(collecte_id);
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM nettoyage_versions WHERE
   identifiant !~ '^ci_corr_[0-9a-f]{32}_(r[123]|w1|w2|w_voisine|d[123])$') THEN
  RAISE EXCEPTION 'La collecte contient des objets autres que les fixtures attendues';
 END IF;
END $$;
-- Enfants Gold puis parents : corrige la violation FK observee.
DELETE FROM gold.pont_troncon_meteo WHERE run_id IN(SELECT run_id FROM nettoyage_runs);
DELETE FROM gold.pont_troncon_travaux WHERE run_id IN(SELECT run_id FROM nettoyage_runs);
DELETE FROM gold.pont_troncon_debit WHERE run_id IN(SELECT run_id FROM nettoyage_runs);
DELETE FROM gold.correspondances_non_appariees WHERE run_id IN(SELECT run_id FROM nettoyage_runs);
DELETE FROM gold.correspondance_publication WHERE run_id IN(SELECT run_id FROM nettoyage_runs);
DELETE FROM gold.troncons_correspondance WHERE run_id IN(SELECT run_id FROM nettoyage_runs);
DELETE FROM gold.points_meteo_correspondance WHERE run_id IN(SELECT run_id FROM nettoyage_runs);
DELETE FROM gold.correspondance_runs WHERE run_id IN(SELECT run_id FROM nettoyage_runs);
-- Observations et versions de geometries propres a ces seuls jeux synthetiques.
DELETE FROM public.geo_rejets WHERE collecte_id IN(SELECT collecte_id FROM nettoyage_collectes);
DELETE FROM public.geo_observations WHERE collecte_id IN(SELECT collecte_id FROM nettoyage_collectes);
DELETE FROM public.geo_collectes_silver WHERE collecte_id IN(SELECT collecte_id FROM nettoyage_collectes);
DELETE FROM public.debits_reference_geo d USING nettoyage_versions v
WHERE d.source=v.source AND d.identifiant=v.identifiant AND d.empreinte=v.empreinte
 AND NOT EXISTS(SELECT 1 FROM public.geo_observations o
   WHERE o.source=v.source AND o.identifiant=v.identifiant AND o.empreinte=v.empreinte);
DELETE FROM public.geometries_versions g USING nettoyage_versions v
WHERE g.source=v.source AND g.identifiant=v.identifiant AND g.empreinte=v.empreinte
 AND NOT EXISTS(SELECT 1 FROM public.geo_observations o
   WHERE o.source=v.source AND o.identifiant=v.identifiant AND o.empreinte=v.empreinte);
SELECT count(*) AS anciens_runs_nettoyes FROM nettoyage_runs;
SELECT count(*) AS anciens_runs_restants FROM gold.correspondance_runs
WHERE run_id IN(SELECT run_id FROM nettoyage_runs);
COMMIT;

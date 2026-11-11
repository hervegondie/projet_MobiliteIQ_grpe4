DO $$ BEGIN
 IF current_database()<>'Silver_test' OR to_regclass('public.geometries_versions') IS NULL THEN
  RAISE EXCEPTION 'Installer les prerequis geospatiaux dans Silver_test';
 END IF;
END $$;
CREATE OR REPLACE FUNCTION gold.normaliser_rtss(value text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$
 SELECT CASE
 WHEN upper(btrim(value)) ~ '^[0-9]{5}-[0-9]{2}-[0-9]{3}-[A-Z0-9]{4}$'
 THEN replace(upper(btrim(value)),'-','')
 WHEN upper(btrim(value)) ~ '^[0-9]{10}[A-Z0-9]{4}$' THEN upper(btrim(value))
 ELSE NULL END
$$;
CREATE TABLE IF NOT EXISTS gold.correspondance_runs (
 run_id uuid PRIMARY KEY,
 empreinte_entrees text UNIQUE NOT NULL,
 algorithme text NOT NULL,
 cree_at timestamptz NOT NULL DEFAULT now(),
 sources jsonb NOT NULL,
 configuration jsonb NOT NULL,
 bilan jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS gold.points_meteo_correspondance (
 run_id uuid NOT NULL REFERENCES gold.correspondance_runs ON DELETE CASCADE,
 lieu_id text NOT NULL,
 nom text NOT NULL,
 geom geometry(Point,4326) NOT NULL,
 PRIMARY KEY(run_id,lieu_id)
);
CREATE TABLE IF NOT EXISTS gold.troncons_correspondance (
 run_id uuid NOT NULL REFERENCES gold.correspondance_runs ON DELETE CASCADE,
 troncon_id text NOT NULL,
 empreinte_source text NOT NULL,
 numero_route text,
 rtss text,
 proprietes jsonb NOT NULL,
 geom geometry(MultiLineString,4326) NOT NULL,
 longueur_m double precision NOT NULL CHECK(longueur_m>0),
 PRIMARY KEY(run_id,troncon_id)
);
CREATE INDEX IF NOT EXISTS troncons_correspondance_gist ON gold.troncons_correspondance USING gist(geom);
CREATE TABLE IF NOT EXISTS gold.pont_troncon_meteo (
 run_id uuid NOT NULL,
 troncon_id text NOT NULL,
 lieu_id text,
 lieu_candidat text NOT NULL,
 distance_repere_m double precision NOT NULL,
 distance_max_troncon_m double precision NOT NULL,
 ecart_second_m double precision,
 statut text NOT NULL CHECK(statut IN ('representatif','a_verifier','hors_couverture')),
 PRIMARY KEY(run_id,troncon_id),
 FOREIGN KEY(run_id,troncon_id) REFERENCES gold.troncons_correspondance ON DELETE CASCADE,
 FOREIGN KEY(run_id,lieu_id) REFERENCES gold.points_meteo_correspondance,
 FOREIGN KEY(run_id,lieu_candidat) REFERENCES gold.points_meteo_correspondance
);
CREATE TABLE IF NOT EXISTS gold.pont_troncon_travaux (
 run_id uuid NOT NULL,
 troncon_id text NOT NULL,
 travaux_id text NOT NULL,
 chantier_id text,
 empreinte_source text NOT NULL,
 distance_m double precision NOT NULL,
 longueur_proximite_m double precision NOT NULL,
 fraction_proximite double precision NOT NULL CHECK(fraction_proximite BETWEEN 0 AND 1),
 longueur_commune_exacte_m double precision NOT NULL,
 route_compatible boolean NOT NULL,
 debut timestamptz,
 fin timestamptz,
 statut_dates text NOT NULL CHECK(statut_dates IN ('ok','inconnues')),
 statut text NOT NULL CHECK(statut IN ('probable','a_verifier')),
 geom_portion geometry(MultiLineString,32198) NOT NULL,
 PRIMARY KEY(run_id,troncon_id,travaux_id),
 FOREIGN KEY(run_id,troncon_id) REFERENCES gold.troncons_correspondance ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS gold.pont_troncon_debit (
 run_id uuid NOT NULL,
 troncon_id text NOT NULL,
 section_trafic_id text NOT NULL,
 empreinte_source text NOT NULL,
 methode text NOT NULL CHECK(methode IN ('rtss_complet_et_geometrie','rtss_extremite_et_geometrie','proximite_seule')),
 distance_m double precision NOT NULL,
 fraction_proximite double precision NOT NULL CHECK(fraction_proximite BETWEEN 0 AND 1),
 rang integer NOT NULL,
 statut text NOT NULL CHECK(statut IN ('probable','ambigu','a_verifier')),
 PRIMARY KEY(run_id,troncon_id,section_trafic_id),
 FOREIGN KEY(run_id,troncon_id) REFERENCES gold.troncons_correspondance ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS gold.correspondances_non_appariees (
 run_id uuid NOT NULL REFERENCES gold.correspondance_runs ON DELETE CASCADE,
 type_objet text NOT NULL,
 identifiant text NOT NULL,
 raison text NOT NULL,
 PRIMARY KEY(run_id,type_objet,identifiant)
);
CREATE TABLE IF NOT EXISTS gold.correspondance_publication (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 run_id uuid NOT NULL REFERENCES gold.correspondance_runs ON DELETE CASCADE,
 publie_at timestamptz NOT NULL DEFAULT now()
);
CREATE OR REPLACE VIEW gold.v_dernier_run_correspondances AS
SELECT r.* FROM gold.correspondance_runs r JOIN gold.correspondance_publication p USING(run_id);
CREATE OR REPLACE VIEW gold.v_correspondances_meteo AS
SELECT m.* FROM gold.pont_troncon_meteo m JOIN gold.v_dernier_run_correspondances r USING(run_id);
CREATE OR REPLACE VIEW gold.v_correspondances_travaux AS
SELECT m.* FROM gold.pont_troncon_travaux m JOIN gold.v_dernier_run_correspondances r USING(run_id);
CREATE OR REPLACE VIEW gold.v_correspondances_debits AS
SELECT m.* FROM gold.pont_troncon_debit m JOIN gold.v_dernier_run_correspondances r USING(run_id);
CREATE OR REPLACE VIEW gold.v_debit_troncon_reference AS
SELECT p.run_id,p.troncon_id,p.section_trafic_id,p.methode,
       d.annee_reference,d.indicateur,d.valeur,d.periode,d.unite,false AS temps_reel
FROM gold.v_correspondances_debits p JOIN public.debits_reference_geo d
 ON d.identifiant=p.section_trafic_id AND d.empreinte=p.empreinte_source
WHERE p.statut='probable';
-- Une seule ligne par troncon dans le contexte geographique courant.
-- Les mesures meteo sont a joindre ensuite sur lieu_id et sur la bonne date.
CREATE OR REPLACE VIEW gold.v_contexte_troncon_courant AS
WITH travaux AS (
 SELECT run_id,troncon_id,
 count(DISTINCT travaux_id) FILTER(WHERE statut='probable' AND statut_dates='ok'
  AND debut<=now() AND fin>now()) AS nb_entraves_actives_probables,
 count(DISTINCT chantier_id) FILTER(WHERE statut='probable' AND statut_dates='ok'
  AND debut<=now() AND fin>now()) AS nb_chantiers_actifs_probables,
 count(*) FILTER(WHERE statut='a_verifier' OR statut_dates='inconnues') AS nb_associations_a_verifier,
 ST_Length(ST_UnaryUnion(ST_Collect(geom_portion) FILTER(WHERE statut='probable'
  AND statut_dates='ok' AND debut<=now() AND fin>now()))) AS longueur_proximite_active_m
 FROM gold.v_correspondances_travaux GROUP BY run_id,troncon_id
), dernier_djma AS (
 SELECT DISTINCT ON(run_id,troncon_id) * FROM gold.v_debit_troncon_reference
 WHERE indicateur='djma' AND annee_reference<=extract(year FROM current_date)::integer
 ORDER BY run_id,troncon_id,annee_reference DESC
)
SELECT t.run_id,t.troncon_id,t.numero_route,t.rtss,t.longueur_m,
 ST_Y(ST_PointOnSurface(t.geom)) AS latitude_repere,
 ST_X(ST_PointOnSurface(t.geom)) AS longitude_repere,
 ST_AsGeoJSON(t.geom) AS geojson,
 m.lieu_id,m.statut AS statut_meteo,m.distance_repere_m,
 coalesce(w.nb_entraves_actives_probables,0) AS nb_entraves_actives_probables,
 coalesce(w.nb_chantiers_actifs_probables,0) AS nb_chantiers_actifs_probables,
 coalesce(w.nb_associations_a_verifier,0) AS nb_associations_a_verifier,
 least(t.longueur_m,coalesce(w.longueur_proximite_active_m,0)) AS longueur_proximite_active_m,
 d.valeur AS djma_reference,d.annee_reference AS annee_djma,
 d.section_trafic_id,false AS debit_temps_reel,r.cree_at AS correspondances_calculees_at,
 r.sources AS collectes_sources
FROM gold.troncons_correspondance t JOIN gold.v_dernier_run_correspondances r USING(run_id)
LEFT JOIN gold.pont_troncon_meteo m USING(run_id,troncon_id)
LEFT JOIN travaux w USING(run_id,troncon_id)
LEFT JOIN dernier_djma d USING(run_id,troncon_id);

-- Adaptateur optionnel : l'installation reste possible sans donnees meteo.
-- Relancer --installer si la vue meteo est creee apres cette migration.
DO $$ BEGIN
 IF to_regclass('public.v_meteo_actuelle_derniere') IS NOT NULL THEN
  EXECUTE 'CREATE OR REPLACE VIEW gold.v_meteo_actuelle_correspondances AS
   SELECT lieu_id::text,latitude::numeric,longitude::numeric,collecte_at::timestamptz,
   heure_meteo::timestamptz,temperature_c::numeric,precipitations_mm::numeric,
   neige_cm::numeric,vent_kmh::numeric,visibilite_m::numeric
   FROM public.v_meteo_actuelle_derniere';
 ELSE
  EXECUTE 'CREATE OR REPLACE VIEW gold.v_meteo_actuelle_correspondances AS
   SELECT NULL::text AS lieu_id,NULL::numeric AS latitude,NULL::numeric AS longitude,
   NULL::timestamptz AS collecte_at,NULL::timestamptz AS heure_meteo,
   NULL::numeric AS temperature_c,NULL::numeric AS precipitations_mm,
   NULL::numeric AS neige_cm,NULL::numeric AS vent_kmh,NULL::numeric AS visibilite_m WHERE false';
 END IF;
END $$;
CREATE OR REPLACE VIEW gold.v_situation_troncon_courante AS
WITH donnees AS (
 SELECT c.*,a.collecte_at AS meteo_collecte_at,a.heure_meteo,
 a.temperature_c,a.precipitations_mm,a.neige_cm,a.vent_kmh,a.visibilite_m,
 extract(epoch FROM(now()-a.heure_meteo))/60 AS age_meteo_minutes,
 CASE WHEN c.statut_meteo<>'representatif' THEN 'association_non_confirmee'
 WHEN a.lieu_id IS NULL THEN 'absente_ou_coordonnees_differentes'
 WHEN a.heure_meteo>now()+interval '5 minutes' THEN 'date_future_incoherente'
 WHEN now()-a.heure_meteo > make_interval(mins =>
  (r.configuration->'parametres'->>'meteo_fraicheur_minutes')::integer) THEN 'perimee'
 ELSE 'recente' END AS qualite_meteo
 FROM gold.v_contexte_troncon_courant c
 JOIN gold.v_dernier_run_correspondances r USING(run_id)
 LEFT JOIN gold.points_meteo_correspondance p ON p.run_id=c.run_id AND p.lieu_id=c.lieu_id
 LEFT JOIN gold.v_meteo_actuelle_correspondances a ON a.lieu_id=c.lieu_id
 AND c.statut_meteo='representatif'
 AND abs(a.latitude-ST_Y(p.geom))<0.000001 AND abs(a.longitude-ST_X(p.geom))<0.000001
)
SELECT *,CASE WHEN qualite_meteo='recente' THEN temperature_c ELSE NULL END AS temperature_utilisable_c,
 CASE WHEN qualite_meteo='recente' THEN visibilite_m ELSE NULL END AS visibilite_utilisable_m
FROM donnees;


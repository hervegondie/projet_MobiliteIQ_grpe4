-- Executer uniquement dans Silver_test (branche de test d'abord).
-- Aucun DROP, TRUNCATE ou changement des tables Silver existantes.
DO $$ BEGIN
  IF current_database() <> 'Silver_test' THEN
    RAISE EXCEPTION 'Cette migration cible exclusivement Silver_test';
  END IF;
END $$;
CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS public.geo_collectes_silver (
  collecte_id uuid NOT NULL,
  source text NOT NULL CHECK (source IN ('routes','travaux','debits')),
  collecte_at timestamptz NOT NULL,
  payload_hash text NOT NULL,
  attendues integer NOT NULL CHECK (attendues >= 0),
  valides integer NOT NULL CHECK (valides >= 0),
  rejetees integer NOT NULL CHECK (rejetees >= 0),
  traite_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (collecte_id,source),
  CHECK (valides + rejetees = attendues)
);
CREATE TABLE IF NOT EXISTS public.geometries_versions (
  source text NOT NULL CHECK (source IN ('routes','travaux','debits')),
  identifiant text NOT NULL,
  empreinte text NOT NULL,
  proprietes jsonb NOT NULL,
  geom geometry(MultiLineString,4326) NOT NULL,
  PRIMARY KEY (source,identifiant,empreinte),
  CHECK (NOT ST_IsEmpty(geom) AND ST_IsValid(geom)),
  CHECK (ST_Length(geom::geography) > 0)
);
CREATE INDEX IF NOT EXISTS geometries_versions_geom_idx
  ON public.geometries_versions USING gist(geom);
CREATE INDEX IF NOT EXISTS geometries_versions_geog_idx
  ON public.geometries_versions USING gist((geom::geography));
CREATE TABLE IF NOT EXISTS public.geo_observations (
  collecte_id uuid NOT NULL,
  source text NOT NULL,
  identifiant text NOT NULL,
  empreinte text NOT NULL,
  PRIMARY KEY (collecte_id,source,identifiant),
  FOREIGN KEY (collecte_id,source) REFERENCES public.geo_collectes_silver(collecte_id,source),
  FOREIGN KEY (source,identifiant,empreinte)
    REFERENCES public.geometries_versions(source,identifiant,empreinte)
);
CREATE TABLE IF NOT EXISTS public.geo_rejets (
  collecte_id uuid NOT NULL,
  source text NOT NULL,
  identifiant text NOT NULL,
  raisons jsonb NOT NULL,
  feature jsonb NOT NULL,
  PRIMARY KEY (collecte_id,source,identifiant),
  FOREIGN KEY (collecte_id,source) REFERENCES public.geo_collectes_silver(collecte_id,source)
);
CREATE TABLE IF NOT EXISTS public.debits_reference_geo (
  source text NOT NULL DEFAULT 'debits' CHECK(source='debits'),
  identifiant text NOT NULL,
  empreinte text NOT NULL,
  annee_reference integer NOT NULL CHECK (annee_reference BETWEEN 1900 AND 2100),
  indicateur text NOT NULL CHECK(indicateur IN ('djma','djme','djmh','cam')),
  valeur numeric NOT NULL CHECK(valeur >= 0),
  periode text NOT NULL,
  unite text NOT NULL,
  PRIMARY KEY (identifiant,empreinte,annee_reference,indicateur),
  FOREIGN KEY (source,identifiant,empreinte)
    REFERENCES public.geometries_versions(source,identifiant,empreinte),
  CHECK(indicateur <> 'cam' OR valeur <= 100),
  CHECK (
    (indicateur='djma' AND periode='annuelle' AND unite='vehicules/jour') OR
    (indicateur='djme' AND periode='estivale' AND unite='vehicules/jour') OR
    (indicateur='djmh' AND periode='hivernale' AND unite='vehicules/jour') OR
    (indicateur='cam' AND periode='annuelle' AND unite='pourcent')
  )
);
-- La derniere collecte publiee remplace la population courante d'une source.
-- Un chantier disparu ne doit pas reapparaitre via un "dernier par identifiant".
CREATE OR REPLACE VIEW public.v_geometries_courantes AS
WITH dernieres AS (
 SELECT DISTINCT ON (source) source,collecte_id,collecte_at
 FROM public.geo_collectes_silver
 ORDER BY source,collecte_at DESC,collecte_id DESC
)
SELECT g.*,d.collecte_id,d.collecte_at
FROM dernieres d
JOIN public.geo_observations o USING (source,collecte_id)
JOIN public.geometries_versions g USING (source,identifiant,empreinte);

CREATE OR REPLACE VIEW gold.dim_troncon AS
SELECT identifiant AS troncon_id,proprietes->>'num_rts' AS rtss_source,
       proprietes->>'num_route' AS numero_route,
       proprietes->>'des_clasf_' AS classe_route,
       proprietes->>'nom_sous_r' AS nom,
       ST_Length(geom::geography) AS longueur_geometrique_m,
       ST_Y(ST_PointOnSurface(geom)) AS latitude_repere,
       ST_X(ST_PointOnSurface(geom)) AS longitude_repere,
       ST_AsGeoJSON(geom) AS geojson,geom,collecte_at
FROM public.v_geometries_courantes WHERE source='routes';

CREATE OR REPLACE VIEW gold.v_travaux_geometries AS
SELECT identifiant,proprietes->>'identifiantChantier' AS chantier_id,
       proprietes->>'routeAutoroute' AS route_source,
       proprietes,geom,collecte_at
FROM public.v_geometries_courantes WHERE source='travaux';

CREATE OR REPLACE VIEW gold.fact_debit_reference AS
SELECT r.identifiant AS section_trafic_id,
       g.proprietes->>'num_sectn_trafc' AS numero_section,
       g.proprietes->>'rtss_debut' AS rtss_debut,
       g.proprietes->>'rtss_fin' AS rtss_fin,
       r.annee_reference,r.indicateur,r.valeur,r.periode,r.unite,
       g.collecte_at AS collecte_source_at,
       extract(year FROM current_date)::integer-r.annee_reference AS anciennete_annees,
       false AS temps_reel,g.geom
FROM public.v_geometries_courantes g JOIN public.debits_reference_geo r
  USING (source,identifiant,empreinte)
WHERE g.source='debits';

CREATE OR REPLACE VIEW gold.v_debits_reference_dernieres AS
SELECT DISTINCT ON(section_trafic_id,indicateur) *
FROM gold.fact_debit_reference
WHERE annee_reference <= extract(year FROM current_date)::integer
ORDER BY section_trafic_id,indicateur,annee_reference DESC;

CREATE OR REPLACE VIEW gold.v_qualite_geometries AS
SELECT c.*,
       extract(epoch FROM (now()-collecte_at))/3600 AS age_collecte_heures
FROM public.geo_collectes_silver c;


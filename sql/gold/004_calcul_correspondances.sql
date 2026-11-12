-- Tables temporaires g2_routes/g2_travaux/g2_debits/g2_points/g2_config preparees par Python.
CREATE INDEX ON g2_routes USING gist(geom_m);
CREATE INDEX ON g2_travaux USING gist(geom_m);
CREATE INDEX ON g2_debits USING gist(geom_m);
ANALYZE g2_routes;
ANALYZE g2_travaux;
ANALYZE g2_debits;

CREATE TEMP TABLE g2_meteo ON COMMIT DROP AS
WITH choix AS (
 SELECT r.identifiant AS troncon_id,p.lieu_id AS lieu_candidat,
 ST_Distance(r.repere_m,p.geom_m) AS distance_repere_m,
 ST_MaxDistance(r.geom_m,p.geom_m) AS distance_max_troncon_m,
 (SELECT min(ST_Distance(r.repere_m,q.geom_m)) FROM g2_points q WHERE q.lieu_id<>p.lieu_id)
  - ST_Distance(r.repere_m,p.geom_m) AS ecart_second_m,c.cfg
 FROM g2_routes r CROSS JOIN g2_config c
 CROSS JOIN LATERAL(SELECT * FROM g2_points q ORDER BY ST_Distance(r.repere_m,q.geom_m),q.lieu_id LIMIT 1) p
), statuts AS (
 SELECT *,CASE WHEN distance_max_troncon_m>(cfg->>'meteo_rayon_m')::double precision
  THEN 'hors_couverture'
  WHEN ecart_second_m<(cfg->>'meteo_ecart_min_m')::double precision THEN 'a_verifier'
  ELSE 'representatif' END AS statut FROM choix
)
SELECT troncon_id,CASE WHEN statut='hors_couverture' THEN NULL ELSE lieu_candidat END AS lieu_id,
 lieu_candidat,distance_repere_m,distance_max_troncon_m,ecart_second_m,statut FROM statuts;

CREATE TEMP TABLE g2_pont_travaux ON COMMIT DROP AS
WITH candidats AS (
 SELECT r.identifiant AS troncon_id,w.identifiant AS travaux_id,
 w.proprietes->>'identifiantChantier' AS chantier_id,w.empreinte,
 ST_Distance(r.geom_m,w.geom_m) AS distance_m,
 ST_Multi(ST_CollectionExtract(ST_Intersection(r.geom_m,
  ST_Buffer(w.geom_m,(c.cfg->>'travaux_distance_m')::double precision,'endcap=flat')),2)) AS geom_portion,
 ST_Length(ST_Intersection(r.geom_m,w.geom_m)) AS exacte_m,r.longueur_m,
 r.numero_route=ANY(w.numeros_routes) AS compatible,
 w.debut,w.fin,w.statut_dates,c.cfg
 FROM g2_travaux w CROSS JOIN g2_config c JOIN g2_routes r
  ON ST_DWithin(r.geom_m,w.geom_m,(c.cfg->>'travaux_distance_m')::double precision)
 WHERE cardinality(w.numeros_routes)=0 OR r.numero_route=ANY(w.numeros_routes)
), mesures AS (
 SELECT *,ST_Length(geom_portion) AS longueur_proximite_m,
 least(1.0,ST_Length(geom_portion)/longueur_m) AS fraction_proximite FROM candidats
)
SELECT troncon_id,travaux_id,chantier_id,empreinte,distance_m,longueur_proximite_m,
 fraction_proximite,exacte_m,coalesce(compatible,false) AS route_compatible,
 debut,fin,statut_dates,
 CASE WHEN compatible AND exacte_m >= (cfg->>'travaux_longueur_min_m')::double precision
 THEN 'probable' ELSE 'a_verifier' END AS statut,geom_portion
FROM mesures
WHERE longueur_proximite_m >= (cfg->>'travaux_longueur_min_m')::double precision
 AND fraction_proximite >= (cfg->>'travaux_fraction_min')::double precision;

CREATE TEMP TABLE g2_pont_debits ON COMMIT DROP AS
WITH candidats AS (
 SELECT r.identifiant AS troncon_id,d.identifiant AS section_trafic_id,d.empreinte,
 ST_Distance(r.geom_m,d.geom_m) AS distance_m,
 least(1.0,ST_Length(ST_Intersection(r.geom_m,
  ST_Buffer(d.geom_m,(c.cfg->>'debit_distance_m')::double precision,'endcap=flat')))/r.longueur_m) AS fraction_proximite,
 CASE WHEN r.rtss IS NOT NULL AND r.rtss=d.rtss_debut AND r.rtss=d.rtss_fin THEN 'rtss_complet_et_geometrie'
  WHEN r.rtss=d.rtss_debut OR r.rtss=d.rtss_fin THEN 'rtss_extremite_et_geometrie'
  ELSE 'proximite_seule' END AS methode,c.cfg
 FROM g2_debits d CROSS JOIN g2_config c JOIN g2_routes r
  ON ST_DWithin(r.geom_m,d.geom_m,(c.cfg->>'debit_distance_m')::double precision)
), eligibles AS (
 SELECT *,methode='rtss_complet_et_geometrie'
  AND fraction_proximite >= (cfg->>'debit_fraction_min')::double precision AS eligible
 FROM candidats WHERE fraction_proximite>=0.05
), classes AS (
 SELECT *,row_number() OVER(PARTITION BY troncon_id ORDER BY eligible DESC,fraction_proximite DESC,distance_m,section_trafic_id) AS rang,
 lead(fraction_proximite) OVER(PARTITION BY troncon_id ORDER BY eligible DESC,fraction_proximite DESC,distance_m,section_trafic_id) AS seconde_fraction,
 lead(eligible) OVER(PARTITION BY troncon_id ORDER BY eligible DESC,fraction_proximite DESC,distance_m,section_trafic_id) AS seconde_eligible
 FROM eligibles
)
SELECT troncon_id,section_trafic_id,empreinte,methode,distance_m,fraction_proximite,rang::integer,
 CASE WHEN eligible AND rang=1 AND (NOT coalesce(seconde_eligible,false)
   OR fraction_proximite-seconde_fraction >= (cfg->>'debit_ecart_fraction_min')::double precision)
 THEN 'probable'
 WHEN eligible AND rang=1 THEN 'ambigu'
 ELSE 'a_verifier' END AS statut
FROM classes;

CREATE TEMP TABLE g2_non_apparies ON COMMIT DROP AS
SELECT 'troncon_sans_debit_probable'::text AS type_objet,r.identifiant,
 'Aucune section de debit unique confirmee par RTSS complet et geometrie'::text AS raison
FROM g2_routes r WHERE NOT EXISTS(SELECT 1 FROM g2_pont_debits p
 WHERE p.troncon_id=r.identifiant AND p.statut='probable')
UNION ALL
SELECT 'travaux_sans_troncon',w.identifiant,'Aucun candidat de longueur suffisante sur une route compatible dans le perimetre'
FROM g2_travaux w WHERE w.dans_perimetre
 AND NOT EXISTS(SELECT 1 FROM g2_pont_travaux p WHERE p.travaux_id=w.identifiant);


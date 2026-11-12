-- Choisir explicitement Silver_test et la branche voulue dans Neon.
SELECT current_database(), PostGIS_Full_Version();
SELECT source,collecte_at,attendues,valides,rejetees
FROM public.geo_collectes_silver
ORDER BY collecte_at DESC;
SELECT source,count(*) AS geometries_courantes,
       count(*) FILTER(WHERE ST_SRID(geom)<>4326 OR NOT ST_IsValid(geom)) AS invalides
FROM public.v_geometries_courantes GROUP BY source;
SELECT count(*) AS troncons FROM gold.dim_troncon;
SELECT annee_reference,indicateur,periode,unite,count(*) AS sections
FROM gold.fact_debit_reference GROUP BY 1,2,3,4 ORDER BY 1 DESC,2;
SELECT count(*) AS anomalies_de_contrat FROM gold.fact_debit_reference
WHERE temps_reel OR (indicateur IN ('djma','djme','djmh') AND unite<>'vehicules/jour');
-- Un nouvel instant de collecte ne devient jamais une nouvelle annee de mesure.
SELECT section_trafic_id,indicateur,annee_reference,valeur,collecte_source_at,anciennete_annees
FROM gold.v_debits_reference_dernieres ORDER BY section_trafic_id,indicateur LIMIT 30;


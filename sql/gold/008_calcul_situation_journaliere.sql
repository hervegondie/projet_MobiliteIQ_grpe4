-- Parametres pg_temp.j3_param prepares par le lanceur.
CREATE TEMP TABLE j3_faits ON COMMIT DROP AS
WITH param AS(SELECT * FROM j3_param), base AS (
 SELECT t.*,m.lieu_id,m.statut AS statut_meteo,p.geom AS point_meteo,
 r.sources,r.cree_at AS correspondance_creee_at
 FROM gold.troncons_correspondance t JOIN param a ON t.run_id=a.correspondance_run_id
 JOIN gold.correspondance_runs r ON r.run_id=t.run_id
 LEFT JOIN gold.pont_troncon_meteo m ON m.run_id=t.run_id AND m.troncon_id=t.troncon_id
 LEFT JOIN gold.points_meteo_correspondance p ON p.run_id=m.run_id AND p.lieu_id=m.lieu_id
), travaux AS (
 SELECT w.troncon_id,
 count(DISTINCT w.travaux_id) FILTER(WHERE w.statut='probable' AND w.statut_dates='ok'
   AND w.debut<=a.instant_reference AND w.fin>a.instant_reference) AS nb_entraves,
 count(DISTINCT w.chantier_id) FILTER(WHERE w.statut='probable' AND w.statut_dates='ok'
   AND w.debut<=a.instant_reference AND w.fin>a.instant_reference) AS nb_chantiers,
 count(*) FILTER(WHERE w.statut='a_verifier' OR w.statut_dates='inconnues') AS a_verifier,
 count(*) FILTER(WHERE w.statut='probable' AND w.statut_dates='ok' AND w.chantier_id IS NULL
   AND w.debut<=a.instant_reference AND w.fin>a.instant_reference) AS chantier_inconnu,
 ST_Length(ST_UnaryUnion(ST_Collect(w.geom_portion) FILTER(WHERE w.statut='probable'
   AND w.statut_dates='ok' AND w.debut<=a.instant_reference AND w.fin>a.instant_reference))) AS longueur_m,
 max(extract(epoch FROM(a.instant_reference-w.debut))/86400) FILTER(WHERE w.statut='probable'
   AND w.statut_dates='ok' AND w.debut<=a.instant_reference AND w.fin>a.instant_reference) AS duree_jours
 FROM gold.pont_troncon_travaux w JOIN param a ON w.run_id=a.correspondance_run_id
 GROUP BY w.troncon_id
), donnees AS (
 SELECT b.*,a.date_jour,a.instant_reference,a.cfg,
 coalesce(w.nb_entraves,0)::integer AS nb_entraves,
 coalesce(w.nb_chantiers,0)::integer AS nb_chantiers,
 coalesce(w.a_verifier,0)::integer AS a_verifier,
 coalesce(w.chantier_inconnu,0) AS chantier_inconnu,
 least(b.longueur_m,coalesce(w.longueur_m,0)) AS longueur_active,
 coalesce(w.duree_jours,0)::double precision AS duree_jours,
 (a.instant_reference-(b.sources->'travaux'->>'collecte_at')::timestamptz)
   <=make_interval(hours=>(a.cfg->>'max_age_travaux_heures')::integer)
   AND (b.sources->'travaux'->>'collecte_at')::timestamptz<=a.instant_reference AS travaux_frais,
 (a.instant_reference-(b.sources->'routes'->>'collecte_at')::timestamptz)
   <=make_interval(hours=>(a.cfg->>'max_age_routes_heures')::integer)
   AND (b.sources->'routes'->>'collecte_at')::timestamptz<=a.instant_reference AS routes_fraiches,
 (a.instant_reference-(b.sources->'debits'->>'collecte_at')::timestamptz)
   <=make_interval(hours=>(a.cfg->>'max_age_debits_heures')::integer)
   AND (b.sources->'debits'->>'collecte_at')::timestamptz<=a.instant_reference AS debits_frais,
 met.collecte_id AS collecte_meteo_id,met.collecte_at AS meteo_collecte_at,
 met.temperature_min,met.temperature_max,met.precipitation_totale,met.pluie,
 met.neige AS neige_cm,met.vitesse_vent_max AS vent_max_kmh,met.visibilite_moyenne AS visibilite_moyenne_m,
 dj.valeur AS djma_reference,dj.annee_reference AS annee_djma
 FROM base b CROSS JOIN param a LEFT JOIN travaux w USING(troncon_id)
 LEFT JOIN LATERAL (
  SELECT m.* FROM public.meteo_quotidienne_points m
  JOIN public.meteo_quotidienne_points_runs audit ON audit.collecte_id=m.collecte_id
  WHERE b.statut_meteo='representatif' AND m.lieu_id=b.lieu_id AND m.date=a.date_jour
   AND m.collecte_at<=a.connaissance_arretee_at
   AND audit.processed_at<=a.connaissance_arretee_at
   AND audit.lieu_id=m.lieu_id AND audit.collecte_at=m.collecte_at
   AND m.collecte_at>=a.fin_jour
   AND m.collecte_at<=a.fin_jour+make_interval(hours=>(a.cfg->>'retard_meteo_max_heures')::integer)
   AND abs(m.latitude-ST_Y(b.point_meteo))<0.000001 AND abs(m.longitude-ST_X(b.point_meteo))<0.000001
  ORDER BY m.collecte_at DESC,m.collecte_id DESC LIMIT 1
 ) met ON true
 LEFT JOIN LATERAL (
  SELECT d.valeur,d.annee_reference FROM gold.pont_troncon_debit p
  JOIN public.debits_reference_geo d ON d.identifiant=p.section_trafic_id AND d.empreinte=p.empreinte_source
  WHERE p.run_id=b.run_id AND p.troncon_id=b.troncon_id AND p.statut='probable'
   AND d.indicateur='djma' AND d.annee_reference<=extract(year FROM a.date_jour)::integer
  ORDER BY d.annee_reference DESC LIMIT 1
 ) dj ON true
), qualite AS (
 SELECT *,to_jsonb(array_remove(ARRAY[
  CASE WHEN NOT coalesce(routes_fraiches,false) THEN 'routes_anciennes_ou_futures' END,
  CASE WHEN NOT coalesce(travaux_frais,false) THEN 'travaux_anciens_ou_futurs' END,
  CASE WHEN coalesce(statut_meteo,'absente')<>'representatif' THEN 'association_meteo_non_confirmee' END,
  CASE WHEN collecte_meteo_id IS NULL THEN 'meteo_quotidienne_absente' END,
  CASE WHEN temperature_min IS NULL OR neige_cm IS NULL OR visibilite_moyenne_m IS NULL
   THEN 'variables_meteo_ml_incompletes' END,
  CASE WHEN a_verifier>0 THEN 'associations_travaux_a_verifier' END,
  CASE WHEN chantier_inconnu>0 THEN 'identifiant_chantier_absent' END
 ]::text[],NULL)) AS raisons_qualite
 FROM donnees
)
SELECT date_jour,troncon_id,empreinte_source,numero_route,
 proprietes->>'des_clasf_' AS classe_route,longueur_m,
 ST_Y(ST_PointOnSurface(geom)) AS latitude_repere,ST_X(ST_PointOnSurface(geom)) AS longitude_repere,
 ST_AsGeoJSON(geom) AS geojson,lieu_id,coalesce(statut_meteo,'absente') AS statut_meteo,
 CASE WHEN travaux_frais AND routes_fraiches THEN nb_entraves ELSE NULL END AS nb_entraves_actives,
 CASE WHEN travaux_frais AND routes_fraiches THEN nb_chantiers ELSE NULL END AS nb_chantiers_actifs,
 a_verifier AS nb_associations_a_verifier,
 CASE WHEN travaux_frais AND routes_fraiches THEN longueur_active ELSE NULL END AS longueur_proximite_active_m,
 CASE WHEN travaux_frais AND routes_fraiches THEN least(1.0,longueur_active/longueur_m) ELSE NULL END AS part_longueur_entravee,
 CASE WHEN travaux_frais AND routes_fraiches THEN duree_jours ELSE NULL END AS duree_max_entrave_jours,
 temperature_min,temperature_max,precipitation_totale,pluie,neige_cm,vent_max_kmh,visibilite_moyenne_m,
 CASE WHEN debits_frais THEN djma_reference ELSE NULL END AS djma_reference,
 CASE WHEN debits_frais THEN annee_djma ELSE NULL END AS annee_djma,
 false AS debit_temps_reel,collecte_meteo_id,meteo_collecte_at,
 sources || jsonb_build_object('debits_reference_frais',coalesce(debits_frais,false)) AS sources,
 raisons_qualite,CASE WHEN jsonb_array_length(raisons_qualite)=0 THEN 1 ELSE 0 END AS qualite_ok
FROM qualite;

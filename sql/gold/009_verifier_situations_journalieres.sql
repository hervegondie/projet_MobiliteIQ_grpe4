SELECT current_database();
SELECT * FROM gold.v_qualite_journalier ORDER BY date_jour DESC;
SELECT date_jour,count(*) AS lignes,count(DISTINCT troncon_id) AS troncons_uniques,
 sum(qualite_ok) AS eligibles_ml,count(*)-sum(qualite_ok) AS non_eligibles_ml
FROM gold.v_situation_journaliere GROUP BY date_jour ORDER BY date_jour DESC;
SELECT date_jour,troncon_id,nb_entraves_actives,nb_chantiers_actifs,temperature_min,
 neige_cm,djma_reference,annee_djma,qualite_ok,raisons_qualite
FROM gold.v_situation_journaliere ORDER BY date_jour DESC,troncon_id LIMIT 30;
SELECT date_jour,raison,count(*) FROM gold.v_situation_journaliere
CROSS JOIN LATERAL jsonb_array_elements_text(raisons_qualite) raison
GROUP BY date_jour,raison ORDER BY date_jour DESC,raison;
SELECT date_jour,run_id,cree_at,bilan FROM gold.situation_jour_runs ORDER BY cree_at DESC;
SELECT count(*) AS lignes_ml FROM gold.v_ml_situation_journaliere;
SELECT min((cree_at AT TIME ZONE 'America/Toronto')::date) AS premier_jour_avec_correspondances
FROM gold.correspondance_runs;

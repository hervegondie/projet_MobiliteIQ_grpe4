SELECT current_database();
SELECT run_id,cree_at,algorithme,bilan FROM gold.v_dernier_run_correspondances;
SELECT statut,count(*) FROM gold.v_correspondances_meteo GROUP BY statut;
SELECT statut,count(*) FROM gold.v_correspondances_travaux GROUP BY statut;
SELECT statut,methode,count(*) FROM gold.v_correspondances_debits GROUP BY statut,methode;
-- Les deux comptes doivent etre identiques : pas de multiplication des lignes.
SELECT count(*) AS lignes,count(DISTINCT troncon_id) AS troncons_uniques
FROM gold.v_contexte_troncon_courant;
SELECT troncon_id,numero_route,lieu_id,statut_meteo,nb_entraves_actives_probables,
       nb_chantiers_actifs_probables,djma_reference,annee_djma,debit_temps_reel
FROM gold.v_contexte_troncon_courant ORDER BY nb_entraves_actives_probables DESC,troncon_id LIMIT 30;
SELECT qualite_meteo,count(*) FROM gold.v_situation_troncon_courante GROUP BY qualite_meteo;
SELECT troncon_id,lieu_id,temperature_utilisable_c,visibilite_utilisable_m,
       meteo_collecte_at,heure_meteo,qualite_meteo
FROM gold.v_situation_troncon_courante ORDER BY troncon_id LIMIT 30;
SELECT n.* FROM gold.correspondances_non_appariees n
JOIN gold.v_dernier_run_correspondances r USING(run_id) LIMIT 50;

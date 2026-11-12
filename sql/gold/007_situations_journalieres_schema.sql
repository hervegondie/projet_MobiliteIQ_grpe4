DO $$ BEGIN
 IF current_database()<>'Silver_test' OR to_regclass('gold.correspondance_runs') IS NULL THEN
  RAISE EXCEPTION 'Les correspondances Gold doivent etre installees dans Silver_test';
 END IF;
 IF to_regclass('public.meteo_quotidienne_points') IS NULL
 OR to_regclass('public.meteo_quotidienne_points_runs') IS NULL THEN
  RAISE EXCEPTION 'Installer le flux meteo quotidienne multi-points avant cette etape';
 END IF;
END $$;
CREATE TABLE IF NOT EXISTS gold.dim_date (
 date_jour date PRIMARY KEY,
 annee integer NOT NULL,mois integer NOT NULL,jour_semaine_iso integer NOT NULL,
 fin_de_semaine boolean NOT NULL,heures_jour integer NOT NULL CHECK(heures_jour IN(23,24,25))
);
CREATE TABLE IF NOT EXISTS gold.dim_troncon_historique (
 troncon_cle text PRIMARY KEY,troncon_id text NOT NULL,empreinte_source text NOT NULL,
 numero_route text,classe_route text,longueur_m double precision NOT NULL,
 latitude_repere double precision,longitude_repere double precision,
 geojson text NOT NULL,UNIQUE(troncon_id,empreinte_source)
);
CREATE TABLE IF NOT EXISTS gold.situation_jour_runs (
 run_id uuid PRIMARY KEY,date_jour date NOT NULL REFERENCES gold.dim_date,
 empreinte_entrees text UNIQUE NOT NULL,algorithme text NOT NULL,
 correspondance_run_id uuid NOT NULL REFERENCES gold.correspondance_runs,
 instant_reference timestamptz NOT NULL,connaissance_arretee_at timestamptz NOT NULL,
 cree_at timestamptz NOT NULL DEFAULT now(),configuration jsonb NOT NULL,bilan jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS gold.fact_situation_journaliere (
 run_id uuid NOT NULL REFERENCES gold.situation_jour_runs,
 date_jour date NOT NULL REFERENCES gold.dim_date,
 troncon_id text NOT NULL,troncon_cle text NOT NULL REFERENCES gold.dim_troncon_historique,
 lieu_id text,statut_meteo text NOT NULL,
 nb_entraves_actives integer,nb_chantiers_actifs integer,
 nb_associations_a_verifier integer NOT NULL,
 longueur_proximite_active_m double precision,part_longueur_entravee double precision,
 duree_max_entrave_jours double precision,
 temperature_min numeric,temperature_max numeric,precipitation_totale numeric,
 pluie numeric,neige_cm numeric,vent_max_kmh numeric,visibilite_moyenne_m numeric,
 djma_reference numeric,annee_djma integer,debit_temps_reel boolean NOT NULL DEFAULT false,
 collecte_meteo_id uuid,meteo_collecte_at timestamptz,
 sources jsonb NOT NULL,raisons_qualite jsonb NOT NULL,
 qualite_ok integer NOT NULL CHECK(qualite_ok IN(0,1)),
 mode_travaux text NOT NULL DEFAULT 'instantane_fin_journee',
 PRIMARY KEY(run_id,troncon_id),
 CHECK(part_longueur_entravee IS NULL OR part_longueur_entravee BETWEEN 0 AND 1),
 CHECK(NOT debit_temps_reel)
);
CREATE INDEX IF NOT EXISTS situations_jour_date_idx ON gold.fact_situation_journaliere(date_jour,troncon_id);
CREATE TABLE IF NOT EXISTS gold.situation_jour_publication (
 date_jour date PRIMARY KEY REFERENCES gold.dim_date,
 run_id uuid NOT NULL REFERENCES gold.situation_jour_runs,
 publie_at timestamptz NOT NULL DEFAULT now()
);
CREATE OR REPLACE VIEW gold.v_situation_journaliere AS
SELECT f.*,r.instant_reference,r.connaissance_arretee_at,r.correspondance_run_id,r.cree_at AS calcule_at
FROM gold.fact_situation_journaliere f JOIN gold.situation_jour_publication p USING(run_id,date_jour)
JOIN gold.situation_jour_runs r USING(run_id,date_jour);
CREATE OR REPLACE VIEW gold.v_ml_situation_journaliere AS
SELECT troncon_id,date_jour AS date,troncon_cle,
 nb_chantiers_actifs AS nb_travaux_actifs,part_longueur_entravee,duree_max_entrave_jours,
 temperature_min,neige_cm,visibilite_moyenne_m,qualite_ok,
 run_id,correspondance_run_id,instant_reference,connaissance_arretee_at
FROM gold.v_situation_journaliere WHERE qualite_ok=1;
CREATE OR REPLACE VIEW gold.v_qualite_journalier AS
SELECT d.date_jour,d.heures_jour,
 CASE WHEN p.run_id IS NULL THEN 'aucune_situation_publiee' ELSE 'publie' END AS statut,
 count(f.troncon_id) AS lignes_publiees,coalesce(sum(f.qualite_ok),0) AS lignes_ml
FROM gold.dim_date d LEFT JOIN gold.situation_jour_publication p USING(date_jour)
LEFT JOIN gold.fact_situation_journaliere f ON f.run_id=p.run_id AND f.date_jour=d.date_jour
GROUP BY d.date_jour,d.heures_jour,p.run_id;

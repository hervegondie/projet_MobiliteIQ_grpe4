"""Connexions et contrats SQL des nouvelles tables météo horaire."""
from contextlib import contextmanager
import os

BRONZE = dict(collecte_id='uuid', lieu_id='text', ingestion_timestamp='timestamp with time zone', payload='jsonb')
BASE = dict(collecte_id='uuid', lieu_id='text', latitude='numeric', longitude='numeric',
            latitude_modele='numeric', longitude_modele='numeric', collecte_at='timestamp with time zone',
            heure_meteo='timestamp with time zone')
METRICS = dict(temperature_c='numeric', ressenti_c='numeric', humidite_pct='numeric',
               precipitations_mm='numeric', pluie_mm='numeric', neige_cm='numeric', code_meteo='integer',
               nuages_pct='numeric', vent_kmh='numeric', direction_vent_deg='numeric',
               rafales_kmh='numeric', visibilite_m='numeric')
ACTUELLE = {**BASE, 'duree_secondes': 'integer', **METRICS}
PREVISIONS = {**BASE, 'horizon_heures': 'numeric', **METRICS, 'probabilite_pluie_pct': 'numeric'}
RUNS = dict(collecte_id='uuid', lieu_id='text', collecte_at='timestamp with time zone',
            processed_at='timestamp with time zone', payload_hash='text', nb_actuelles='integer', nb_previsions='integer')
REJETS = dict(collecte_id='uuid', raisons='jsonb', processed_at='timestamp with time zone')


@contextmanager
def connexion(role, readonly=False):
    import psycopg
    from psycopg.conninfo import conninfo_to_dict
    if role not in ('bronze', 'silver'):
        raise ValueError('Role de connexion inconnu')
    variable, database = (('BRONZE_DATABASE_URL', 'mobiliteIQIngestion') if role == 'bronze'
                          else ('SILVER_DATABASE_URL', 'Silver_test'))
    value = os.environ.get(variable) or (os.environ.get('DATABASE_URL') if role == 'bronze' else None)
    if not value:
        raise ValueError(f'Configurer {variable}')
    try:
        options = conninfo_to_dict(value)
    except Exception:
        raise ValueError(f'{variable} invalide') from None
    if options.get('dbname') != database or options.get('sslmode') not in ('require', 'verify-ca', 'verify-full'):
        raise ValueError(f'{variable} doit cibler {database} avec TLS')
    with psycopg.connect(value, connect_timeout=30) as conn:
        if readonly:
            conn.execute('SET TRANSACTION READ ONLY')
        conn.execute("SET LOCAL TIME ZONE 'UTC'")
        conn.execute("SET LOCAL statement_timeout = '90s'")
        if conn.execute('SELECT current_database()').fetchone()[0] != database:
            raise ValueError('Base connectee incorrecte')
        yield conn


def table(conn, name, columns, keys):
    from psycopg import sql
    definitions = [sql.SQL('{} {}').format(sql.Identifier(k), sql.SQL(t)) for k, t in columns.items()]
    definitions.append(sql.SQL('PRIMARY KEY ({})').format(sql.SQL(',').join(map(sql.Identifier, keys))))
    conn.execute(sql.SQL('CREATE TABLE IF NOT EXISTS public.{} ({})').format(
        sql.Identifier(name), sql.SQL(',').join(definitions)))
    actual = dict(conn.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_schema='public' AND table_name=%s", (name,)).fetchall())
    if actual != columns:
        raise ValueError(f'Schema incompatible : public.{name}. Migration explicite requise')
    key = conn.execute("SELECT array_agg(a.attname::text ORDER BY k.ord) FROM pg_constraint c "
                       "JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace "
                       "CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum,ord) "
                       "JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=k.attnum "
                       "WHERE n.nspname='public' AND t.relname=%s AND c.contype='p'", (name,)).fetchone()[0]
    if key != list(keys):
        raise ValueError(f'Cle primaire incompatible : public.{name}')


def inserer(conn, name, columns, rows):
    from psycopg import sql
    statement = sql.SQL('INSERT INTO public.{} ({}) VALUES ({}) ON CONFLICT DO NOTHING').format(
        sql.Identifier(name), sql.SQL(',').join(map(sql.Identifier, columns)),
        sql.SQL(',').join(sql.Placeholder() for _ in columns))
    with conn.cursor() as cursor:
        cursor.executemany(statement, [tuple(row[k] for k in columns) for row in rows])
        return cursor.rowcount


def initialiser_silver(conn):
    conn.execute('SELECT pg_advisory_xact_lock(740221106)')
    for name, columns, keys in [('meteo_actuelle', ACTUELLE, ('collecte_id',)),
                                ('meteo_previsions_horaires', PREVISIONS, ('collecte_id', 'heure_meteo')),
                                ('meteo_horaire_runs', RUNS, ('collecte_id',)),
                                ('meteo_horaire_rejets', REJETS, ('collecte_id',))]:
        table(conn, name, columns, keys)
    conn.execute('CREATE INDEX IF NOT EXISTS meteo_horaire_lieu_collecte_idx ON public.meteo_previsions_horaires (lieu_id, collecte_at DESC)')
    conn.execute('CREATE OR REPLACE VIEW public.v_meteo_actuelle_derniere AS '
                 'SELECT DISTINCT ON (lieu_id) * FROM public.meteo_actuelle '
                 'ORDER BY lieu_id, collecte_at DESC, collecte_id DESC')
    conn.execute('CREATE OR REPLACE VIEW public.v_meteo_previsions_dernieres AS '
                 'WITH derniere AS (SELECT DISTINCT ON (lieu_id) lieu_id,collecte_id '
                 'FROM public.meteo_horaire_runs ORDER BY lieu_id,collecte_at DESC,collecte_id DESC) '
                 'SELECT p.* FROM public.meteo_previsions_horaires p JOIN derniere d '
                 'ON p.collecte_id=d.collecte_id AND p.lieu_id=d.lieu_id')

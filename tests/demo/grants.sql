-- Demo-only copy of roles/timescaledb/templates/04-roles-grants.sql.j2 with
-- fixed passwords, so the demo needs no Ansible run and no vault access.
-- Grants are IDENTICAL to production on purpose: a missing grant is a real
-- failure mode, and running the demo as superuser would hide it.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'importer') THEN
        CREATE ROLE importer LOGIN PASSWORD 'demo-importer-pw';
    ELSE
        ALTER ROLE importer PASSWORD 'demo-importer-pw';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO importer;
GRANT INSERT ON metrics_raw TO importer;
GRANT SELECT, INSERT, UPDATE, DELETE ON importer_state TO importer;

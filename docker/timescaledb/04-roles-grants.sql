-- docker compose counterpart of roles/timescaledb/templates/04-roles-grants.sql.j2.
-- Passwords arrive as psql variables (-v receiver_password=...) instead of
-- being templated in, so this file can be baked into the image. :'var'
-- quotes them as literals, so any character is safe in a password.
--
-- Not a DO $$ block like the Ansible template: psql does not substitute
-- variables inside dollar-quoted strings. format() + \gexec does the same.
-- Keep the GRANTs below in sync with the Ansible template.

SELECT format(
         CASE WHEN EXISTS (SELECT FROM pg_roles WHERE rolname = r.name)
              THEN 'ALTER ROLE %I PASSWORD %L'
              ELSE 'CREATE ROLE %I LOGIN PASSWORD %L' END,
         r.name, r.pw)
  FROM (VALUES ('receiver',   :'receiver_password'),
               ('importer',   :'importer_password'),
               ('grafana_ro', :'grafana_ro_password')) AS r(name, pw)
\gexec

-- receiver: insert-only on the raw hypertable, nothing else.
GRANT USAGE ON SCHEMA public TO receiver;
GRANT INSERT ON metrics_raw TO receiver;

-- importer: insert-only on the raw hypertable plus its own bookkeeping
-- table. Deliberately no SELECT on metrics_raw.
GRANT USAGE ON SCHEMA public TO importer;
GRANT INSERT ON metrics_raw TO importer;
GRANT SELECT, INSERT, UPDATE, DELETE ON importer_state TO importer;

-- grafana_ro: read-only across all three tiers.
GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT SELECT ON metrics_raw, metrics_1h, metrics_4h TO grafana_ro;
GRANT SELECT ON importer_state TO grafana_ro;

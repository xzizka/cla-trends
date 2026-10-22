# TimescaleDB + this stack's schema, for the docker compose deployment.
#
# The server itself is the unmodified upstream image. What is added is the
# SQL (shared with roles/timescaledb, not copied) and `cla-apply-schema`,
# which the compose `db-init` service runs on every `up` — the same
# "apply idempotently on every deploy" approach the Ansible role uses,
# rather than docker-entrypoint-initdb.d (see bug-014 in .wolf/buglog.json).
#
# NOT the -oss tag: continuous aggregates and retention/compression policies
# are Timescale-License features, absent from the Apache-only build.
ARG TIMESCALEDB_TAG=2.31.0-pg18
FROM docker.io/timescale/timescaledb:${TIMESCALEDB_TAG}

COPY roles/timescaledb/files/init/01-schema.sql \
     roles/timescaledb/files/init/02-continuous-aggregates.sql \
     roles/timescaledb/files/init/03-policies.sql \
     docker/timescaledb/04-roles-grants.sql \
     /opt/sql-init/
COPY --chmod=0755 docker/timescaledb/apply-schema.sh /usr/local/bin/cla-apply-schema

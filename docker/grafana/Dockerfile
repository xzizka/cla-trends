# Grafana with this stack's datasource and dashboards provisioned, for the
# docker compose deployment. The datasource password comes from the
# container env at startup (Grafana expands ${VAR} in provisioning files).
#
# Dashboards live under /etc/grafana, NOT /var/lib/grafana/dashboards as in
# the Ansible role: /var/lib/grafana is a named volume here, and a volume
# keeps the files of the image it was first created from — an upgraded
# image would keep serving the old dashboards.
ARG GRAFANA_TAG=13.2.4
FROM docker.io/grafana/grafana:${GRAFANA_TAG}

COPY docker/grafana/datasource.yml /etc/grafana/provisioning/datasources/timescaledb.yml
COPY docker/grafana/dashboards.yml /etc/grafana/provisioning/dashboards/dashboards.yml
COPY roles/grafana/files/provisioning/dashboards-json/ /etc/grafana/dashboards/

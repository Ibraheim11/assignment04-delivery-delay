# syntax=docker/dockerfile:1
FROM apache/airflow:slim-3.1.0-python3.12 AS runtime
USER airflow
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt
# Keep Airflow fixed while adding the versions used by the Phase 4 Joblib model.
# Optional public CA bundle for managed proxy builds; never stored in an image layer.
RUN --mount=type=secret,id=proxy_ca,required=false \
    if [ -f /run/secrets/proxy_ca ]; then export PIP_CERT=/run/secrets/proxy_ca; fi; \
    pip install --no-cache-dir "apache-airflow==3.1.0" "apache-airflow-providers-fab==2.4.3" "psycopg2-binary==2.9.10" "asyncpg==0.30.0" -r /opt/airflow/requirements.txt && \
    pip check
ENV PYTHONPATH=/opt/airflow
WORKDIR /opt/airflow

# Optional test image; production services use the runtime target.
FROM runtime AS test
COPY --chown=airflow:root requirements-dev.txt /opt/airflow/requirements-dev.txt
RUN --mount=type=secret,id=proxy_ca,required=false \
    if [ -f /run/secrets/proxy_ca ]; then export PIP_CERT=/run/secrets/proxy_ca; fi; \
    pip install --no-cache-dir "apache-airflow==3.1.0" -r /opt/airflow/requirements-dev.txt && \
    pip check

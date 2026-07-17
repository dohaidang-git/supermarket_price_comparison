FROM docker:27-cli AS docker_cli

FROM apache/airflow:2.10.5-python3.12

USER root

COPY --from=docker_cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker_cli /usr/local/libexec/docker/cli-plugins/docker-compose /usr/local/libexec/docker/cli-plugins/docker-compose

USER airflow

COPY --chown=airflow:root requirements.txt /tmp/project-requirements.txt
RUN pip install --no-cache-dir -r /tmp/project-requirements.txt

USER root
RUN /home/airflow/.local/bin/playwright install-deps chromium

USER airflow
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/airflow/.cache/ms-playwright
# The local network MITM certificate is not available inside this build image.
# Restrict the compatibility override to downloading Playwright's browser artifact.
RUN NODE_TLS_REJECT_UNAUTHORIZED=0 /home/airflow/.local/bin/playwright install chromium

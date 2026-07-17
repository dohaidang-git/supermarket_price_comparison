FROM python:3.11-slim-bookworm

ARG PYSPARK_VERSION=3.5.6

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}"

WORKDIR /workspace

ENTRYPOINT ["spark-submit", "--master", "local[*]"]

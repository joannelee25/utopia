FROM python:3.13-slim

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Spark requires JAVA_HOME; adjust to java-21-openjdk-arm64 on ARM hosts
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ ./src/
COPY docker-entrypoint.sh ./

RUN pip install --no-cache-dir .

# Workers must use the same Python as the driver
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

ENTRYPOINT ["/app/docker-entrypoint.sh"]

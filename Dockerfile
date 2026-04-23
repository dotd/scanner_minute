# scanner_minute — single container with Node server + Python scanners.
# Code is mounted at runtime; rebuild only when requirements or Dockerfile change.
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    NODE_MAJOR=20

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates gnupg tini \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
        | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_${NODE_MAJOR}.x nodistro main" \
        > /etc/apt/sources.list.d/nodesource.list \
    && apt-get update && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY docker/requirements.txt /app/docker/requirements.txt
RUN pip install -r /app/docker/requirements.txt

# Install Node deps at /app so /app/node_modules lives ABOVE /app/node_server.
# That way mounting node_server/ as a read-only volume at runtime doesn't
# shadow the installed modules — Node's resolver walks up from server.js to
# /app/node_modules.
COPY node_server/package.json /app/package.json
RUN cd /app && npm install --omit=dev

COPY docker/entrypoint.sh /app/docker/entrypoint.sh
RUN chmod +x /app/docker/entrypoint.sh

ENV PYTHONPATH=/app \
    SCANNER_MINUTE_EMBEDDED_SERVER=1 \
    HOST=0.0.0.0 \
    AUTH_ENABLED=1

EXPOSE 3000

ENTRYPOINT ["/usr/bin/tini", "--", "/app/docker/entrypoint.sh"]

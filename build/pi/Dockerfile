FROM docker.io/library/node:22-slim@sha256:7af03b14a13c8cdd38e45058fd957bf00a72bbe17feac43b1c15a689c029c732

ARG PI_VERSION=0.78.0

RUN set -eu; \
    apt-get update -qq; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
      git ca-certificates curl; \
    rm -rf /var/lib/apt/lists/*

RUN npm install -g @earendil-works/pi-coding-agent@${PI_VERSION}

COPY --chown=root:root --chmod=0755 parley-pi-entrypoint /usr/local/bin/parley-pi-entrypoint

RUN mkdir -p \
      /project/repo \
      /project/workspace \
      /project/reference \
      /home/node/.pi/agent \
      /home/node/.pi-shared/agent && \
    chown -R node:node /project /home/node/.pi /home/node/.pi-shared

USER node
WORKDIR /project/repo

ENTRYPOINT ["/usr/local/bin/parley-pi-entrypoint"]

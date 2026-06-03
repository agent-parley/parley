FROM docker.io/library/node:22-slim@sha256:7af03b14a13c8cdd38e45058fd957bf00a72bbe17feac43b1c15a689c029c732

ARG PI_VERSION=0.78.0
ARG DEBIAN_SNAPSHOT=20260601T000000Z

# Install ca-certificates first from the stock (HTTP, GPG-verified) sources so
# the HTTPS snapshot archive below can be TLS-verified. node:22-slim ships
# without ca-certificates, so rewriting to https://snapshot.debian.org before
# installing them is a chicken-and-egg that fails the TLS handshake.
RUN set -eu; \
    apt-get update -qq; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
      ca-certificates; \
    snapshot="https://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}"; \
    security_snapshot="https://snapshot.debian.org/archive/debian-security/${DEBIAN_SNAPSHOT}"; \
    sed -i \
      -e "s|http://deb.debian.org/debian-security|${security_snapshot}|g" \
      -e "s|https://deb.debian.org/debian-security|${security_snapshot}|g" \
      -e "s|http://security.debian.org/debian-security|${security_snapshot}|g" \
      -e "s|https://security.debian.org/debian-security|${security_snapshot}|g" \
      -e "s|http://deb.debian.org/debian|${snapshot}|g" \
      -e "s|https://deb.debian.org/debian|${snapshot}|g" \
      /etc/apt/sources.list.d/debian.sources; \
    printf 'Acquire::Check-Valid-Until "false";\n' > /etc/apt/apt.conf.d/99snapshot; \
    apt-get update -qq; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
      git curl; \
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

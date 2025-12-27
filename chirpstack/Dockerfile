#=ghcr.io/hassio-addons/base:19.0.0 
ARG BUILD_FROM
FROM ${BUILD_FROM}

# ---- Pull pinned ChirpStack binary + defaults ----
FROM chirpstack/chirpstack-sqlite:4.15.0 AS chirpstack_upstream

ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache bash jq nginx

# ChirpStack binary
COPY --from=chirpstack_upstream /usr/bin/chirpstack /usr/bin/chirpstack

# s6 init + service scripts + ingress config + Upstream default config + region files
COPY rootfs/ /

RUN chmod +x /etc/cont-init.d/00-config \
    && chmod +x /etc/services.d/chirpstack/run

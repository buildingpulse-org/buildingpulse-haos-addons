#=ghcr.io/hassio-addons/base:19.0.0 
ARG BUILD_FROM 
FROM ${BUILD_FROM}

# ---- Pull pinned ChirpStack binary + defaults ----
FROM chirpstack/chirpstack-gateway-bridge:4.1 AS chirpstack_gw_upstream

ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache bash jq 

# ChirpStack binary
COPY --from=chirpstack_gw_upstream /usr/bin/chirpstack-gateway-bridge /usr/bin/chirpstack-gateway-bridge

# s6 init + service scripts + ingress config + Upstream default config + region files
COPY rootfs/ /

RUN chmod +x /etc/cont-init.d/00-config-gw \
    && chmod +x /etc/services.d/chirpstack-gw/run

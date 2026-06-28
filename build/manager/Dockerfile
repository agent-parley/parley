FROM docker.io/library/golang:1.26-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# cmd/parley embeds the hypermedia UI assets at compile time. The same binary can
# spawn itself as the local runner child, so the runtime image only needs this one
# application binary plus CA roots for outbound HTTPS integrations.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/parley ./cmd/parley

RUN set -eu; \
    mkdir -p \
      /out/rootfs/data/home \
      /out/rootfs/data/config \
      /out/rootfs/tmp \
      /out/rootfs/usr/local/bin \
      /out/rootfs/workspace/repo \
      /out/rootfs/workspace/reference; \
    chmod 0777 /out/rootfs/data /out/rootfs/data/home /out/rootfs/data/config /out/rootfs/workspace /out/rootfs/workspace/repo /out/rootfs/workspace/reference; \
    chmod 1777 /out/rootfs/tmp; \
    printf 'nonroot:x:65532:65532:nonroot:/data/home:/sbin/nologin\n' > /out/passwd; \
    printf 'nonroot:x:65532:\n' > /out/group

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/passwd /etc/passwd
COPY --from=build /out/group /etc/group
COPY --from=build /out/rootfs/ /
COPY --from=build /out/parley /usr/local/bin/parley

ENV HOME=/data/home \
    XDG_CONFIG_HOME=/data/config \
    PARLEY_ADDR=0.0.0.0:8080 \
    PARLEY_DATA_DIR=/data \
    PARLEY_SOURCE_REPO=/workspace/repo \
    PARLEY_REFERENCE_ROOT=/workspace/reference

USER 65532:65532
WORKDIR /workspace/repo
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/parley"]

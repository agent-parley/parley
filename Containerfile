FROM docker.io/library/golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/parley ./cmd/parley \
    && mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
ENV PARLEY_DATA_ROOT=/data \
    PARLEY_APP_CONTAINER=true \
    PARLEY_EXECUTION_MODE=dry-run
COPY --from=build --chown=nonroot:nonroot /out/data /data
VOLUME ["/data"]
COPY --from=build /out/parley /usr/local/bin/parley
EXPOSE 7345
ENTRYPOINT ["/usr/local/bin/parley"]
CMD ["--app-container", "--bind", "0.0.0.0:7345"]

FROM docker.io/library/golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN go build -o /out/parley ./cmd/parley

FROM gcr.io/distroless/base-debian12:nonroot
ENV PARLEY_DATA_ROOT=/data
COPY --from=build /out/parley /usr/local/bin/parley
EXPOSE 7345
ENTRYPOINT ["/usr/local/bin/parley"]
CMD ["--bind", "127.0.0.1:7345"]

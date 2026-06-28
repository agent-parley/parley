# Container deployment

Parley can run as a local server process or as an app container. The manager image is a single static `parley` binary with embedded web assets. At runtime it persists state under an explicit data root and reads the target repository from an explicit repository root.

## Build the manager image

From the repository root:

```sh
podman build \
  -f build/manager/Dockerfile \
  -t localhost/parley-manager:dev \
  .
```

## Run the UI against mounted roots

Create or choose two host paths:

- `/absolute/path/to/parley-data` — durable Parley state, SQLite, artifacts, workspaces, and logs.
- `/absolute/path/to/repository` — the repository Parley should inspect and run against.

Start the app container:

```sh
podman run --rm \
  --name parley-manager \
  --publish 127.0.0.1:8080:8080 \
  --userns keep-id \
  --user "$(id -u):$(id -g)" \
  --env PARLEY_ADDR=0.0.0.0:8080 \
  --env PARLEY_DATA_DIR=/data \
  --env PARLEY_SOURCE_REPO=/workspace/repo \
  --volume /absolute/path/to/parley-data:/data:rw \
  --volume /absolute/path/to/repository:/workspace/repo:rw \
  localhost/parley-manager:dev
```

Open `http://127.0.0.1:8080`.

The `--userns keep-id` and `--user` flags make files written under the mounted data root owned by the invoking host user in rootless Podman. If your runtime or host policy handles bind-mount ownership differently, use any UID/GID that can write the data root and read/write the repository root. On SELinux-enforcing hosts, append `,Z` to each writable bind mount option, for example `:/data:rw,Z`.

## Optional reference material mount

If the run should receive read-only reference material, mount it and keep the container path in `PARLEY_REFERENCE_ROOT`:

```sh
podman run --rm \
  --name parley-manager \
  --publish 127.0.0.1:8080:8080 \
  --userns keep-id \
  --user "$(id -u):$(id -g)" \
  --env PARLEY_REFERENCE_ROOT=/workspace/reference \
  --volume /absolute/path/to/parley-data:/data:rw \
  --volume /absolute/path/to/repository:/workspace/repo:rw \
  --volume /absolute/path/to/reference:/workspace/reference:ro \
  localhost/parley-manager:dev
```

## Runtime notes

- The image defaults to `PARLEY_ADDR=0.0.0.0:8080`, `PARLEY_DATA_DIR=/data`, and `PARLEY_SOURCE_REPO=/workspace/repo`.
- The default `noop` adapter starts the UI and exercises the harness without needing agent credentials.
- Full Pi-backed implementation and validation still require the same local substrate as process mode: a working rootless Podman setup, worker image, and explicit credential paths or volumes. Do not mount container runtime sockets or credentials unless the host operator has deliberately chosen that deployment shape.
- The published port above binds to loopback only. Use a different `--publish` address only when you also provide the intended network access controls.
